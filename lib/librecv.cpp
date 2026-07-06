#include <pthread.h>
#include <cstdlib>
#include <map>
#include <cstdint>
#include "lib.h"
#include "utils.h"
#include "protocol.h"
#include <poll.h>
#include <cassert>
#include <sys/timerfd.h>
#include <cstring>
#include <unistd.h>

using namespace std;

int listen_sockfd;
int conn_idx = 1;

std::map<int, struct connection *> cons;

struct pollfd data_fds[MAX_CONNECTIONS];
/* Used for timers per connection */
struct pollfd timer_fds[MAX_CONNECTIONS];
int fdmax = 0;

static struct poli_tcp_ctrl_hdr syn_queue_pkts[MAX_CONNECTIONS];
static struct sockaddr_in       syn_queue_addrs[MAX_CONNECTIONS];
static int                      syn_queue_len = 0;
static pthread_mutex_t          syn_queue_lock = PTHREAD_MUTEX_INITIALIZER;

int recv_data(int conn_id, char *buffer, int len)
{
    if (cons.find(conn_id) == cons.end())
        return -1;

    struct connection *con = cons[conn_id];
    int size = 0;

    pthread_mutex_lock(&con->con_lock);

    /* We will write code here as to not have sync problems with recv_handler */

    while (con->buffer_len == 0)
        pthread_cond_wait(&con->data_cond, &con->con_lock);

    int to_cpy = (len < con->buffer_len) ? len : con->buffer_len;

    if (to_cpy > 0) {
        memcpy(buffer, con->recv_buffer, to_cpy);
        size = to_cpy;
        if (to_cpy < con->buffer_len)
            memmove(con->recv_buffer, con->recv_buffer + to_cpy, con->buffer_len - to_cpy);
        con->buffer_len -= to_cpy;
        pthread_cond_broadcast(&con->data_cond);
    }

    pthread_mutex_unlock(&con->con_lock);

    return size;
}

void *receiver_handler(void *arg)
{
    char segment[MAX_SEGMENT_SIZE];
    int res;
    DEBUG_PRINT("Starting recviver handler\n");

    while (1) {

        int conn_id = -1;
        res = recv_message_or_timeout(segment, MAX_SEGMENT_SIZE, &conn_id);

        if (res == -14) {
            usleep(100);
            continue;
        }

        if (res == -1) {
            for (auto const& [cid, c] : cons) {
                pthread_mutex_lock(&c->con_lock);
                bool consecutive = false;

                while (c->received[c->recv_base % WINDOW_SIZE]) {
                    int bi = c->recv_base % WINDOW_SIZE;
                    int plen = c->lengths[bi];
                    if (c->buffer_len + plen > MAX_BUFFER_SIZE)
                        break;
                    memcpy(c->recv_buffer + c->buffer_len, c->window_buffers[bi], plen);
                    c->buffer_len += plen;
                    c->received[bi] = false;
                    c->recv_base++;
                    consecutive = true;
                }
                if (consecutive)
                    pthread_cond_broadcast(&c->data_cond);

                pthread_mutex_unlock(&c->con_lock);
            }
            continue;
        }

        if (res < (int)sizeof(struct poli_tcp_data_hdr))
            continue;
        if (cons.find(conn_id) == cons.end())
            continue;

        struct connection *con = cons[conn_id];
        struct poli_tcp_data_hdr *hdr = (struct poli_tcp_data_hdr *)segment;

        if (hdr->type != DATA || hdr->protocol_id != POLI_PROTOCOL_ID)
            continue;

        int payload_len = ntohs(hdr->len);

        uint16_t seq16 = ntohs(hdr->seq_num);
        uint32_t base16 = con->recv_base & 0xFFFF;
        uint32_t base32_high = con->recv_base & 0xFFFF0000;
        uint32_t seq_num;

        if (base16 > 60000 && seq16 < 5000)
            seq_num = (base32_high + 0x10000) | seq16;
        else if (base16 < 5000 && seq16 > 60000)
            seq_num = (base32_high - 0x10000) | seq16;
        else
            seq_num = base32_high | seq16;

        pthread_mutex_lock(&con->con_lock);

        if (seq_num >= con->recv_base && seq_num < con->recv_base + WINDOW_SIZE) {
            int index = seq_num % WINDOW_SIZE;

            if (!con->received[index]) {
                con->received[index] = true;
                con->lengths[index] = payload_len;
                memcpy(con->window_buffers[index], segment + sizeof(struct poli_tcp_data_hdr), payload_len);

                bool consecutive = false;
                while (con->received[con->recv_base % WINDOW_SIZE]) {
                    int bi = con->recv_base % WINDOW_SIZE;
                    int plen = con->lengths[bi];
                    if (con->buffer_len + plen > MAX_BUFFER_SIZE)
                        break;
                    memcpy(con->recv_buffer + con->buffer_len, con->window_buffers[bi], plen);
                    con->buffer_len += plen;
                    con->received[bi] = false;
                    con->recv_base++;
                    consecutive = true;
                }
                if (consecutive)
                    pthread_cond_broadcast(&con->data_cond);
            }
        }

        if (seq_num < con->recv_base + WINDOW_SIZE) {
            struct poli_tcp_ctrl_hdr ack_packet;
            memset(&ack_packet, 0, sizeof(ack_packet));
            ack_packet.protocol_id = POLI_PROTOCOL_ID;
            ack_packet.type = ACK;
            ack_packet.conn_id = con->conn_id;
            ack_packet.ack_num = htons(con->recv_base & 0xFFFF);

            int free_space = MAX_BUFFER_SIZE - con->buffer_len;
            if (free_space < 0) free_space = 0;
            int free_segments = free_space / MAX_DATA_SIZE;
            if (free_segments > WINDOW_SIZE) free_segments = WINDOW_SIZE;
            ack_packet.recv_window = htons((uint16_t)free_segments);

            sendto(con->sockfd, &ack_packet, sizeof(ack_packet), 0, (const sockaddr*)&con->servaddr, sizeof(con->servaddr));
        }

        /* Handle segment received from the sender. We use this between locks
        as to not have synchronization issues with the recv_data calls which are
        on the main thread */

        pthread_mutex_unlock(&con->con_lock);
    }

}

int wait4connect(uint32_t ip, uint16_t port)
{
    struct sockaddr_in cli_addr;
    socklen_t cli_len = sizeof(cli_addr);
    struct poli_tcp_ctrl_hdr packet;

    while (1) {
        pthread_mutex_lock(&syn_queue_lock);
        if (syn_queue_len > 0) {
            packet   = syn_queue_pkts[0];
            cli_addr = syn_queue_addrs[0];
            for (int i = 0; i < syn_queue_len - 1; i++) {
                syn_queue_pkts[i]  = syn_queue_pkts[i + 1];
                syn_queue_addrs[i] = syn_queue_addrs[i + 1];
            }
            syn_queue_len--;
            pthread_mutex_unlock(&syn_queue_lock);
            break;
        }
        pthread_mutex_unlock(&syn_queue_lock);

        int bytes_received = recvfrom(listen_sockfd, &packet, sizeof(packet), 0, (struct sockaddr*)&cli_addr, &cli_len);
        if (bytes_received > 0 && packet.type == SYN &&
            packet.protocol_id == POLI_PROTOCOL_ID)
            break;
    }

    struct sockaddr_in initial_cli_addr;
    memcpy(&initial_cli_addr, &cli_addr, sizeof(cli_addr));

    struct connection *con = (struct connection *)calloc(1, sizeof(struct connection));

    int conn_id = conn_idx++;

    /* Receive SYN on the connection socket. Create a new socket and bind it to
     * the chosen port. Send the data port number via SYN-ACK to the client */
    con->sockfd = socket(AF_INET, SOCK_DGRAM, 0);
    con->conn_id = conn_id;
    if (con->sockfd < 0) { 
        perror("socket creation failed\n");
        exit(EXIT_FAILURE); 
    }

    struct sockaddr_in my_addr;
    socklen_t my_addr_len = sizeof(my_addr);
    my_addr.sin_family = AF_INET;
    my_addr.sin_addr.s_addr = INADDR_ANY;
    my_addr.sin_port = htons(0);
    bind(con->sockfd, (struct sockaddr *)&my_addr, sizeof(my_addr));
    getsockname(con->sockfd, (struct sockaddr*)&my_addr, &my_addr_len);

    struct poli_tcp_ctrl_hdr ack_packet;
    memset(&ack_packet, 0, sizeof(ack_packet));
    ack_packet.protocol_id = POLI_PROTOCOL_ID;
    ack_packet.type = SYN_ACK;
    ack_packet.recv_window = my_addr.sin_port;
    ack_packet.conn_id = conn_id;

    sendto(listen_sockfd, &ack_packet, sizeof(ack_packet), 0, (const sockaddr*)&initial_cli_addr, sizeof(initial_cli_addr));

    while (1) {
        struct sockaddr_in current_cli_addr;
        int bytes_received = recvfrom(listen_sockfd, &packet, sizeof(packet), 0, (struct sockaddr *)&current_cli_addr, &cli_len);

        if (bytes_received > 0 && packet.protocol_id == POLI_PROTOCOL_ID) {
            if (current_cli_addr.sin_port == initial_cli_addr.sin_port && current_cli_addr.sin_addr.s_addr == initial_cli_addr.sin_addr.s_addr) {
                if (packet.type == ACK || packet.type == DATA)
                    break;
                else if (packet.type == SYN)
                    sendto(listen_sockfd, &ack_packet, sizeof(ack_packet), 0, (const sockaddr*)&initial_cli_addr, sizeof(initial_cli_addr));
            } else if (packet.type == SYN) {
                pthread_mutex_lock(&syn_queue_lock);
                if (syn_queue_len < MAX_CONNECTIONS) {
                    syn_queue_pkts[syn_queue_len]  = packet;
                    syn_queue_addrs[syn_queue_len] = current_cli_addr;
                    syn_queue_len++;
                }
                pthread_mutex_unlock(&syn_queue_lock);
            }
        }
    }

    memcpy(&con->servaddr, &initial_cli_addr, sizeof(initial_cli_addr));

    /* Since we can have multiple connection, we want to know if data is available
       on the socket used by a given connection. We use POLL for this */
    data_fds[fdmax].fd = con->sockfd;
    data_fds[fdmax].events = POLLIN;

     /* This creates a timer and sets it to trigger every 1 sec. We use this
       to know if a timeout has happend on a connection */  
    timer_fds[fdmax].fd = timerfd_create(CLOCK_REALTIME, 0);
    timer_fds[fdmax].events = POLLIN;

    struct itimerspec spec;
    spec.it_value.tv_sec = 0;
    spec.it_value.tv_nsec = 20000000;
    spec.it_interval.tv_sec = 0;
    spec.it_interval.tv_nsec = 20000000;
    timerfd_settime(timer_fds[fdmax].fd, 0, &spec, NULL);
    fdmax++;

    pthread_mutex_init(&con->con_lock, NULL);
    pthread_cond_init(&con->data_cond, NULL);
    cons.insert({conn_id, con});

    DEBUG_PRINT("Connection established!");

    return conn_id;
}

void init_receiver(int recv_buffer_bytes)
{
    pthread_t thread1;
    int ret;

    int sockfd;
    struct sockaddr_in servaddr;

    sockfd = socket(AF_INET, SOCK_DGRAM, 0);
    if (sockfd < 0) {
        perror("socket creation failed\n");
        exit(EXIT_FAILURE); 
    }

    servaddr.sin_family = AF_INET;
    servaddr.sin_addr.s_addr = INADDR_ANY;
    servaddr.sin_port = htons(8032);

    if (bind(sockfd, (const struct sockaddr *)&servaddr, sizeof(servaddr)) < 0) {
        perror("bind failed\n");
        exit(EXIT_FAILURE);
    }

    listen_sockfd = sockfd;

    ret = pthread_create(&thread1, NULL, receiver_handler, NULL);
    assert(ret == 0);
}