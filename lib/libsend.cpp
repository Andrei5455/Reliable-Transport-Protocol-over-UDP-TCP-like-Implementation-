#include <pthread.h>
#include <cstdlib>
#include <map>
#include <cstdint>
#include "lib.h"
#include "utils.h"
#include "protocol.h"
#include <cassert>
#include <poll.h>
#include <sys/timerfd.h>
#include <cstring>
#include <unistd.h>

using namespace std;

int conn_idx = 1;
std::map<int, struct connection *> cons;

int optimal_speed = 0;
int optimal_delay = 0;

struct pollfd data_fds[MAX_CONNECTIONS];
struct pollfd timer_fds[MAX_CONNECTIONS];
int fdmax = 0;

int send_data(int conn_id, char *buffer, int len)
{
    if (cons.find(conn_id) == cons.end())
        return -1;

    struct connection *con = cons[conn_id];

    pthread_mutex_lock(&con->con_lock);

    /* We will write code here as to not have sync problems with sender_handler */

    while (con->send_buffer_len + len > MAX_BUFFER_SIZE)
        pthread_cond_wait(&con->data_cond, &con->con_lock);

    memcpy(con->send_buffer + con->send_buffer_len, buffer, len);
    con->send_buffer_len += len;

    uint32_t effective_window = con->max_window_seq;
    if (con->recv_window_size < (int)effective_window)
        effective_window = (con->recv_window_size > 1) ?(uint32_t)con->recv_window_size : 1;

    while (con->next_seq_num < con->send_base + effective_window) {
        if (con->send_buffer_len <= 0)
            break;

        int payload_len = (con->send_buffer_len < MAX_DATA_SIZE) ? con->send_buffer_len : MAX_DATA_SIZE;
        int index = con->next_seq_num % WINDOW_SIZE;

        struct poli_tcp_data_hdr *data_hdr = (struct poli_tcp_data_hdr *)con->window_buffers[index];
        data_hdr->protocol_id = POLI_PROTOCOL_ID;
        data_hdr->type = DATA;
        data_hdr->seq_num = htons(con->next_seq_num & 0xFFFF);
        data_hdr->len = htons(payload_len);
        data_hdr->conn_id = con->conn_id;

        memcpy(con->window_buffers[index] + sizeof(struct poli_tcp_data_hdr), con->send_buffer, payload_len);
        con->window_lengths[index] = payload_len + sizeof(struct poli_tcp_data_hdr);
        con->acked[index] = false;

        sendto(con->sockfd, con->window_buffers[index], con->window_lengths[index], 0, (const sockaddr*)&con->servaddr, sizeof(con->servaddr));

        if (con->send_buffer_len > payload_len)
            memmove(con->send_buffer, con->send_buffer + payload_len, con->send_buffer_len - payload_len);
        con->send_buffer_len -= payload_len;
        con->next_seq_num++;
    }

    pthread_mutex_unlock(&con->con_lock);

    return len;
}

void *sender_handler(void *arg)
{
    char buf[MAX_SEGMENT_SIZE];

    while (1) {
        if (fdmax == 0) {
            usleep(100);
            continue;
        }

        int conn_id = -1;
        int res = recv_message_or_timeout(buf, MAX_SEGMENT_SIZE, &conn_id);

        if (res == -14) {
            usleep(100);
            continue;
        }

        if (res == -1) {
            for (auto const& [cid, c] : cons) {
                pthread_mutex_lock(&c->con_lock);

                for (uint32_t seq = c->send_base; seq < c->next_seq_num; seq++) {
                    int index = seq % WINDOW_SIZE;
                    if (!c->acked[index])
                        sendto(c->sockfd, c->window_buffers[index], c->window_lengths[index], 0, (const sockaddr*)&c->servaddr, sizeof(c->servaddr));
                }

                uint32_t effective_window = c->max_window_seq;
                if (c->recv_window_size < (int)effective_window)
                    effective_window = (c->recv_window_size > 1) ? (uint32_t)c->recv_window_size : 1;

                while (c->next_seq_num < c->send_base + effective_window) {
                    if (c->send_buffer_len <= 0)
                        break;

                    int payload_len = (c->send_buffer_len < MAX_DATA_SIZE) ? c->send_buffer_len : MAX_DATA_SIZE;
                    int index = c->next_seq_num % WINDOW_SIZE;

                    struct poli_tcp_data_hdr *data_hdr = (struct poli_tcp_data_hdr *)c->window_buffers[index];
                    data_hdr->protocol_id = POLI_PROTOCOL_ID;
                    data_hdr->type = DATA;
                    data_hdr->seq_num = htons(c->next_seq_num & 0xFFFF);
                    data_hdr->len = htons(payload_len);
                    data_hdr->conn_id = c->conn_id;

                    memcpy(c->window_buffers[index] + sizeof(struct poli_tcp_data_hdr), c->send_buffer, payload_len);
                    c->window_lengths[index] = payload_len + sizeof(struct poli_tcp_data_hdr);
                    c->acked[index] = false;

                    sendto(c->sockfd, c->window_buffers[index], c->window_lengths[index], 0, (const sockaddr*)&c->servaddr, sizeof(c->servaddr));

                    if (c->send_buffer_len > payload_len)
                        memmove(c->send_buffer, c->send_buffer + payload_len, c->send_buffer_len - payload_len);
                    c->send_buffer_len -= payload_len;
                    c->next_seq_num++;
                }

                /* Handle segment received from the receiver. We use this between locks
                as to not have synchronization issues with the send_data calls which are
                on the main thread */

                pthread_mutex_unlock(&c->con_lock);
            }
            continue;
        }

        if (res > 0 && conn_id >= 0 && cons.find(conn_id) != cons.end()) {
            if (res < (int)sizeof(struct poli_tcp_ctrl_hdr))
                continue;

            struct poli_tcp_ctrl_hdr *hdr = (struct poli_tcp_ctrl_hdr *)buf;
            if (hdr->type != ACK || hdr->protocol_id != POLI_PROTOCOL_ID)
                continue;

            pthread_mutex_lock(&cons[conn_id]->con_lock);
            struct connection *c = cons[conn_id];

            uint16_t ack16 = ntohs(hdr->ack_num);
            uint32_t base16 = c->send_base & 0xFFFF;
            uint32_t base32_high = c->send_base & 0xFFFF0000;
            uint32_t ack_num;

            if (base16 > 60000 && ack16 < 5000)
                ack_num = (base32_high + 0x10000) | ack16;
            else if (base16 < 5000 && ack16 > 60000)
                ack_num = (base32_high - 0x10000) | ack16;
            else
                ack_num = base32_high | ack16;

            uint16_t rwin = ntohs(hdr->recv_window);
            if (rwin > 0)
                c->recv_window_size = rwin;

            if (ack_num > c->send_base && ack_num <= c->next_seq_num) {
                while (c->send_base < ack_num) {
                    c->acked[c->send_base % WINDOW_SIZE] = true;
                    c->send_base++;
                }
                pthread_cond_broadcast(&c->data_cond);
            }

            uint32_t effective_window = c->max_window_seq;
            if (c->recv_window_size < (int)effective_window)
                effective_window = (c->recv_window_size > 1) ? (uint32_t)c->recv_window_size : 1;

            while (c->next_seq_num < c->send_base + effective_window) {
                if (c->send_buffer_len <= 0)
                    break;

                int payload_len = (c->send_buffer_len < MAX_DATA_SIZE) ? c->send_buffer_len : MAX_DATA_SIZE;
                int index = c->next_seq_num % WINDOW_SIZE;

                struct poli_tcp_data_hdr *data_hdr = (struct poli_tcp_data_hdr *)c->window_buffers[index];
                data_hdr->protocol_id = POLI_PROTOCOL_ID;
                data_hdr->type = DATA;
                data_hdr->seq_num = htons(c->next_seq_num & 0xFFFF);
                data_hdr->len = htons(payload_len);
                data_hdr->conn_id = c->conn_id;

                memcpy(c->window_buffers[index] + sizeof(struct poli_tcp_data_hdr), c->send_buffer, payload_len);
                c->window_lengths[index] = payload_len + sizeof(struct poli_tcp_data_hdr);
                c->acked[index] = false;

                sendto(c->sockfd, c->window_buffers[index], c->window_lengths[index], 0, (const sockaddr*)&c->servaddr, sizeof(c->servaddr));

                if (c->send_buffer_len > payload_len)
                    memmove(c->send_buffer, c->send_buffer + payload_len, c->send_buffer_len - payload_len);
                c->send_buffer_len -= payload_len;
                c->next_seq_num++;
            }

            pthread_mutex_unlock(&cons[conn_id]->con_lock);
        }
    }

    return NULL;
}

int setup_connection(uint32_t ip, uint16_t port)
{
    
    /* Implement the sender part of the Three Way Handshake. Blocks
    until the connection is established */

    struct connection *con = (struct connection *)calloc(1, sizeof(struct connection));
    int conn_id = conn_idx++;
    con->conn_id = conn_id;
    con->sockfd = socket(AF_INET, SOCK_DGRAM, 0);
    if (con->sockfd < 0) { 
        perror("socket creation failed\n"); 
        exit(EXIT_FAILURE); 
    }
    
    // This can be used to set a timer on a socket 
    struct timeval tv;
    tv.tv_sec = 0;
    tv.tv_usec = 100000;
    if (setsockopt(con->sockfd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv)) < 0) {
        perror("Error\n");
    }

    struct sockaddr_in servaddr;
    memset(&servaddr, 0, sizeof(servaddr));
    servaddr.sin_family = AF_INET;
    servaddr.sin_addr.s_addr = ip;
    servaddr.sin_port = port;

    struct poli_tcp_ctrl_hdr packet, recv_packet;
    memset(&packet, 0, sizeof(packet));
    packet.protocol_id = POLI_PROTOCOL_ID;
    packet.type = SYN;
    packet.conn_id = conn_id;

    while (1) {
        sendto(con->sockfd, &packet, sizeof(packet), 0, (const sockaddr*)&servaddr, sizeof(servaddr));

        sockaddr_in cli_addr;
        socklen_t len = sizeof(cli_addr);
        int bytes_received = recvfrom(con->sockfd, &recv_packet, sizeof(recv_packet), 0, (struct sockaddr *)&cli_addr, &len);

        if (bytes_received < 0) 
            continue;

        if (bytes_received > 0 && recv_packet.protocol_id == POLI_PROTOCOL_ID &&
            recv_packet.type == SYN_ACK) {
            con->servaddr = cli_addr;
            con->servaddr.sin_port = recv_packet.recv_window;
            break;
        }
    }

    memset(&packet, 0, sizeof(packet));
    packet.protocol_id = POLI_PROTOCOL_ID;
    packet.type = ACK;
    packet.conn_id = conn_id;
    sendto(con->sockfd, &packet, sizeof(packet), 0, (const sockaddr*)&servaddr, sizeof(servaddr));

    tv.tv_sec = 0;
    tv.tv_usec = 0;
    setsockopt(con->sockfd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

    /* Since we can have multiple connection, we want to know if data is available
       on the socket used by a given connection. We use POLL for this */
    data_fds[fdmax].fd = con->sockfd;
    data_fds[fdmax].events = POLLIN;

    /* This creates a timer and sets it to trigger every 1 sec. We use this
       to know if a timeout has happend on our connection */
    timer_fds[fdmax].fd = timerfd_create(CLOCK_REALTIME, 0);
    timer_fds[fdmax].events = POLLIN;
    struct itimerspec spec;
    spec.it_value.tv_sec = 0;
    spec.it_value.tv_nsec = 20000000;
    spec.it_interval.tv_sec = 0;
    spec.it_interval.tv_nsec = 20000000;
    timerfd_settime(timer_fds[fdmax].fd, 0, &spec, NULL);
    fdmax++;

    long long bandwidth_bytes = (long long)optimal_speed * 1000000LL / 8;
    long long rtt_ms = 2 * optimal_delay;
    long long bdp_bytes = (bandwidth_bytes * rtt_ms) / 1000;
    con->max_window_seq = (int)(bdp_bytes / MAX_DATA_SIZE);
    if (con->max_window_seq > WINDOW_SIZE) 
        con->max_window_seq = WINDOW_SIZE;
    if (con->max_window_seq < 1)
        con->max_window_seq = 1;

    con->recv_window_size = WINDOW_SIZE;

    pthread_mutex_init(&con->con_lock, NULL);
    pthread_cond_init(&con->data_cond, NULL);
    cons.insert({conn_id, con});

    DEBUG_PRINT("Connection established!");

    return conn_id;
}

void init_sender(int speed, int delay)
{
    pthread_t thread1;
    int ret;

    optimal_speed = speed;
    optimal_delay = delay;
    /* Create a thread that will*/
    ret = pthread_create(&thread1, NULL, sender_handler, NULL);
    assert(ret == 0);
}