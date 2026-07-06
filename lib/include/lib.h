
#pragma once

#include <cstdint>
#include "utils.h"
#include <arpa/inet.h>

#include "protocol.h"

/* Maximum segment size, change as you see fit */
#define MAX_DATA_SIZE 512
#define MAX_SEGMENT_SIZE (MAX_DATA_SIZE + sizeof(poli_tcp_data_hdr))

#define MAX_CONNECTIONS 32

#define WINDOW_SIZE 100
#define MAX_BUFFER_SIZE 1000000

/* Protocol control block. Used track different parameters about a connection. 
 * Will need to be extenden to solve the homework with other parameters such as
 * last_ack or status depending on how you implement your protocol. */
struct connection {
    /* common window for both the sender and receiver. */
    /* list window: A window representation */

    int sockfd; /* socket used for this connection */
    int conn_id; /* connection identifier */
    struct sockaddr_in servaddr; /* used to identify the destination */
    pthread_mutex_t con_lock; /* Used for syncronization with the handler thread and read/send calls.*/

    uint32_t recv_base;
    bool received[WINDOW_SIZE];
    int lengths[WINDOW_SIZE]; /* Used to store the length of the data in each packet, useful for the receiver */
    /* TODO. Parameters used only by the sender */
    int recv_window_size; /* recv_window trimis de receiver în ACK-uri, pentru flow control */
    char recv_buffer[MAX_BUFFER_SIZE]; /* Used to store the data received but not yet read by the application, useful for the receiver */
    int buffer_len;
    
    pthread_cond_t data_cond; /* Used to signal the application that data is available in the recv_buffer, useful for the receiver */

    int max_window_seq; /* Used to store the max number of packets that can be inflight, since we can
                           have many more packets in our window */

    /* TODO. Parameters used only by the client */
    uint32_t send_base;
    uint32_t next_seq_num;
    bool acked[WINDOW_SIZE];
    char window_buffers[WINDOW_SIZE][MAX_SEGMENT_SIZE];
    int window_lengths[WINDOW_SIZE];
    char send_buffer[MAX_BUFFER_SIZE];
    int send_buffer_len;

};

/* ########## API that we expose to the application ########### */

/* Equivalent of listen. Ran by the server to waits for a connection from a
 * client. Returns a connection id. Blocking untill it receives a connection
 * request */
int wait4connect(uint32_t ip, uint16_t port);
/* Equivalent of connect. Used by the client to connect to a server. */
int setup_connection(uint32_t ip, uint16_t port);
/* Equivalent to recv. Blocking if there is no data to be written in buffer */
int recv_data(int connectionid, char *buffer, int len);
/* Equivalent to send. Used by the client to send a stream of bytes as segments */
int send_data(int conn_id, char *buffer, int len);
/* Used to initialize your protocol on the receiver side. */
void init_receiver(int recv_buffer_bytes);
/* Used to initialize your protocol on the sender side */
void init_sender(int speed, int delay);

/* ######### Internal API used by sender and receiver ########### */
int recv_message_or_timeout(char *buff, size_t len, int *conn_id);
