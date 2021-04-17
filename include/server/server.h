#ifndef SERVER_H
#define SERVER_H

#include <arpa/inet.h>

int server(in_port_t query_port, in_port_t statistics_port, int n_threads, int buffer_size);

#endif /* SERVER_H */
