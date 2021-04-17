#ifndef CLIENT_H
#define CLIENT_H

#include <arpa/inet.h>

int client(char *query_file, int n_threads, in_port_t server_port, in_addr_t server_ip);

#endif /* CLIENT_H */
