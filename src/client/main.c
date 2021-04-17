#include <arpa/inet.h>
#include <getopt.h>
#include <netdb.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "client/client.h"
#include "common.h"

int print_usage(const char *program);

int main(int argc, char *argv[])
{
	/* Parameters & relevant checking */
	struct option long_options[3] = {
		{"sp", required_argument, NULL, 'p'},
		{"sip", required_argument, NULL, 's'},
		{0}
	};
	int opt;

	int n_threads = 0, server_port = 0;
	char *query_file = NULL, *server_host = NULL;

	/* IP resolving */
	int ret;
	struct addrinfo *res;

	/* In network byte order */
	in_addr_t s_ip;
	in_port_t s_port;

	while ((opt = getopt_long_only(argc, argv, "q:w:", long_options, NULL)) != -1) {
		switch (opt) {
		case 'q':
			query_file = strdup(optarg);
			break;

		case 'w':
			n_threads = atoi(optarg);
			break;

		case 'p':
			server_port = atoi(optarg);
			break;

		case 's':
			server_host = strdup(optarg);
			break;

		default:
			return print_usage(argv[0]);
		}
	}

	if (n_threads <= 0 || server_port < 0 || server_port > UINT16_MAX)
		return print_usage(argv[0]);

	if (!query_file || !server_host)
		return print_usage(argv[0]);

	/* Get server IPv4 */
	if ((ret = getaddrinfo(server_host, NULL, &(struct addrinfo){.ai_family = AF_INET}, &res))) {
		fprintf(stderr, "%s: %s\n", server_host, gai_strerror(ret));
		return print_usage(argv[0]);
	}

	/* In string form */
	free(server_host);

	s_ip = ((struct sockaddr_in*) res->ai_addr)->sin_addr.s_addr;
	s_port = htons((in_port_t) server_port);

	freeaddrinfo(res);

	/* The magic begins... */
	return client(query_file, n_threads, s_port, s_ip);
}


int print_usage(const char *program)
{
	fprintf(stderr, "%s –q queryFile -w numThreads –sp servPort –sip servIP\n",
	        program);
	return DA_INVALID_PARAMETER;
}
