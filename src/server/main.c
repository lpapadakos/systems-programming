#include <arpa/inet.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "common.h"
#include "server/server.h"

int print_usage(const char *program);

int main(int argc, char *argv[])
{
	/* Parameters & relevant checking */
	int opt;
	int query_port = 0, statistics_port = 0;
	int n_threads = 0, buffer_size = 0;

	in_port_t q_port, s_port;

	while ((opt = getopt(argc, argv, "q:s:w:b:")) != -1) {
		switch (opt) {
		case 'q':
			query_port = atoi(optarg);
			break;

		case 's':
			statistics_port = atoi(optarg);
			break;

		case 'w':
			n_threads = atoi(optarg);
			break;

		case 'b':
			buffer_size = atoi(optarg);
			break;

		default:
			return print_usage(argv[0]);
		}
	}

	if (n_threads <= 0 || buffer_size <= 0)
		return print_usage(argv[0]);

	if (query_port <= 0 || query_port > UINT16_MAX ||
	    statistics_port <= 0 || statistics_port > UINT16_MAX)
		return print_usage(argv[0]);

	q_port = htons((in_port_t) query_port);
	s_port = htons((in_port_t) statistics_port);

	/* The magic begins... */
	return server(q_port, s_port, n_threads, buffer_size);
}


int print_usage(const char *program)
{
	fprintf(stderr, "%s –q queryPortNum -s statisticsPortNum –w numThreads –b bufferSize\n",
	        program);
	return DA_INVALID_PARAMETER;
}
