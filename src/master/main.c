#include <arpa/inet.h>
#include <dirent.h>
#include <netdb.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#include "common.h"
#include "master/master.h"

int print_usage(const char *program);

int main(int argc, char *argv[])
{
	/* Parameters & relevant checking */
	int opt;
	int workers = 0, buffer_size = 0, server_port = 0;
	char *server_host = NULL, *input_dir = NULL;

	char str_server_port[16];

	/* String checking */
	int ret;
	struct addrinfo *res;
	struct stat s;

	DIR *dir;
	struct dirent *entry;
	int subdirs = 0;

	while ((opt = getopt(argc, argv, "w:b:s:p:i:")) != -1) {
		switch (opt) {
		case 'w':
			workers = atoi(optarg);
			break;

		case 'b':
			buffer_size = atoi(optarg);
			break;

		case 's':
			server_host = strdup(optarg);
			break;

		case 'p':
			server_port = atoi(optarg);
			break;

		case 'i':
			input_dir = strdup(optarg);
			break;

		default:
			return print_usage(argv[0]);
		}
	}

	if (workers <= 0 || buffer_size <= 0 || server_port <= 0 || server_port > UINT16_MAX)
		return print_usage(argv[0]);

	if (!server_host || !input_dir)
		return print_usage(argv[0]);

	/* Get server IPv4 */
	if ((ret = getaddrinfo(server_host, NULL, &(struct addrinfo){.ai_family = AF_INET}, &res))) {
		fprintf(stderr, "%s: %s\n", server_host, gai_strerror(ret));
		return print_usage(argv[0]);
	}

	/* In string form */
	free(server_host);
	server_host = inet_ntoa(((struct sockaddr_in*) res->ai_addr)->sin_addr);

	freeaddrinfo(res);

	snprintf(str_server_port, sizeof(str_server_port), "%d", server_port);

	/* Check if input_dir is, in fact, a directory */
	if (stat(input_dir, &s)) {
		perror(input_dir);
		return print_usage(argv[0]);
	}

	if (!S_ISDIR(s.st_mode)) {
		fprintf(stderr, "%s: Not a directory\n", input_dir);
		return print_usage(argv[0]);
	}

	/* Limit workers to amount of subdirs */
	dir = opendir(input_dir);
	if (!dir)
		return DA_FILE_ERROR;

	while ((entry = readdir(dir))) {
		if (!strcmp(entry->d_name, ".") || !strcmp(entry->d_name, ".."))
				continue;

		if (entry->d_type == DT_DIR)
			subdirs++;
	}

	closedir(dir);

	/* No more workers than there are directories */
	workers = MIN(workers, subdirs);

	/* The magic begins... */
	return master(workers, buffer_size, strdup(server_host), str_server_port, input_dir);
}


int print_usage(const char *program)
{
	fprintf(stderr, "%s –w numWorkers -b bufferSize -s serverIP -p serverPort -i input_dir\n",
	        program);
	return DA_INVALID_PARAMETER;
}
