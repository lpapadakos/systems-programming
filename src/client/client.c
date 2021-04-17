#include <arpa/inet.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

#include "client/client.h"
#include "common.h"
#include "pipes.h"

/* Thread-shared variables */
static struct sockaddr_in to_server;
static pthread_barrier_t barrier;

/* Thread function */
void *send_cmd(void *arg);

int client(char *query_file, int n_threads, in_port_t server_port, in_addr_t server_ip)
{
	FILE *file;
	char *line = NULL;
	int i;
	void *ret;

	pthread_t threads[n_threads];
	char cmd[n_threads][1024];

	to_server.sin_family = AF_INET;
	to_server.sin_addr.s_addr = server_ip;
	to_server.sin_port = server_port;

	pipes_init(sizeof(((struct p_msg*) 0)->buffer)/4);

	if (!(file = fopen(query_file, "r"))) {
		perror(query_file);
		return DA_FILE_ERROR;
	}

	/* After waking up the threads, barrier will reset back to <count> */
	pthread_barrier_init(&barrier, NULL, n_threads);

	do {
		for (i = 0; i < n_threads; ++i) {
			line = fgets(cmd[i], sizeof(cmd[0]), file);
			pthread_create(threads + i, NULL, send_cmd, line);
		}

		for (i = 0; i < n_threads; ++i) {
			pthread_join(threads[i], &ret);

			if (ret != (void*) DA_OK)
				line = NULL;
		}
	} while (line);

	pthread_barrier_destroy(&barrier);

	fclose(file);
	free(query_file);

	return DA_OK;
}

void *send_cmd(void *arg)
{
	char *cmd = arg;
	int cmd_len;

	int sock;
	struct p_msg result;

	/* Will block until all n_threads are up */
	pthread_barrier_wait(&barrier);

	if (!cmd)
		return (void*) DA_OK;

	cmd_len = strlen(cmd);
	cmd[cmd_len - 1] = '\0';                       /* Replace trailing \n */

	if ((sock = socket(AF_INET, SOCK_STREAM, 0)) == -1) {
		perror("thread: socket()");
		return (void*) DA_SOCK_ERROR;
	}

	if (connect(sock, (struct sockaddr*) &to_server, sizeof(to_server)) == -1) {
		perror("thread: connect()");
		return (void*) DA_SOCK_ERROR;
	}

	msg_write(sock, cmd, cmd_len + 1);            /* Send query to server */

	msg_init(&result);
	msg_read(sock, &result);              /* Receive response from server */

	close(sock);                             /* Don't need server anymore */

	/* Write results to stdout (printf guarantees thread-safety) */
	printf("[%lu] %s\n%s\n", pthread_self(), cmd, result.buffer);

	return (void*) DA_OK;
}
