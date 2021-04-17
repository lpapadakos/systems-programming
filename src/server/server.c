#define _GNU_SOURCE

#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <signal.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>
#include <poll.h>

#include "common.h"
#include "pipes.h"
#include "server/r_buf.h"
#include "server/server.h"

#define STATISTICS 0
#define QUERY 1

/* Thread-shared variables */
static int workers;
/* In network byte order */
static in_addr_t worker_ip;
static in_port_t *worker_ports;

static struct ring_buffer *fds;
static pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t space_available = PTHREAD_COND_INITIALIZER;
static pthread_cond_t data_available = PTHREAD_COND_INITIALIZER;

static const char _whitespace[] = " \f\n\r\t\v";

int listen_at(in_port_t port);

/* Thread Functions */
void *server_thread(void *args);
int server_thread_statistics(int worker_fd);
int server_thread_query(int client_fd);

int connect_to_workers(struct pollfd *worker_fd);

/* Commands */
int s_disease_frequency(char *args, struct pollfd *worker_fd);
int s_topk_age_ranges(char *args, struct pollfd *worker_fd);
int s_search_patient_record(char *args, struct pollfd *worker_fd);
int s_num_patients(enum mode mode, char *args, struct pollfd *worker_fd);

int s_get_response(struct pollfd *worker_fd, struct p_msg *result, int client_fd);
int s_sum_cases(struct pollfd *worker_fd, struct p_msg *result, int client_fd);

/* Signal Stuff */
static volatile sig_atomic_t server_quit;
static struct sigaction sigact;

static void s_sig_handler(int sig, siginfo_t *siginfo, void *context)
{
	switch (sig) {
	case SIGINT:
	case SIGQUIT:
		server_quit = 1;
		break;
	};
}

int server(in_port_t query_port, in_port_t statistics_port, int n_threads, int buffer_size)
{
	in_port_t listen_ports[2];
	struct pollfd sock[2];                          /* Socket Descriptors */

	struct sockaddr_in incoming;
	socklen_t len;
	int fd;                                     /* Returned from accept() */

	pthread_t threads[n_threads];
	int i;

	/* Setup Signal Handlers */
	sigact.sa_sigaction = s_sig_handler;
	sigaction(SIGINT, &sigact, NULL);
	sigaction(SIGQUIT, &sigact, NULL);

	/* Socket creation - STATS IN (from workers) */
	if ((sock[STATISTICS].fd = listen_at(statistics_port)) < 0)
		return DA_SOCK_ERROR;

	sock[STATISTICS].events = POLLIN;
	listen_ports[STATISTICS] = statistics_port;

	/* Socket creation - QUERY IN (from client) */
	if ((sock[QUERY].fd = listen_at(query_port)) < 0)
		return DA_SOCK_ERROR;

	sock[QUERY].events = POLLIN;
	listen_ports[QUERY] = query_port;

	/* Setup msg framework */
	pipes_init(sizeof(((struct p_msg*) 0)->buffer)/4);

	fds = make_r_buf(buffer_size);                   /* Setup ring buffer */

	/* Assume 1 worker - might be amended later */
	workers = 1;
	worker_ports = malloc(workers * sizeof(worker_ports[0]));

	/* Create threads */
	for (i = 0; i < n_threads; ++i)
		pthread_create(threads + i, NULL, server_thread, listen_ports);

	while (!server_quit) {
		/* Wait for incoming connections on our 2 ports: */
		if (poll(sock, 2, -1) == -1) {
			if (errno != EINTR) {
				perror("server: poll()");
				server_quit = 1;
			}

			continue;                            /* Check SIGNALS */
		}

		for (i = 0; i < 2; ++i) {
			if (!(sock[i].revents & POLLIN))
				continue;

			len = sizeof(incoming);
			fd = accept(sock[i].fd, (struct sockaddr*) &incoming, &len);

			if (fd == -1) {
				if (errno != EINTR) {
					perror("server: accept()");
					server_quit = 1;
				}

				break;                       /* Check SIGNALS */
			}

			/* Acquire worker IP (to forward queries) */
			if (i == STATISTICS)
				worker_ip = incoming.sin_addr.s_addr;

			pthread_mutex_lock(&mutex);

			while (r_buf_full(fds))
				pthread_cond_wait(&space_available, &mutex);

			r_buf_push(fds, fd);  /* *Plop* that fd in the buffer */

			pthread_cond_signal(&data_available);
			pthread_mutex_unlock(&mutex);
		}
	}

	/* Wake the threads up and wait for their exit */
	pthread_cond_broadcast(&data_available);
	puts("Waiting for threads to exit...");

	for (i = 0; i < n_threads; ++i) {
		//printf("Waiting for %lu...\n", threads[i]);
		pthread_join(threads[i], NULL);
	}

	free(worker_ports);

	r_buf_destroy(fds);

	close(sock[STATISTICS].fd);
	close(sock[QUERY].fd);

	pthread_cond_destroy(&data_available);
	pthread_cond_destroy(&space_available);
	pthread_mutex_destroy(&mutex);

	return DA_OK;
}

int listen_at(in_port_t port)
{
	int sock;
	int enable = 1;                                   /* For setsockopt() */
	struct sockaddr_in listen_sock;

	if ((sock = socket(AF_INET, SOCK_STREAM, 0)) == -1) {
		perror("server: socket()");
		return DA_SOCK_ERROR;
	}

	/* Prevent "Address in use" after crash */
	if (setsockopt(sock, SOL_SOCKET, SO_REUSEADDR, &enable, sizeof(enable)) == -1) {
		perror("server: setsockopt()");
		return DA_SOCK_ERROR;
	}

	listen_sock.sin_family = AF_INET;
	listen_sock.sin_addr.s_addr = htonl(INADDR_ANY);
	listen_sock.sin_port = port;

	if (bind(sock, (struct sockaddr*) &listen_sock, sizeof(listen_sock)) == -1) {
		perror("server: bind()");
		return DA_SOCK_ERROR;
	}

	/* See README */
	if (listen(sock, SOMAXCONN) == -1) {
		perror("server: listen()");
		return DA_SOCK_ERROR;
	}

	return sock;
}

void *server_thread(void *args)
{
	in_port_t *listen_ports = args;

	int fd;
	socklen_t len;
	struct sockaddr_in dst;

	intptr_t ret = DA_OK;

	while (!server_quit) {
		pthread_mutex_lock(&mutex);

		while (r_buf_empty(fds)) {
			pthread_cond_wait(&data_available, &mutex);

			if (server_quit) {
				pthread_mutex_unlock(&mutex);
				return (void*) DA_OK;
			}
		}

		fd = r_buf_pop(fds);   /* Snatch that fd from the ring buffer */

		pthread_cond_signal(&space_available);
		pthread_mutex_unlock(&mutex);

		/* Depending on the destination of the request, handle it:
		 * - Print statistics and save worker port
		 * - Forward queries/requests to workers */
		len = sizeof(dst);
		if (getsockname(fd, (struct sockaddr*) &dst, &len) == -1) {
			perror("server: thread: getsockname()");
			return (void*) DA_SOCK_ERROR;
		}

		if (dst.sin_port == listen_ports[STATISTICS])
			ret = server_thread_statistics(fd);
		else if (dst.sin_port == listen_ports[QUERY])
			ret = server_thread_query(fd);

		close(fd);

		if (ret == DA_SOCK_ERROR) {
			pthread_mutex_lock(&mutex);
			server_quit = 1;
			pthread_mutex_unlock(&mutex);
		}
	}

	return (void*) ret;
}

int server_thread_statistics(int worker_fd)
{
	int ready = 0;
	struct p_msg msg;
	char *start, *end;

	int worker_tag;
	in_port_t worker_port;
	int got_header = 0;

	msg_init(&msg);

	while (!ready) {
		if (msg_read(worker_fd, &msg) == DA_SOCK_ERROR) {
			start = strerror_r(errno, msg.buffer, sizeof(msg.buffer));
			fprintf(stderr, "read() stats from worker: %s\n", start);
			return DA_SOCK_ERROR;
		}

		start = msg.buffer;

		/* Find last position of '\0' in read data.
		 * Then we can can print the strings (statistics) */
		end = memrchr(start, '\0', msg.pos - start);

		if (!end)
			continue;

		if (!got_header) {
			/* Get worker info */
			sscanf(start, "%d" MSG_DELIMITER "%hu", &worker_tag, &worker_port);

			pthread_mutex_lock(&mutex);
			if (worker_tag >= workers) {
				workers = worker_tag + 1;
				worker_ports = realloc(worker_ports, workers * sizeof(worker_ports[0]));
			}

			worker_ports[worker_tag] = htons(worker_port);
			pthread_mutex_unlock(&mutex);

			got_header = 1;

			start += strlen(start) + 1;

			if (start > end) {
				msg.consumed = 1;
				continue;
			}
		}

		if (!strcmp(end - strlen(MSG_READY), MSG_READY)) {
			ready++;
			end -= strlen(MSG_READY) + 1;

			close(worker_fd);
		}

		/* Print statistics (fwrite guarantees thread safety) */
		if (start < end)
			fwrite(start, end - start, 1, stdout);

		msg.consumed = 1;
	}

	return DA_OK;
}

int server_thread_query(int client_fd)
{
	struct p_msg client_msg;
	char *cmd, *args = NULL;

	struct pollfd worker_fd[workers];
	struct p_msg result;

	int ret = DA_INVALID_CMD;
	char *cmd_err = "Error in request.";

	/* Read cmd from client */
	msg_init(&client_msg);

	if (msg_read(client_fd, &client_msg) == DA_SOCK_ERROR) {
		cmd = strerror_r(errno, client_msg.buffer, sizeof(client_msg.buffer));
		fprintf(stderr, "read() cmd from client: %s\n", cmd);
		return DA_SOCK_ERROR;
	}

	msg_init(&result);

	/* Broadcast cmd (if valid) to workers and forward results to client */
	if ((cmd = strtok_r(client_msg.buffer, _whitespace, &args))) {
		result.pos += sprintf(result.buffer, "[%lu]: %s %s\n", pthread_self(), cmd, args);

		if (!strcmp(cmd, CMD_DISEASE_FREQUENCY))
			ret = s_disease_frequency(args, worker_fd);
		else if (!strcmp(cmd, CMD_TOPK_AGE_RANGES))
			ret = s_topk_age_ranges(args, worker_fd);
		else if (!strcmp(cmd, CMD_SEARCH_RECORD))
			ret = s_search_patient_record(args, worker_fd);
		else if (!strcmp(cmd, CMD_NUM_ADMISSIONS))
			ret = s_num_patients(ENTER, args, worker_fd);
		else if (!strcmp(cmd, CMD_NUM_DISCHARGES))
			ret = s_num_patients(EXIT, args, worker_fd);
	}

	if (ret == DA_OK) {
		if (!strcmp(cmd, CMD_DISEASE_FREQUENCY))
			ret = s_sum_cases(worker_fd, &result, client_fd);
		else
			ret = s_get_response(worker_fd, &result, client_fd);
	} else {
		result.pos += sprintf(result.pos, "%s\n", cmd_err);
		msg_write_line(client_fd, cmd_err);
	}

	puts(result.buffer);
	msg_done(client_fd);

	return ret;
}

int connect_to_workers(struct pollfd *worker_fd)
{
	int w;
	struct sockaddr_in to_worker;

	/* Open connection to worker, to forward query */
	to_worker.sin_family = AF_INET;
	to_worker.sin_addr.s_addr = worker_ip;

	for (w = 0; w < workers; ++w) {
		worker_fd[w].fd = socket(AF_INET, SOCK_STREAM, 0);
		worker_fd[w].events = POLLIN;

		to_worker.sin_port = worker_ports[w];
		//printf("Worker %d: Port %hu\n", w, worker_ports[w]);

		if (connect(worker_fd[w].fd, (struct sockaddr*) &to_worker, sizeof(to_worker)) == -1) {
			perror("server: connect() to worker");
			return DA_SOCK_ERROR;
		}
	}

	return DA_OK;
}

int s_disease_frequency(char *args, struct pollfd *worker_fd)
{
	char *disease, *date1, *date2, *country;
	char *saveptr;

	int w;

	if (!(disease = strtok_r(args, _whitespace, &saveptr)))
		return DA_INVALID_PARAMETER;

	if (!(date1 = strtok_r(NULL, _whitespace, &saveptr)))
		return DA_INVALID_PARAMETER;

	if (!(date2 = strtok_r(NULL, _whitespace, &saveptr)))
		return DA_INVALID_PARAMETER;

	if ((country = strtok_r(NULL, _whitespace, &saveptr))) {
		if (strtok_r(NULL, _whitespace, &saveptr))
			return DA_INVALID_PARAMETER;  /* Extra arguments: BAD */
	}

	if (connect_to_workers(worker_fd) != DA_OK)
		return DA_SOCK_ERROR;

	for (w = 0; w < workers; ++w) {
		msg_write_line(worker_fd[w].fd, CMD_NUM_ADMISSIONS);
		msg_write_line(worker_fd[w].fd, disease);
		msg_write_line(worker_fd[w].fd, date1);
		msg_write_line(worker_fd[w].fd, date2);

		if (country)
			msg_write_line(worker_fd[w].fd, country);

		msg_done(worker_fd[w].fd);
	}

	return DA_OK;
}

int s_topk_age_ranges(char *args, struct pollfd *worker_fd)
{
	char *k, *country, *disease, *date1, *date2;
	char *saveptr = NULL;

	int w;

	if (!(k = strtok_r(args, _whitespace, &saveptr)))
		return DA_INVALID_PARAMETER;

	if (strspn(k, "0123456789") != strlen(k))
		return DA_INVALID_PARAMETER;            /* k must be a number */

	if (!(country = strtok_r(NULL, _whitespace, &saveptr)))
		return DA_INVALID_PARAMETER;

	if (!(disease = strtok_r(NULL, _whitespace, &saveptr)))
		return DA_INVALID_PARAMETER;

	if (!(date1 = strtok_r(NULL, _whitespace, &saveptr)))
		return DA_INVALID_PARAMETER;

	if (!(date2 = strtok_r(NULL, _whitespace, &saveptr)))
		return DA_INVALID_PARAMETER;

	if (strtok_r(NULL, _whitespace, &saveptr))    /* Extra arguments: BAD */
		return DA_INVALID_PARAMETER;

	if (connect_to_workers(worker_fd) != DA_OK)
		return DA_SOCK_ERROR;

	for (w = 0; w < workers; ++w) {
		msg_write_line(worker_fd[w].fd, CMD_TOPK_AGE_RANGES);
		msg_write_line(worker_fd[w].fd, k);
		msg_write_line(worker_fd[w].fd, country);
		msg_write_line(worker_fd[w].fd, disease);
		msg_write_line(worker_fd[w].fd, date1);
		msg_write_line(worker_fd[w].fd, date2);

		msg_done(worker_fd[w].fd);
	}

	return DA_OK;
}

int s_search_patient_record(char *args, struct pollfd *worker_fd)
{
	char *record_id;
	char *saveptr = NULL;

	int w;

	if (!(record_id = strtok_r(args, _whitespace, &saveptr)))
		return DA_INVALID_PARAMETER;

	if (strtok_r(NULL, _whitespace, &saveptr))  /* Extra arguments: BAD */
		return DA_INVALID_PARAMETER;

	if (connect_to_workers(worker_fd) != DA_OK)
		return DA_SOCK_ERROR;

	for (w = 0; w < workers; ++w) {
		msg_write_line(worker_fd[w].fd, CMD_SEARCH_RECORD);
		msg_write_line(worker_fd[w].fd, record_id);

		msg_done(worker_fd[w].fd);
	}

	return DA_OK;
}

/* See README */
int s_num_patients(enum mode mode, char *args, struct pollfd *worker_fd)
{
	char *disease, *date1, *date2, *country;
	char *saveptr = NULL;

	int w;

	if (!(disease = strtok_r(args, _whitespace, &saveptr)))
		return DA_INVALID_PARAMETER;

	if (!(date1 = strtok_r(NULL, _whitespace, &saveptr)))
		return DA_INVALID_PARAMETER;

	if (!(date2 = strtok_r(NULL, _whitespace, &saveptr)))
		return DA_INVALID_PARAMETER;

	if ((country = strtok_r(NULL, _whitespace, &saveptr))) {
		if (strtok_r(NULL, _whitespace, &saveptr))
			return DA_INVALID_PARAMETER;  /* Extra arguments: BAD */
	}

	if (connect_to_workers(worker_fd) != DA_OK)
		return DA_SOCK_ERROR;

	for (w = 0; w < workers; ++w) {
		if (mode == ENTER)
			msg_write_line(worker_fd[w].fd, CMD_NUM_ADMISSIONS);
		else
			msg_write_line(worker_fd[w].fd, CMD_NUM_DISCHARGES);

		msg_write_line(worker_fd[w].fd, disease);
		msg_write_line(worker_fd[w].fd, date1);
		msg_write_line(worker_fd[w].fd, date2);

		if (country)
			msg_write_line(worker_fd[w].fd, country);

		msg_done(worker_fd[w].fd);
	}

	return DA_OK;
}

int s_get_response(struct pollfd *worker_fd, struct p_msg *result, int client_fd)
{
	struct p_msg worker_msg[workers];
	char *start, *end;
	int w, ready = 0, invalid = 0;
	int flags;

	int ret = DA_OK;

	for (w = 0; w < workers; ++w) {
		/* Set non-blocking mode for socket */
		flags = fcntl(worker_fd[w].fd, F_GETFL, 0);
		fcntl(worker_fd[w].fd, F_SETFL, flags | O_NONBLOCK);

		msg_init(worker_msg + w);
	}

	while (ready < workers) {
		if (poll(worker_fd, workers, TIMEOUT) < 0) {
			ret = DA_INVALID_PARAMETER;
			break;
		}

		for (w = 0; w < workers; ++w) {
			if (!(worker_fd[w].revents & POLLIN))
				continue;

			if (msg_read(worker_fd[w].fd, worker_msg + w) == DA_SOCK_ERROR) {
				start = strerror_r(errno, worker_msg[0].buffer, sizeof(worker_msg[0].buffer));
				fprintf(stderr, "read() response from worker: %s\n", start);
				return DA_SOCK_ERROR;
			}

			start = worker_msg[w].buffer;

			/* Find last position of '\0' in read data.
			   Then we can can handle the string (message) */
			end = memrchr(start, '\0', worker_msg[w].pos - start);

			if (!end)
				continue;         /* No complete messages yet */

			if (!strcmp(end - strlen(MSG_READY), MSG_READY)) {
				ready++;
				end -= strlen(MSG_READY) + 1;

				worker_fd[w].events = 0;
				close(worker_fd[w].fd);
			}

			/* Send (valid) worker result to client */
			if (start < end) {
				if (!strcmp(end - strlen(MSG_INVALID), MSG_INVALID)) {
					invalid++;
					ret = DA_INVALID_PARAMETER;
				} else {
					result->pos += sprintf(result->pos, "%s", start);
					msg_write(client_fd, start, strlen(start));
				}
			}

			worker_msg[w].consumed = 1;
		}
	}

	return ret;
}

int s_sum_cases(struct pollfd *worker_fd, struct p_msg *result, int client_fd)
{
	int flags;
	struct p_msg worker_msg[workers];
	char *start, *end, *saveptr = NULL;
	int w, ready = 0, invalid = 0;

	int n, cases = 0;
	int ret = DA_OK;

	for (w = 0; w < workers; ++w) {
		/* Set non-blocking mode for socket */
		flags = fcntl(worker_fd[w].fd, F_GETFL, 0);
		fcntl(worker_fd[w].fd, F_SETFL, flags | O_NONBLOCK);

		msg_init(worker_msg + w);
	}

	while (ready < workers) {
		if (poll(worker_fd, workers, TIMEOUT) < 0) {
			ret = DA_INVALID_PARAMETER;
			break;
		}

		for (w = 0; w < workers; ++w) {
			if (!(worker_fd[w].revents & POLLIN))
				continue;

			if (msg_read(worker_fd[w].fd, worker_msg + w) == DA_SOCK_ERROR) {
				start = strerror_r(errno, worker_msg[0].buffer, sizeof(worker_msg[0].buffer));
				fprintf(stderr, "read() response from worker: %s\n", start);
				return DA_SOCK_ERROR;
			}

			start = worker_msg[w].buffer;

			/* Find last position of '\0' in read data.
			   Then we can can handle the string (message) */
			end = memrchr(start, '\0', worker_msg[w].pos - start);

			if (!end)
				continue;         /* No complete messages yet */

			if (!strcmp(end - strlen(MSG_READY), MSG_READY)) {
				ready++;
				end -= strlen(MSG_READY) + 1;

				worker_fd[w].events = 0;
				close(worker_fd[w].fd);
			}

			/* Send (valid) worker result to client */
			if (start < end) {
				if (!strcmp(end - strlen(MSG_INVALID), MSG_INVALID)) {
					invalid++;
					ret = DA_INVALID_PARAMETER;
				} else {
					start = strtok_r(start, MSG_DELIMITER, &saveptr);
					while (start) {
						sscanf(start, "%*s %d", &n);
						cases += n;

						start = strtok_r(NULL, MSG_DELIMITER, &saveptr);
					}
				}
			}

			worker_msg[w].consumed = 1;
		}
	}

	/* Send back result to client */
	n = sprintf(result->pos, "%d\n", cases);
	msg_write(client_fd, result->pos, n);

	result->pos += n;

	return ret;
}
