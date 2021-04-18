#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

#include "common.h"
#include "master/master.h"
#include "master/worker.h"
#include "pipes.h"

/* Signal stuff */
static volatile sig_atomic_t master_quit, worker_died;
static struct sigaction sigact;

static void m_sig_handler(int sig, siginfo_t *siginfo, void *context)
{
	switch (sig) {
	case SIGINT:
	case SIGQUIT:
		master_quit = 1;
		break;

	case SIGCHLD:
		worker_died = 1;
		break;
	};
}

/* Directories announced to workers */
static struct country_entry {
	char *name;
	struct country_entry *next;
} **countries;

/* Declarations */
int m_assign_directories(char *input_dir, int workers);
int spawn_worker(int workers, char *input_dir, int w, char *server_ip, char *server_port, int *pid, int *request_fd);
int m_exit(char *input_dir, int workers, int *pid);

/* Implementation */
int master(int workers, int buffer_size, char *server_ip, char *server_port, char *input_dir)
{
	int request_fd[workers];            /* Request (directories) pipe fds */
	int pid[workers], w, child_pid;

	/* Setup Signal Handlers */
	sigact.sa_sigaction = m_sig_handler;

	sigaction(SIGINT, &sigact, NULL);
	sigaction(SIGQUIT, &sigact, NULL);
	sigaction(SIGCHLD, &sigact, NULL);

	pipes_init(buffer_size);

	m_assign_directories(input_dir, workers);

	/* Spawn Workers */
	for (w = 0; w < workers; ++w) {
		spawn_worker(workers, input_dir, w, server_ip, server_port, pid, request_fd);
	}

	/* Wait for signals (Maybe a worker died...) */
	while (!master_quit) {
		if (worker_died) {
			/* Get return value of child (tag) */
			child_pid = wait(&w);

			/* Get worker tag */
			if (WIFEXITED(w)) {
				w = WEXITSTATUS(w);
			} else {
				for (w = 0; w < workers; ++w) {
					if (child_pid == pid[w])
						break;
				}
			}

			/* Replace dead worker */
			spawn_worker(workers, input_dir, w, server_ip, server_port, pid, request_fd);

			worker_died = 0;
		}

		pause();
	}

	m_exit(input_dir, workers, pid);

	free(server_ip);
	free(input_dir);

	return DA_OK;
}

int m_assign_directories(char *input_dir, int workers)
{
	DIR *dir;
	struct dirent *entry;

	struct country_entry *new;
	int w;

	if (!(countries = calloc(workers, sizeof(countries[0]))))
		return DA_ALLOCATION_ERROR;

	if (!(dir = opendir(input_dir)))
		return DA_FILE_ERROR;

	/* Iterate though directories and assign */
	w = 0;
	while ((entry = readdir(dir))) {
		if (entry->d_type == DT_DIR) {
			if (!strcmp(entry->d_name, ".") || !strcmp(entry->d_name, ".."))
				continue;

			new = malloc(sizeof(*new));

			new->name = strdup(entry->d_name);
			new->next = countries[w];

			countries[w] = new;

			/* Round Robin */
			if (++w == workers)
				w = 0;
		}
	}

	closedir(dir);

	return DA_OK;
}

int spawn_worker(int workers, char *input_dir, int w, char *server_ip, char *server_port, int *pid, int *request_fd)
{
	char p_request[64];                            /* Named pipe filename */
	struct country_entry *entry, *next;
	int i, ret;

	if (w >= workers)
		return DA_INVALID_PARAMETER;

	/* Create and open pipes on parent end */
	snprintf(p_request, sizeof(p_request), "/tmp/p_request.%d", w);
	mkfifo(p_request, 0600);

	/* Fork:
	* Worker will open request pipe (R) on the other side */
	pid[w] = fork();
	if (!pid[w]) {                                 /* Worker Path */
		ret = worker(w, input_dir);

		/* Free data structures left over from parent */
		if (countries) {
			for (i = 0; i < workers; ++i) {
				entry = countries[i];
				while (entry) {
					next = entry->next;

					free(entry->name);
					free(entry);

					entry = next;
				}
			}

			free(countries);
			free(server_ip);
		}

		exit(ret);
	} else if (pid[w] < 0) {
		perror("master: fork()");
		return DA_ALLOCATION_ERROR;
	}

	/* Master Path */

	/* Open request pipe (W) */
	request_fd[w] = open(p_request, O_WRONLY);

	if (request_fd[w] == -1) {
		perror("master: request_fd");
		exit(DA_PIPE_ERROR);
	}

	/* Send the assigned directories */
	entry = countries[w];
	while (entry) {
		msg_write_line(request_fd[w], entry->name);
		entry = entry->next;
	}

	msg_done(request_fd[w]);

	/* Announce server host:port */
	msg_write_line(request_fd[w], server_ip);
	msg_write_line(request_fd[w], server_port);
	msg_done(request_fd[w]);

	msg_ready(request_fd[w]);
	close(request_fd[w]);

	return DA_OK;
}

int m_exit(char *input_dir, int workers, int *pid)
{
	char path[64];
	int w;

	struct country_entry *entry, *next;

	/* SIGKILL to workers, delete named pipes */
	for (w = 0; w < workers; ++w) {
		kill(pid[w], SIGKILL);
		wait(NULL);

		snprintf(path, sizeof(path), "/tmp/p_request.%d", w);
		unlink(path);

		/* Free Worker Countries */
		entry = countries[w];

		while (entry) {
			next = entry->next;

			free(entry->name);
			free(entry);

			entry = next;
		}
	}

	free(countries);

	return DA_OK;
}
