#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <poll.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>

#define __USE_GNU /* For memrchr */
#include <string.h>
#undef __USE_GNU

#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

#include "aggregator.h"
#include "common.h"
#include "pipes.h"
#include "worker.h"

/* Signal stuff */
static volatile sig_atomic_t read_new_files, parent_quit, child_died;
static struct sigaction sigact;

/* See README[12] */
static void agr_sig_handler(int sig, siginfo_t *siginfo, void *context)
{
	switch (sig) {
	case SIGUSR1:
		read_new_files = siginfo->si_pid;
		break;

	case SIGINT:
	case SIGQUIT:
		parent_quit = 1;
		break;

	case SIGCHLD:
		child_died = 1;
		break;
	};
}

static const char _whitespace[] = " \f\n\r\t\v";

/* Directories announced to workers */
static struct country_entry {
	char *name;
	struct country_entry *next;
} **countries;

/* Declarations */
int spawn_worker(int workers, char *input_dir, int w, int *pid, struct pollfd *request, struct pollfd *response, struct p_msg **msg);

/* Phases */
int agr_directories(char *input_dir, int workers, struct pollfd *request, struct pollfd *response, struct p_msg **msg);
int agr_cmd_phase(char *input_dir, int workers, int *pid, struct pollfd *request, struct pollfd *response, struct p_msg **msg);
int agr_exit(char *input_dir, int workers, int *pid, int requests_total, int requests_ok);

/* Commands */
void agr_print_usage(void);
int agr_list_countries(char *args, int workers, struct pollfd *request, struct pollfd *response, struct p_msg **msg);
int agr_disease_frequency(char *args, int workers, struct pollfd *request, struct pollfd *response, struct p_msg **msg);
int agr_topk_age_ranges(char *args, int workers, struct pollfd *request, struct pollfd *response, struct p_msg **msg);
int agr_search_patient_record(char *args, int workers, struct pollfd *request, struct pollfd *response, struct p_msg **msg);
int agr_num_patients(enum mode, char *args, int workers, struct pollfd *request, struct pollfd *response, struct p_msg **msg);

int agr_get_response(int workers, int target, struct pollfd *response, struct p_msg **msg);

/* Implementation */
int aggregator(int workers, int buffer_size, char *input_dir)
{
	struct pollfd request[workers];                   /* Request pipe fds */
	struct pollfd response[workers];                 /* Response pipe fds */
	struct p_msg *msg[workers]; /* Chonky buffers to reconstruct messages */

	int pid[workers], w;

	/* Setup Signal Handlers */
	sigact.sa_sigaction = agr_sig_handler;
	sigact.sa_flags = SA_SIGINFO;

	sigaction(SIGUSR1, &sigact, NULL);
	sigaction(SIGINT, &sigact, NULL);
	sigaction(SIGQUIT, &sigact, NULL);
	sigaction(SIGCHLD, &sigact, NULL);

	pipes_init(buffer_size);

	for (w = 0; w < workers; ++w)
		msg[w] = NULL;

	for (w = 0; w < workers; ++w) {
		spawn_worker(workers, input_dir, w, pid, request, response, msg);
	}

	agr_directories(input_dir, workers, request, response, msg);
	agr_cmd_phase(input_dir, workers, pid, request, response, msg);

	while (workers--) {
		free(msg[workers]);
		close(response[workers].fd);
		close(request[workers].fd);
	}

	free(input_dir);

	return DA_OK;
}

int spawn_worker(int workers, char *input_dir, int w, int *pid, struct pollfd *request, struct pollfd *response, struct p_msg **msg)
{
	char p_request[64], p_response[64];            /* Named pipe filename */
	struct country_entry *entry, *next;
	int i, ret;

	if (w >= workers)
		return DA_INVALID_PARAMETER;

	/* Create and open pipes on parent end */
	snprintf(p_request, sizeof(p_request), "/tmp/p_request.%d", w);
	mkfifo(p_request, 0600);

	snprintf(p_response, sizeof(p_response), "/tmp/p_response.%d", w);
	mkfifo(p_response, 0600);

	/* Fork:
	* Worker will open request pipe (R) on the other side */
	pid[w] = fork();
	if (!pid[w]) {                                 /* Worker Path */
		ret = worker(w, input_dir);

		/* Free data structures left over from parent */
		for (i = 0; i < workers; ++i) {
			if (msg[i])
				free(msg[i]);
		}

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
		}

		exit(ret);
	} else if (pid[w] < 0) {
		perror("fork() failed");
		return DA_ALLOCATION_ERROR;
	}

	/* Aggregator Path */

	/* Open request pipe (W) */
	request[w].fd = open(p_request, O_WRONLY);

	if (request[w].fd == -1) {
		perror("aggregator: request_fd");
		exit(DA_PIPE_ERROR);
	}

	/* Open response pipe (R - polling) */
	response[w].fd = open(p_response, O_RDONLY | O_NONBLOCK);
	response[w].events = POLLIN;

	if (response[w].fd == -1) {
		perror("aggregator: response_fd");
		exit(DA_PIPE_ERROR);
	}

	/* Initialize p_msg for this worker */
	if (!(msg[w] = malloc(sizeof(**msg))))
		return DA_ALLOCATION_ERROR;

	msg[w]->pos = msg[w]->buffer;
	msg[w]->consumed = 0;

	return DA_OK;
}

int agr_directories(char *input_dir, int workers, struct pollfd *request, struct pollfd *response, struct p_msg **msg)
{
	DIR *dir;
	struct dirent *entry;

	struct country_entry *new;
	int w;

	if (!(countries = calloc(workers, sizeof(countries[0]))))
		return DA_ALLOCATION_ERROR;

	if (!(dir = opendir(input_dir)))
		return DA_FILE_ERROR;

	/* Iterate though directories, assign directories */
	for (w = 0; w < workers; w++)
		msg_write_line(request[w].fd, CMD_DIRECTORIES);

	w = 0;
	while ((entry = readdir(dir))) {
		if (entry->d_type == DT_DIR) {
			if (!strcmp(entry->d_name, ".") || !strcmp(entry->d_name, ".."))
				continue;

			new = malloc(sizeof(*new));

			new->name = strdup(entry->d_name);
			new->next = countries[w];

			countries[w] = new;

			msg_write_line(request[w].fd, entry->d_name);

			/* Round Robin */
			if (++w == workers)
				w = 0;
		}
	}

	closedir(dir);

	for (w = 0; w < workers; w++)
		msg_done(request[w].fd);

	return agr_get_response(workers, workers, response, msg);
}

void agr_print_usage()
{
	fprintf(stderr,
		"\x1b[1mCommands:\x1b[0m\n"
		CMD_LIST_COUNTRIES "\n"
		CMD_DISEASE_FREQUENCY " virusName date1 date2 [country]\n"
		CMD_TOPK_AGE_RANGES " k country disease date1 date2\n"
		CMD_SEARCH_RECORD " recordID\n"
		CMD_NUM_ADMISSIONS " disease date1 date2 [country]\n"
		CMD_NUM_DISCHARGES " disease date1 date2 [country]\n"
		CMD_EXIT "\n");
}

int agr_cmd_phase(char *input_dir, int workers, int *pid, struct pollfd *request, struct pollfd *response, struct p_msg **msg)
{
	char *cmd, *args;
	char buffer[1024];
	int ret = DA_OK;

	int w;
	struct country_entry *entry;

	int requests_total = 0, requests_ok = 0;

	for (;;) {
		if (read_new_files) {     /* SIGUSR1: Statistics for new file */
			for (w = 0; w < workers; ++w) {
				if (pid[w] == read_new_files)
					/* Statistics from 1 worker, w */
					agr_get_response(workers, 1, response, msg);
			}

			read_new_files = 0;
		}

		if (parent_quit)          /* SIGINT, SIGQUIT: Make log & exit */
			break;

		if (child_died) {            /* SICHLD: Respawn exited worker */
			ret = wait(&w);      /* Get return value of child (the tag) */

			/* Get worker tag */
			if (WIFEXITED(w)) {
				w = WEXITSTATUS(w);
			} else {
				for (w = 0; w < workers; ++w) {
					if (ret == pid[w])
						break;
				}
			}

			close(request[w].fd);
			close(response[w].fd);
			free(msg[w]);
			msg[w] = NULL;

			/* Spawn new worker */
			spawn_worker(workers, input_dir, w, pid, request, response, msg);

			/* Resend the directories */
			msg_write_line(request[w].fd, CMD_DIRECTORIES);

			entry = countries[w];
			while (entry) {
				msg_write_line(request[w].fd, entry->name);
				entry = entry->next;
			}

			msg_done(request[w].fd);

			/* Ignore statistics, we know them */
			while (poll(&(struct pollfd){response[w].fd, POLLIN}, 1, TIMEOUT) > 0) {
				ret = read(response[w].fd, buffer, sizeof(buffer));

				if (!strcmp(buffer + ret - (strlen(MSG_READY) + 1), MSG_READY))
					break;
			}

			child_died = 0;
		}

		fputs("> ", stderr);                            /* CMD Prompt */
		if (!(fgets(buffer, sizeof(buffer), stdin))) {
			if (errno == EINTR)
				continue;                    /* Check SIGNALS */
			else
				break;                       /* Handle Ctrl+D */
		}

		if (!(cmd = strtok(buffer, _whitespace)))
			continue;                              /* Empty input */

		if ((args = strtok(NULL, _whitespace)))
			args[strlen(args)] = ' '; /* Replace strtok NULL byte */

		ret = DA_INVALID_PARAMETER;

		if (!strcmp(cmd, CMD_EXIT))
			break;
		else if (!strcmp(cmd, CMD_LIST_COUNTRIES))
			ret = agr_list_countries(args, workers, request, response, msg);
		else if (!strcmp(cmd, CMD_DISEASE_FREQUENCY))
			ret = agr_disease_frequency(args, workers, request, response, msg);
		else if (!strcmp(cmd, CMD_TOPK_AGE_RANGES))
			ret = agr_topk_age_ranges(args, workers, request, response, msg);
		else if (!strcmp(cmd, CMD_SEARCH_RECORD))
			ret = agr_search_patient_record(args, workers, request, response, msg);
		else if (!strcmp(cmd, CMD_NUM_ADMISSIONS))
			ret = agr_num_patients(ENTER, args, workers, request, response, msg);
		else if (!strcmp(cmd, CMD_NUM_DISCHARGES))
			ret = agr_num_patients(EXIT, args, workers, request, response, msg);

		requests_total++;

		if (ret == DA_INVALID_PARAMETER)
			agr_print_usage();

		if (ret == DA_OK)
			requests_ok++;
	}

	return agr_exit(input_dir, workers, pid, requests_total, requests_ok);
}

/* Commands */
int agr_list_countries(char *args, int workers, struct pollfd *request, struct pollfd *response, struct p_msg **msg)
{
	int w;

	if (args)                                     /* Extra arguments: BAD */
		return DA_INVALID_PARAMETER;

	for (w = 0; w < workers; ++w)
		msg_write(request[w].fd, CMD_LIST_COUNTRIES MSG_DELIMITER, 16);

	return agr_get_response(workers, workers, response, msg);
}

int agr_disease_frequency(char *args, int workers, struct pollfd *request, struct pollfd *response, struct p_msg **msg)
{
	char *disease, *date1, *date2, *country;
	int n, cases = 0;

	int w, ready = 0, invalid = 0;
	char *start, *end;

	if (!(disease = strtok(args, _whitespace)))
		return DA_INVALID_PARAMETER;

	if (!(date1 = strtok(NULL, _whitespace)))
		return DA_INVALID_PARAMETER;

	if (!(date2 = strtok(NULL, _whitespace)))
		return DA_INVALID_PARAMETER;

	if ((country = strtok(NULL, _whitespace))) {
		if (strtok(NULL, _whitespace))        /* Extra arguments: BAD */
			return DA_INVALID_PARAMETER;
	}

	for (w = 0; w < workers; ++w) {
		msg_write_line(request[w].fd, CMD_NUM_ADMISSIONS);
		msg_write_line(request[w].fd, disease);
		msg_write_line(request[w].fd, date1);
		msg_write_line(request[w].fd, date2);

		if (country)
			msg_write_line(request[w].fd, country);

		msg_done(request[w].fd);
	}

	/* Poll: Add up results from workers */
	while (poll(response, workers, TIMEOUT) > 0) {
		/* Find the pipe for reading */
		for (w = 0; w < workers; ++w) {
			if (!(response[w].revents & POLLIN))
				continue;        /* Nothing to read (for now) */

			msg_read(response[w].fd, msg[w]);

			/* Find last position of '\0' in read data.
			   Then we can can print the strings (messages) */
			start = msg[w]->buffer;
			end = memrchr(start, '\0', msg[w]->pos - start);

			if (!end)
				continue;         /* No complete messages yet */

			if (!strcmp(end - strlen(MSG_READY), MSG_READY)) {
				ready++;
				end -= strlen(MSG_READY) + 1;
			} else if (!strcmp(end - strlen(MSG_INVALID), MSG_INVALID)) {
				invalid++;
				end -= strlen(MSG_INVALID) + 1;
			}

			start = strtok(start, MSG_DELIMITER);
			while (start) {
				sscanf(start, "%*s %d", &n);
				cases += n;

				start = strtok(NULL, MSG_DELIMITER);
			}

			msg[w]->consumed = 1;
		}

		/* Don't wait for the poll timeout if every worker is done */
		if (ready == workers)
			break;
	}

	printf("%d\n", cases);

	return DA_OK;
}

int agr_topk_age_ranges(char *args, int workers, struct pollfd *request, struct pollfd *response, struct p_msg **msg)
{
	char *k, *country, *disease, *date1, *date2;
	int w;

	if (!(k = strtok(args, _whitespace)))
		return DA_INVALID_PARAMETER;

	if (strspn(k, "0123456789") != strlen(k))
		return DA_INVALID_PARAMETER;            /* k must be a number */

	if (!(country = strtok(NULL, _whitespace)))
		return DA_INVALID_PARAMETER;

	if (!(disease = strtok(NULL, _whitespace)))
		return DA_INVALID_PARAMETER;

	if (!(date1 = strtok(NULL, _whitespace)))
		return DA_INVALID_PARAMETER;

	if (!(date2 = strtok(NULL, _whitespace)))
		return DA_INVALID_PARAMETER;

	if (strtok(NULL, _whitespace))                /* Extra arguments: BAD */
		return DA_INVALID_PARAMETER;

	for (w = 0; w < workers; ++w) {
		msg_write_line(request[w].fd, CMD_TOPK_AGE_RANGES);
		msg_write_line(request[w].fd, k);
		msg_write_line(request[w].fd, country);
		msg_write_line(request[w].fd, disease);
		msg_write_line(request[w].fd, date1);
		msg_write_line(request[w].fd, date2);

		msg_done(request[w].fd);
	}

	return agr_get_response(workers, workers, response, msg);
}

int agr_search_patient_record(char *args, int workers, struct pollfd *request, struct pollfd *response, struct p_msg **msg)
{
	char *record_id;
	int w;

	if (!(record_id = strtok(args, _whitespace)))
		return DA_INVALID_PARAMETER;

	if (strtok(NULL, _whitespace))                /* Extra arguments: BAD */
		return DA_INVALID_PARAMETER;

	for (w = 0; w < workers; ++w) {
		msg_write_line(request[w].fd, CMD_SEARCH_RECORD);
		msg_write_line(request[w].fd, record_id);

		msg_done(request[w].fd);
	}

	return agr_get_response(workers, workers, response, msg);
}

int agr_num_patients(enum mode mode, char *args, int workers, struct pollfd *request, struct pollfd *response, struct p_msg **msg)
{
	char *disease, *date1, *date2, *country;
	int w;

	if (!(disease = strtok(args, _whitespace)))
		return DA_INVALID_PARAMETER;

	if (!(date1 = strtok(NULL, _whitespace)))
		return DA_INVALID_PARAMETER;

	if (!(date2 = strtok(NULL, _whitespace)))
		return DA_INVALID_PARAMETER;

	if ((country = strtok(NULL, _whitespace))) {
		if (strtok(NULL, _whitespace))        /* Extra arguments: BAD */
			return DA_INVALID_PARAMETER;
	}

	for (w = 0; w < workers; ++w) {
		if (mode == ENTER)
			msg_write_line(request[w].fd, CMD_NUM_ADMISSIONS);
		else
			msg_write_line(request[w].fd, CMD_NUM_DISCHARGES);

		msg_write_line(request[w].fd, disease);
		msg_write_line(request[w].fd, date1);
		msg_write_line(request[w].fd, date2);

		if (country)
			msg_write_line(request[w].fd, country);

		msg_done(request[w].fd);
	}

	return agr_get_response(workers, workers, response, msg);
}

int agr_get_response(int workers, int target, struct pollfd *response, struct p_msg **msg)
{
	int w, ready = 0, invalid = 0;
	char *start, *end;

	/* Poll: Response from workers */
	while (poll(response, workers, TIMEOUT) > 0) {
		/* Find the pipe for reading */
		for (w = 0; w < workers; ++w) {
			if (!(response[w].revents & POLLIN))
				continue;        /* Nothing to read (for now) */

			msg_read(response[w].fd, msg[w]);

			/* Find last position of '\0' in read data.
			   Then we can can print the strings (messages) */
			start = msg[w]->buffer;
			end = memrchr(start, '\0', msg[w]->pos - start);

			if (!end)
				continue;         /* No complete messages yet */

			if (!strcmp(end - strlen(MSG_READY), MSG_READY)) {
				ready++;
				end -= strlen(MSG_READY) + 1;
			} else if (!strcmp(end - strlen(MSG_INVALID), MSG_INVALID)) {
				invalid++;
				end -= strlen(MSG_INVALID) + 1;
			}

			if (end > start)
				write(STDOUT_FILENO, start, end - start);

			msg[w]->consumed = 1;
		}

		/* Don't wait for the poll timeout if every worker is done */
		if (ready == target)
			return DA_OK;
	}

	return DA_INVALID_PARAMETER;                       /* Reached timeout */
}

int agr_exit(char *input_dir, int workers, int *pid, int requests_total, int requests_ok)
{
	char path[64];
	int w;

	FILE *log;
	struct country_entry *entry, *next;

	/* SIGKILL to workers, delete named pipes */
	for (w = 0; w < workers; ++w) {
		kill(pid[w], SIGKILL);
		wait(NULL);

		snprintf(path, sizeof(path), "/tmp/p_request.%d", w);
		unlink(path);

		snprintf(path, sizeof(path), "/tmp/p_response.%d", w);
		unlink(path);
	}

	/* Make log */
	mkdir("logs", 0755);
	snprintf(path, sizeof(path), "logs/log_file.%d", getpid());

	if (!(log = fopen(path, "w")))
		return DA_FILE_ERROR;

	for (w = 0; w < workers; ++w) {
		entry = countries[w];

		while (entry) {
			next = entry->next;

			fprintf(log, "%s\n", entry->name);
			free(entry->name);
			free(entry);

			entry = next;
		}
	}

	free(countries);

	fprintf(log, "TOTAL %d\n", requests_total);
	fprintf(log, "SUCCESS %d\n", requests_ok);
	fprintf(log, "FAIL %d\n", requests_total - requests_ok);

	fclose(log);

	return DA_OK;
}
