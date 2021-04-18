#include <arpa/inet.h>
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <netdb.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <unistd.h>

#include "common.h"
#include "master/hashtable.h"
#include "master/tree.h"
#include "master/worker.h"
#include "pipes.h"

/* For length specifier in sscanf */
#define STR2(x) #x
#define STR(X) STR2(X)

/* Signal stuff */
static volatile sig_atomic_t check_for_new_files, worker_quit;
static struct sigaction sigact;

static void w_sig_handler(int sig, siginfo_t *siginfo, void *context)
{
	switch (sig) {
	case SIGUSR1:
		check_for_new_files = 1;
		break;

	case SIGINT:
	case SIGQUIT:
		worker_quit = 1;
		break;
	};
}

/* Declarations */
/* Phases */
int w_master_phase(int tag, char *input_dir, int master_pipe);
int w_directories(char *args, char *input_dir, int response_fd);

int w_cmd_phase(char *input_dir, int request_socket);
int w_exit(char *input_dir, int requests_total, int requests_ok);

int w_insert_from_file(char *country, char *file, int response_fd);
int w_insert_record(char *country, char *file, char *record);

/* Commands */
int w_topk_age_ranges(char *args, int response_fd);
int w_search_patient_record(char *args, int response_fd);
int w_num_patients(enum mode, char *args, int response_fd);

int str_datecmp(const struct dirent ** file1, const struct dirent ** file2)
{
	struct date date1 = to_date((*(struct dirent **) file1)->d_name);
	struct date date2 = to_date((*(struct dirent **) file2)->d_name);

	return datecmp(&date1, &date2);
}

int str_datefilter(const struct dirent *file)
{
	if (file->d_type != DT_REG)
		return 0;

	return 1;
}

/* Implementation */
int worker(int tag, char *input_dir)
{
	char path[64];
	int master_pipe, request_sock;

	/* Setup Signal Handlers */
	sigact.sa_sigaction = w_sig_handler;

	sigaction(SIGINT, &sigact, NULL);
	sigaction(SIGQUIT, &sigact, NULL);

	ht_init(13, 13, 512);    /* 13 is a nice prime number for the buckets */

	/* Open pipes on worker end */
	snprintf(path, sizeof(path), "/tmp/p_request.%d", tag);
	master_pipe = open(path, O_RDONLY);

	if (master_pipe == -1) {
		perror("worker: master pipe open()");
		exit(DA_PIPE_ERROR);
	}

	request_sock = w_master_phase(tag, input_dir, master_pipe);
	w_cmd_phase(input_dir, request_sock);

	close(request_sock);

	ht_destroy();
	free(input_dir);

	return tag;                      /* Used from master to replace child */
}

/* Returns request_sock (CMD IN - from server) */
int w_master_phase(int tag, char *input_dir, int master_pipe)
{
	/* Needed to get information from master pipe */
	struct p_msg msg;
	char *countries, *server_ip;
	int server_port;

	/* Needed to open worker's listen() socket */
	int request_sock;
	struct sockaddr_in from_server;
	socklen_t len;

	/* Temporarily needed to send statistics to the server */
	int stats_sock;
	struct sockaddr_in to_server;
	char str[16];

	msg_init(&msg);

	do {
		msg_read(master_pipe, &msg);
	} while (strcmp(msg.pos - strlen(MSG_READY) - 1, MSG_READY));

	close(master_pipe);           /* No more requests from master process */

	/* Extract information from master's pipe message */
	countries = msg.buffer;

	/* Make socket to send statistics to server */
	server_ip = memchr(msg.buffer, '\0', msg.pos - msg.buffer);

	if (server_ip)
		server_ip++;

	server_ip = strtok(server_ip, MSG_DELIMITER);
	server_port = atoi(strtok(NULL, MSG_DELIMITER));

	/* Socket creation - CMD IN (from server) */
	if ((request_sock = socket(AF_INET, SOCK_STREAM, 0)) == -1) {
		perror("worker: request socket()");
		exit(DA_SOCK_ERROR);
	}

	from_server.sin_family = AF_INET;
	from_server.sin_addr.s_addr = htonl(INADDR_ANY); /* On all interfaces */
	from_server.sin_port = htons(0);                        /* Autoselect */

	if (bind(request_sock, (struct sockaddr*) &from_server, sizeof(from_server)) == -1) {
		perror("worker: bind()");
		exit(DA_SOCK_ERROR);
	}

	/* See README */
	if (listen(request_sock, SOMAXCONN) == -1) {
		perror("worker: listen()");
		exit(DA_SOCK_ERROR);
	}

	/* Socket creation - STATISTICS OUT (to server) */
	if ((stats_sock = socket(AF_INET, SOCK_STREAM, 0)) == -1) {
		perror("worker: statistics socket()");
		exit(DA_SOCK_ERROR);
	}

	to_server.sin_family = AF_INET;
	to_server.sin_addr.s_addr = inet_addr(server_ip);
	to_server.sin_port = htons(server_port);

	if (connect(stats_sock, (struct sockaddr*) &to_server, sizeof(to_server)) == -1) {
		perror("worker: connect()");
		exit(DA_SOCK_ERROR);
	}

	/* Send worker tag and listening port to the server */
	len = sizeof(from_server);
	getsockname(request_sock, (struct sockaddr*) &from_server, &len);

	snprintf(str, sizeof(str), "%d" MSG_DELIMITER "%hu",
	         tag, ntohs(from_server.sin_port));
	msg_write_line(stats_sock, str);
	msg_done(stats_sock);

	/* Fill data structures & send statistics */
	w_directories(countries, input_dir, stats_sock);

	msg_ready(stats_sock);

	close(stats_sock);                         /* Done sending statistics */

	return request_sock;
}

int w_directories(char *args, char* input_dir, int response_fd)
{
	struct dirent **file_list;
	int n, i;

	if (chdir(input_dir) == -1) {
		perror(input_dir);
		return DA_FILE_ERROR;
	}

	args = strtok(args, MSG_DELIMITER);
	while (args) {
		/* Get files in this directory (args) in date order */
		n = scandir(args, &file_list, str_datefilter, str_datecmp);

		if (n == -1)
			return DA_FILE_ERROR;

		for (i = 0; i < n; ++i) {
			w_insert_from_file(args, file_list[i]->d_name, response_fd);
			free(file_list[i]);
		}

		free(file_list);

		args = strtok(NULL, MSG_DELIMITER);
	}

	if (chdir("..") == -1) {
		perror("Parent directory (..)");
		return DA_FILE_ERROR;
	}

	return DA_OK;
}

int w_cmd_phase(char *input_dir, int request_sock)
{
	int query_fd;                               /* Returned from accept() */
	struct sockaddr_in from_server;
	socklen_t len;

	struct p_msg msg;
	char *cmd, *args;
	int ret = DA_OK;

	int requests_total = 0, requests_ok = 0;

	msg_init(&msg);

	/* Loop forever REQ->, handle, RESP-> */
	while (!worker_quit) {
		len = sizeof(from_server);
		query_fd = accept(request_sock, (struct sockaddr*) &from_server, &len);

		if (query_fd == -1) {
			if (errno == EINTR) {
				continue;                    /* Check SIGNALS */
			} else {
				perror("worker: accept()");
				exit(DA_SOCK_ERROR);
			}
		}

		/* Read request */
		while (msg_read(query_fd, &msg) == -1 && errno == EINTR) {}

		if (!(cmd = strtok_r(msg.buffer, MSG_DELIMITER, &args)))
			continue;                              /* Empty input */

		ret = DA_INVALID_CMD;

		/* Depending on the kind of request, handle the request */
		if (!strcmp(cmd, CMD_EXIT))
			break;
		//else if (!strcmp(cmd, CMD_DIRECTORIES))
			//ret = w_directories(args, input_dir, query_fd);
		else if (!strcmp(cmd, CMD_LIST_COUNTRIES)) /* Easter egg for nc */
			ret = list_countries(query_fd);
		else if (!strcmp(cmd, CMD_TOPK_AGE_RANGES))
			ret = w_topk_age_ranges(args, query_fd);
		else if (!strcmp(cmd, CMD_SEARCH_RECORD))
			ret = w_search_patient_record(args, query_fd);
		else if (!strcmp(cmd, CMD_NUM_ADMISSIONS))
			ret = w_num_patients(ENTER, args, query_fd);
		else if (!strcmp(cmd, CMD_NUM_DISCHARGES))
			ret = w_num_patients(EXIT, args, query_fd);

		msg.consumed = 1;

		if (ret == DA_INVALID_CMD) {
			fprintf(stderr, "Invalid request: %s\n", cmd);
			msg_invalid(query_fd);
		}

		msg_ready(query_fd);

		requests_total++;

		if (ret == DA_OK)
			requests_ok++;

		close(query_fd);                      /* Done with this query */
	}

	return w_exit(input_dir, requests_total, requests_ok);
}

int w_insert_from_file(char *country, char *file, int response_fd)
{
	FILE *records_file;
	char record[1024];

	/* e.g. China/29-03-2017 */
	snprintf(record, sizeof(record), "%s/%s", country, file);

	if (!(records_file = fopen(record, "r")))
		return DA_FILE_ERROR;

	while (fgets(record, sizeof(record), records_file)) {
		if (w_insert_record(country, file, record) != DA_OK)
			fputs("ERROR\n", stderr);
	}

	fclose(records_file);

	return file_statistics(country, file, response_fd);
}

int w_insert_record(char *country, char *file, char *record)
{
	/* Temporary storage for disease_id and country.
	 * Dedicated space allocated later, in the buckets. */
	char record_id[FIELD_SIZE];
	char status[FIELD_SIZE];
	char first_name[FIELD_SIZE];
	char last_name[FIELD_SIZE];
	char disease_id[FIELD_SIZE];
	struct record tmp = {
		NULL,
		record_id, first_name, last_name,
		disease_id, country
	};

	if (!country || !file || !record)
		return DA_INVALID_PARAMETER;

	if (sscanf(record, "%" STR(FIELD_SIZE) "s "
		"%" STR(FIELD_SIZE) "s "
		"%" STR(FIELD_SIZE) "s "
		"%" STR(FIELD_SIZE) "s "
		"%" STR(FIELD_SIZE) "s "
		"%d",
		tmp.record_id, status, tmp.first_name, tmp.last_name,
		tmp.disease_id, &tmp.age) != 6) {
		fprintf(stderr, "There are fields missing from the record [%s]\n", record);
		return DA_INVALID_RECORD;
	}

	if (tmp.age < 0 || tmp.age > 120)
		return DA_INVALID_RECORD;

	if (!strcmp(status, "ENTER"))
		tmp.entry_date = to_date(file);
	else if (!strcmp(status, "EXIT"))
		tmp.exit_date = to_date(file);
	else
		return DA_INVALID_RECORD;

	return insert_record(&tmp);
}

/* Commands */
int w_topk_age_ranges(char *args, int response_fd)
{
	char *str_k, *country, *disease, *str_date1, *str_date2;
	int k;
	struct date date1, date2;

	if (!(str_k = strtok(args, MSG_DELIMITER)))
		return DA_INVALID_PARAMETER;

	k = atoi(str_k);

	if (!(country = strtok(NULL, MSG_DELIMITER)))
		return DA_INVALID_PARAMETER;

	if (!(disease = strtok(NULL, MSG_DELIMITER)))
		return DA_INVALID_PARAMETER;

	if (!(str_date1 = strtok(NULL, MSG_DELIMITER)))
		return DA_INVALID_PARAMETER;

	date1 = to_date(str_date1);

	if (!(str_date2 = strtok(NULL, MSG_DELIMITER)))
		return DA_INVALID_PARAMETER;

	date2 = to_date(str_date2);

	return topk_age_ranges(k, country, disease, &date1, &date2, response_fd);
}

int w_search_patient_record(char *args, int response_fd)
{
	struct record *patient_record;
	char *record_id, printed_record[1024];

	if (!(record_id = strtok(args, MSG_DELIMITER)))
		return DA_INVALID_PARAMETER;

	if (!(patient_record = record_get(record_id)))
		return DA_INVALID_RECORD;

	/* Because dates are nullified from tmp stage (w_insert_record) */
	sprintf(printed_record, "%s %s %s %s %d %.2s-%.2s-%.4s %.2s-%.2s-%.4s\n",
	        patient_record->record_id,
		patient_record->first_name,
		patient_record->last_name,
		patient_record->disease_id,
		patient_record->age,
		patient_record->entry_date.day,
		patient_record->entry_date.month,
		patient_record->entry_date.year,
		patient_record->exit_date.day,
		patient_record->exit_date.month,
		patient_record->exit_date.year);

	msg_write(response_fd, printed_record, strlen(printed_record) + 1);
	return DA_OK;
}

int w_num_patients(enum mode mode, char *args, int response_fd)
{
	char *disease, *str_date1, *str_date2, *country;
	struct date date1, date2;

	if (!(disease = strtok(args, MSG_DELIMITER)))
		return DA_INVALID_PARAMETER;

	if (!(str_date1 = strtok(NULL, MSG_DELIMITER)))
		return DA_INVALID_PARAMETER;

	date1 = to_date(str_date1);

	if (!(str_date2 = strtok(NULL, MSG_DELIMITER)))
		return DA_INVALID_PARAMETER;

	date2 = to_date(str_date2);

	country = strtok(NULL, MSG_DELIMITER);

	if (mode == ENTER)
		return num_patient_admissions(disease, &date1, &date2, country, response_fd);
	else
		return num_patient_discharges(disease, &date1, &date2, country, response_fd);
}

int w_exit(char *input_dir, int requests_total, int requests_ok)
{
	char path[64];
	FILE *log;

	struct bucket_entry *entry;

	mkdir("logs", 0755);

	snprintf(path, sizeof(path), "logs/log_file.%d", getpid());
	log = fopen(path, "w");

	if (!log)
		return DA_FILE_ERROR;

	entry = get_next_country(1);

	while (entry) {
		fprintf(log, "%s\n", entry->name);
		entry = get_next_country(0);
	}

	fprintf(log, "TOTAL %d\n", requests_total);
	fprintf(log, "SUCCESS %d\n", requests_ok);
	fprintf(log, "FAIL %d\n", requests_total - requests_ok);

	fclose(log);

	return DA_OK;
}
