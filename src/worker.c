#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#include "common.h"
#include "hashtable.h"
#include "pipes.h"
#include "tree.h"
#include "worker.h"

/* For length specifier in sscanf */
#define STR2(x) #x
#define STR(X) STR2(X)

/* Signal stuff */
static volatile sig_atomic_t check_for_new_files, worker_quit;
static struct sigaction sigact;

/* See README[12] */
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
int w_directories(char *args, char *input_dir, int response_fd);
int w_cmd_phase(char *input_dir, int request_fd, int response_fd);
int w_exit(char *input_dir, int requests_total, int requests_ok);

int w_directories_recheck(char* input_dir, int response_fd);
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
	int request_fd, response_fd;

	/* Setup Signal Handlers */
	sigact.sa_sigaction = w_sig_handler;

	sigaction(SIGUSR1, &sigact, NULL);
	sigaction(SIGINT, &sigact, NULL);
	sigaction(SIGQUIT, &sigact, NULL);

	ht_init(13, 13, 512);    /* 13 is a nice prime number for the buckets */

	/* Open pipes on worker end */
	snprintf(path, sizeof(path), "/tmp/p_request.%d", tag);
	request_fd = open(path, O_RDONLY);

	if (request_fd == -1) {
		perror("worker: request_fd");
		exit(DA_PIPE_ERROR);
	}

	snprintf(path, sizeof(path), "/tmp/p_response.%d", tag);
	response_fd = open(path, O_WRONLY);

	if (response_fd == -1) {
		perror("worker: response_fd");
		exit(DA_PIPE_ERROR);
	}

	w_cmd_phase(input_dir, request_fd, response_fd);

	close(response_fd);
	close(request_fd);

	ht_destroy();
	free(input_dir);

	return tag;                  /* Used from aggregator to replace child */
}

int w_directories(char *args, char* input_dir, int response_fd)
{
	struct dirent **file_list;
	int n, i;

	chdir(input_dir);

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

	chdir("..");

	return DA_OK;
}

int w_cmd_phase(char *input_dir, int request_fd, int response_fd)
{
	struct p_msg msg;
	char *cmd, *args;
	int ret = DA_OK;

	int requests_total = 0, requests_ok = 0;

	msg.pos = msg.buffer;                             /* Initialize p_msg */

	/* Loop forever REQ->, handle, RESP-> */
	for (;;) {
		if (check_for_new_files) {  /* SIGUSR1: New files, statistics */
			kill(getppid(), SIGUSR1);           /* See README[13] */

			w_directories_recheck(input_dir, response_fd);
			msg_ready(response_fd);

			check_for_new_files = 0;
		}

		if (worker_quit)          /* SIGINT, SIGQUIT: Make log & exit */
			break;

		/* Read request */
		if (msg_read(request_fd, &msg) == -1 && errno == EINTR)
			continue;                            /* Check SIGNALS */

		if (!(cmd = strtok(msg.buffer, MSG_DELIMITER)))
			continue;                              /* Empty input */

		if ((args = strtok(NULL, MSG_DELIMITER)))
			args[strlen(args)] = '\n';/* Replace strtok NULL byte */

		ret = DA_INVALID_PARAMETER;

		/* Depending on the kind of request, handle the request */
		if (!strcmp(cmd, CMD_EXIT))
			break;
		else if (!strcmp(cmd, CMD_DIRECTORIES))
			ret = w_directories(args, input_dir, response_fd);
		else if (!strcmp(cmd, CMD_LIST_COUNTRIES))
			ret = list_countries(response_fd);
		else if (!strcmp(cmd, CMD_TOPK_AGE_RANGES))
			ret = w_topk_age_ranges(args, response_fd);
		else if (!strcmp(cmd, CMD_SEARCH_RECORD))
			ret = w_search_patient_record(args, response_fd);
		else if (!strcmp(cmd, CMD_NUM_ADMISSIONS))
			ret = w_num_patients(ENTER, args, response_fd);
		else if (!strcmp(cmd, CMD_NUM_DISCHARGES))
			ret = w_num_patients(EXIT, args, response_fd);

		msg.consumed = 1;
		msg_ready(response_fd);

		requests_total++;

		if (ret == DA_INVALID_PARAMETER) {
			fprintf(stderr, "Invalid request: %s\n", cmd);
			msg_invalid(response_fd);
		}

		if (ret == DA_OK)
			requests_ok++;
	}

	return w_exit(input_dir, requests_total, requests_ok);
}

int w_directories_recheck(char* input_dir, int response_fd)
{
	struct bucket_entry *entry;
	struct dirent **file_list;
	int n, i;

	chdir(input_dir);

	entry = get_next_country(1);
	while (entry) {
		/* Get files in this directory in date order */
		n = scandir(entry->name, &file_list, str_datefilter, str_datecmp);

		if (n == -1)
			return DA_FILE_ERROR;

		for (i = 0; i < n; ++i) {
			if (!have_date_records(entry->tree, file_list[i]->d_name))
				w_insert_from_file(entry->name, file_list[i]->d_name, response_fd);

			free(file_list[i]);
		}

		free(file_list);
		entry = get_next_country(0);
	}

	chdir("..");

	return DA_OK;
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
