#include <getopt.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "commands.h"
#include "common.h"

//static const char _prompt[] = "\x1b[1;31m$\x1b[0m ";
static const char _whitespace[] = " \f\n\r\t\v";

int print_usage(const char *program);
int file_phase(char *filename);
int cmd_phase(void);

int main(int argc, char *argv[])
{
	/* Program parameters */
	struct option long_options[3] = {
		{"h1", required_argument, NULL, 'd'},
		{"h2", required_argument, NULL, 'c'},
		{0}
	};
	char *filename;
	int ret, opt, disease_entries = 0, country_entries = 0, bucket_size = 0;

	/* Argument Parsing */
	if (argc < 9)
		return print_usage(argv[0]);

	while ((opt = getopt_long_only(argc, argv, "p:b:", long_options, NULL)) != -1) {
		switch (opt) {
		case 'p':
			filename = strdup(optarg);
			break;
		case 'd':
			disease_entries = atoi(optarg);
			break;
		case 'c':
			country_entries = atoi(optarg);
			break;
		case 'b':
			bucket_size = atoi(optarg);
			break;
		default:
			return print_usage(argv[0]);
		}
	}

	if (disease_entries <= 0 || country_entries <= 0 || bucket_size <= 0)
		return print_usage(argv[0]);

	ret = cmd_init(disease_entries, country_entries, bucket_size);
	if (ret)
		return cmd_exit(ret);

	ret = file_phase(filename);               /* Import records from file */
	free(filename);
	if (ret)
		return cmd_exit(ret);

	return cmd_phase();                             /* Command Line Phase */
}

int print_usage(const char *program)
{
	fprintf(stderr,
	        "Usage: %s "
	        "-p patientRecordsFile –h1 diseaseHashtableNumOfEntries "
	        "–h2 countryHashtableNumOfEntries –b bucketSize\n",
	        program);
	return DM_INVALID_PARAMETER;
}

int file_phase(char *filename)
{
	FILE *records_file;
	char buffer[1024];
	int ret = 0;

	records_file = fopen(filename, "r");
	if (!records_file) {
		perror(filename);
		return DM_FILE_ERROR;
	}

	while (fgets(buffer, sizeof(buffer), records_file)) {
		ret = cmd_insert_record(buffer);
		if (ret) {
			fclose(records_file);
			return cmd_exit(ret);
		}
	}

	fclose(records_file);

	//puts("Records inserted.");
	return ret;
}

int cmd_phase()
{
	char *cmd, *args;
	char buffer[1024];
	int ret;

	//puts("Type your commands below:\n(Type /exit or Ctrl+D to quit)\n");

	for (;;) {
		//fputs(_prompt, stdout);
		if (!fgets(buffer, sizeof(buffer), stdin))   /* Handle Ctrl+D */
			break;

		cmd = strtok(buffer, _whitespace);
		if (!cmd)
			continue;                              /* Empty input */

		args = strtok(NULL, _whitespace);
		if (args)
			args[strlen(args)] = ' '; /* Replace strtok NULL byte */

		ret = 0;

		if (!strcmp(cmd, "/exit")) {                  /* Handle /exit */
			break;
		} else if (!strcmp(cmd, "/globalDiseaseStats")) {
			ret = cmd_global_disease_stats(args);
		} else if (!strcmp(cmd, "/diseaseFrequency")) {
			ret = cmd_disease_frequency(args);
		} else if (!strcmp(cmd, "/topk-Diseases")) {
			ret = cmd_topk_diseases(args);
		} else if (!strcmp(cmd, "/topk-Countries")) {
			ret = cmd_topk_countries(args);
		} else if (!strcmp(cmd, "/insertPatientRecord")) {
			ret = cmd_insert_record(args);
			if (!ret)
				puts("Record added");
		} else if (!strcmp(cmd, "/recordPatientExit")) {
			ret = cmd_record_patient_exit(args);
		} else if (!strcmp(cmd, "/numCurrentPatients")) {
			ret = cmd_num_current_patients(args);
		} else {
			cmd_print_usage();
		}

		if (ret)
			puts("error");

		if (ret == DM_INVALID_PARAMETER)
			cmd_print_usage();
	}

	return cmd_exit(DM_OK);
}
