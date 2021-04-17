#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>
#include <dirent.h>

#include "aggregator.h"
#include "common.h"

int print_usage(const char *program);

int main(int argc, char *argv[])
{
	/* Parameters & relevant checking */
	int workers = 0, buffer_size = 0;
	char* input_dir = NULL;
	char opt;

	struct stat s;

	DIR *dir;
	struct dirent *entry;
	int subdirs = 0;

	while ((opt = getopt(argc, argv, "w:b:i:")) != -1) {
		switch (opt) {
		case 'w':
			workers = atoi(optarg);
			break;

		case 'b':
			buffer_size = atoi(optarg);
			break;

		case 'i':
			input_dir = strdup(optarg);
			break;

		default:
			return print_usage(argv[0]);
		}
	}

	if (workers <= 0 || buffer_size <= 0 || !input_dir)
		return print_usage(argv[0]);

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
	workers = (workers < subdirs ? workers : subdirs);

	/* The magic begins... */
	return aggregator(workers, buffer_size, input_dir);
}


int print_usage(const char *program)
{
	fprintf(stderr, "%s –w numWorkers -b bufferSize -i input_dir\n",
	        program);
	return DA_INVALID_PARAMETER;
}
