#include <stdio.h>
#include <string.h>

#include "commands.h"
#include "common.h"
#include "hashtable.h"

/* For length specifier in sscanf */
#define STR2(x) #x
#define STR(X) STR2(X)
#define FIELD_SIZE 32

int cmd_init(int disease_entries, int country_entries, int bucket_size)
{
	return ht_init(disease_entries, country_entries, bucket_size);
}

void cmd_print_usage()
{
	fprintf(stderr,
		"\x1b[1mCommands:\x1b[0m\n"
		"/globalDiseaseStats [date1 date2]\n"
		"/diseaseFrequency virusName date1 date2 [country]\n"
		"/topk-Diseases k country [date1 date2]\n"
		"/topk-Countries k disease [date1 date2]\n"
		"/insertPatientRecord recordID patientFirstName patientLastName diseaseID country entryDate [exitDate]\n"
		"/recordPatientExit recordID exitDate\n"
		"/numCurrentPatients [disease]\n"
		"/exit\n");
}

int cmd_global_disease_stats(char *args)
{
	struct date date1 = {0};
	struct date date2 = {0};

	char c;                                     /* Extra uneeded argument */

	if (args) {
		/* If date1 exists, date2 must also */
		if (sscanf(args, "%2c-%2c-%4c %2c-%2c-%4c %c",
		           date1.day, date1.month, date1.year,
	                   date2.day, date2.month, date2.year, &c) != 6) {
			fputs("Either specify no interval, or both dates!\n",
			      stderr);
			return DM_INVALID_PARAMETER;
		}
	}

	return global_disease_stats(&date1, &date2);
}

int cmd_disease_frequency(char *args)
{
	char disease_id[FIELD_SIZE];
	struct date date1;
	struct date date2;
	char country[FIELD_SIZE] = {0};

	int matched;
	char c;                                     /* Extra uneeded argument */

	if (!args)
		return DM_INVALID_PARAMETER;

	matched = sscanf(args, "%" STR(FIELD_SIZE) "s "
		"%2c-%2c-%4c %2c-%2c-%4c "
		"%" STR(FIELD_SIZE) "s %c",
		disease_id,
		date1.day, date1.month, date1.year,
		date2.day, date2.month, date2.year,
		country, &c);

	if (matched < 7 || matched > 8)
		return DM_INVALID_PARAMETER;

	return disease_frequency(disease_id, &date1, &date2, country);
}

int cmd_topk_diseases(char *args)
{
	int k;
	char disease[FIELD_SIZE];
	struct date date1 = {0};
	struct date date2 = {0};

	int matched;
	char c;                                     /* Extra uneeded argument */

	if (!args)
		return DM_INVALID_PARAMETER;

	matched = sscanf(args, "%d %" STR(FIELD_SIZE) "s %2c-%2c-%4c %2c-%2c-%4c %c",
	                 &k, disease,
	                 date1.day, date1.month, date1.year,
	                 date2.day, date2.month, date2.year, &c);

	if (matched < 2 || matched > 8)
		return DM_INVALID_PARAMETER;

	if (k < 0)
		return DM_INVALID_PARAMETER;

	return topk_diseases(k, disease, &date1, &date2);
}

int cmd_topk_countries(char *args)
{
	int k;
	char country[FIELD_SIZE];
	struct date date1 = {0};
	struct date date2 = {0};

	int matched;
	char c;                                     /* Extra uneeded argument */

	if (!args)
		return DM_INVALID_PARAMETER;

	matched = sscanf(args, "%d %" STR(FIELD_SIZE) "s "
		"%2c-%2c-%4c %2c-%2c-%4c %c",
		&k, country,
		date1.day, date1.month, date1.year,
		date2.day, date2.month, date2.year, &c);

	if (matched < 2 || matched > 8)
		return DM_INVALID_PARAMETER;

	if (k < 0)
		return DM_INVALID_PARAMETER;

	return topk_countries(k, country, &date1, &date2);
}

int cmd_insert_record(char *args)
{
	/* Temporary storage for disease_id and country.
	 * Dedicated space allocated later, in the buckets.
	 * See README[1] */
	char record_id[FIELD_SIZE];
	char first_name[FIELD_SIZE];
	char last_name[FIELD_SIZE];
	char disease_id[FIELD_SIZE];
	char country[FIELD_SIZE];
	struct record tmp = {
		NULL,
		record_id, first_name, last_name,
		disease_id, country,
		{0}, {0}};

	if (!args)
		return DM_INVALID_PARAMETER;

	if (sscanf(args, "%" STR(FIELD_SIZE) "s "
		"%" STR(FIELD_SIZE) "s "
		"%" STR(FIELD_SIZE) "s "
		"%" STR(FIELD_SIZE) "s "
		"%" STR(FIELD_SIZE) "s "
		"%2c-%2c-%4c %2c-%2c-%4c",
		tmp.record_id, tmp.first_name, tmp.last_name,
		tmp.disease_id, tmp.country,
		tmp.entry_date.day, tmp.entry_date.month, tmp.entry_date.year,
		tmp.exit_date.day, tmp.exit_date.month, tmp.exit_date.year) < 8) {
		fputs("There are fields missing from the record.\n", stderr);
		return DM_INVALID_RECORD;
	}

	/* For records in the file, a dash signifies a missing exitDate */
	if (tmp.exit_date.day[0] == '-')
		tmp.exit_date.year[0] = '\0';

	return insert_record(&tmp);
}

int cmd_record_patient_exit(char *args)
{
	char record_id[FIELD_SIZE];
	struct date exit_date;

	char c;                                     /* Extra uneeded argument */

	if (!args)
		return DM_INVALID_PARAMETER;

	if (sscanf(args, "%" STR(FIELD_SIZE) "s %2c-%2c-%4c %c", record_id,
	    exit_date.day, exit_date.month, exit_date.year, &c) != 4) {
		return DM_INVALID_PARAMETER;
	}

	return record_patient_exit(record_id, &exit_date);
}

int cmd_num_current_patients(char *args)
{
	char disease_id[FIELD_SIZE] = {0};

	char c;                                     /* Extra uneeded argument */

	/* If no arguments, disease[0] is 0 and we get numbers for all the
	 * diseases */
	if (args) {
		if (sscanf(args, "%" STR(FIELD_SIZE) "s %c", disease_id, &c) != 1)
			return DM_INVALID_PARAMETER;
	}

	return num_current_patients(disease_id);
}

int cmd_exit(int status)
{
	ht_destroy();
	puts("exiting");

	return status;
}
