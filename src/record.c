#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "common.h"
#include "hashtable.h"
#include "record.h"

static struct hash_table *records_ht;

int valid_date(struct date *date)
{
	struct date max = {{0}, {'1','2'}, {'3','1'}};
	int i;

	for (i = 0; i < sizeof(*date); ++i) {
		if (!isdigit(((char*) date)[i])) {
			fprintf(stderr, "%.8s: Invalid Date\n", (char*) date);
			return 0;
		}
	}

	if (memcmp(date->month, max.month, sizeof(date->month)) > 0) {
		fprintf(stderr, "%.2s: Invalid Month\n", date->month);
		return 0;
	}

	if (memcmp(date->day, max.day, sizeof(date->day)) > 0) {
		fprintf(stderr, "%.2s: Invalid Day\n", date->day);
		return 0;
	}

	/* We could go for a while with the checks... Leap years e.t.c. ... */
	return 1;
}

int datecmp(struct date *date1, struct date *date2)
{
	return memcmp(date1, date2, sizeof(*date1));
}

int valid_interval(struct date *date1, struct date *date2)
{
	if (!valid_date(date1))
		return 0;

	if (!null_date(date2)) {
		if (!valid_date(date2))
			return 0;

		if (datecmp(date1, date2) > 0) {
			fprintf(stderr, "[%.8s, %.8s]: Invalid Interval\n",
			        (char*) date1, (char*) date2);
			return 0;
		}
	}

	return 1;
}

/* Record Functions */

void records_init(int record_entries)
{
	int i;

	records_ht = malloc(sizeof(*records_ht) + record_entries*sizeof(records_ht->bucket[0]));
	records_ht->entries = record_entries;

	for (i = 0; i < records_ht->entries; ++i)
		records_ht->bucket[i] = NULL;
}

struct record *record_get(char *record_id)
{
	struct record *current;

	current = records_ht->bucket[string_hash(records_ht, record_id)];
	while (current) {
		if (!strcmp(record_id, current->record_id))
			break;

		current = current->next;
	}

	return current;
}

struct record *record_add(struct record *tmp)
{
	struct record *patient_record;
	int hash = string_hash(records_ht, tmp->record_id);

	/* Check if this id exists already */
	if (record_get(tmp->record_id)) {
		fprintf(stderr, "The record with id %s already exists!\n",
			tmp->record_id);

		return NULL;
	}

	/* Allocate dedicated space for the new record */
	patient_record = malloc(sizeof(*patient_record));
	*patient_record = *tmp;

	/* Update the record fields for permanent storage */
	patient_record->record_id = strdup(tmp->record_id);
	patient_record->first_name = strdup(tmp->first_name);
	patient_record->last_name = strdup(tmp->last_name);

	/* Insert at beginning of list */
	patient_record->next = records_ht->bucket[hash];
	records_ht->bucket[hash] = patient_record;

	return patient_record;
}

void records_destroy()
{
	struct record *current, *next;
	int i;

	for (i = 0; i < records_ht->entries; ++i) {
		current = records_ht->bucket[i];
		while (current) {
			next = current->next;

			free(current->record_id);
			free(current->first_name);
			free(current->last_name);
			free(current);

			current = next;
		}
	}
	free(records_ht);
}
