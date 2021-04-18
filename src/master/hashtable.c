#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "common.h"
#include "master/hashtable.h"
#include "master/record.h"
#include "master/tree.h"
#include "pipes.h"

/* Hash Tables */
/* struct bucket_entry {
	char *name;
	struct tree_node* tree;
}; */

struct bucket {
	struct bucket *next;
	int count;
	struct bucket_entry entry[];
};

static struct hash_table *countries_ht;
static struct hash_table *diseases_ht;

static int bucket_size, max_bucket_entries;

int string_hash(struct hash_table *ht, char *_str)
{
	unsigned long hash = 5381;
	unsigned char *str = (unsigned char*) _str;
	int c;

	while ((c = *str++))
		hash = ((hash << 5) + hash) + c; /* hash * 33 + c */

	return (int) (hash % (unsigned long) ht->entries);
}

struct bucket *make_bucket()
{
	struct bucket *bucket = malloc(bucket_size);
	bucket->count = 0;
	bucket->next = NULL;

	return bucket;
}

int ht_init(int disease_entries, int country_entries, int _bucket_size)
{
	int i;

	bucket_size = _bucket_size;
	max_bucket_entries = (bucket_size - sizeof(struct bucket)) /
	                     sizeof(struct bucket_entry);

	if (max_bucket_entries < 1) {
		fprintf(stderr, "bucketSize is too small!\n");
		return DA_INVALID_PARAMETER;
	}

	/* Intialize hash tables */
	diseases_ht = malloc(sizeof(*diseases_ht) + disease_entries*sizeof(diseases_ht->bucket[0]));
	diseases_ht->entries = disease_entries;

	for (i = 0; i < diseases_ht->entries; ++i)
		diseases_ht->bucket[i] = NULL;


	countries_ht = malloc(sizeof(*countries_ht) + country_entries*sizeof(countries_ht->bucket[0]));
	countries_ht->entries = country_entries;

	for (i = 0; i < countries_ht->entries; ++i)
		countries_ht->bucket[i] = NULL;

	records_init(20);                                /* Hardcoded for now */

	return DA_OK;
}

void ht_insert(struct hash_table *ht, struct record *patient_record)
{
	struct bucket *bucket;
	char **field;  /* Used to update the record field, to point elsewhere */
	int hash;
	int i;

	if (ht == diseases_ht) {
		hash = string_hash(ht, patient_record->disease_id);
		field = &patient_record->disease_id;
	} else { /* diseases_ht */
		hash = string_hash(ht, patient_record->country);
		field = &patient_record->country;
	}

	if (!ht->bucket[hash])
		ht->bucket[hash] = make_bucket();

	bucket = ht->bucket[hash];

	for (;;) {
		for (i = 0; i < bucket->count; i++) {
			if (!strcmp(bucket->entry[i].name, *field)) {
				/* Country/Disease found. No need for a new entry */
				/* Record points to the same spot as the bucket entry */
				*field = bucket->entry[i].name;
				bucket->entry[i].tree = tree_insert(bucket->entry[i].tree, patient_record);

				return;
			}
		}

		if (bucket->count < max_bucket_entries)
			break;          /* Country not found but it fits here */
		else if (!(bucket->next)) /* Country not found and we ran out of overflow blocks */
			bucket->next = make_bucket();

		/* Country not found... YET! keep looking. */
		bucket = bucket->next;
	}

	/* Make new entry at the last open position */
	i = bucket->count;

	bucket->entry[i].name = strdup(*field);
	bucket->entry[i].tree = NULL;

	/* Record points to the same spot as the bucket entry */
	*field = bucket->entry[i].name;
	bucket->entry[i].tree = tree_insert(bucket->entry[i].tree, patient_record);

	bucket->count++;
}

/* Iterates through the entries of a hash table (passed in ht).
 * The first call (or any call where we need to start from the beginning)
 * should have the reset set to 1 */
struct bucket_entry *get_next_entry(struct hash_table *ht, int reset)
{
	static struct bucket *bucket;
	static int hash, i;

	if (reset) {
		hash = 0;
		bucket = ht->bucket[0];
		i = 0;
	}

	if (!bucket) {
		if (++hash >= ht->entries)
			return NULL;

		bucket = ht->bucket[hash];
		i = 0;

		return get_next_entry(ht, 0);
	};

	if (i == bucket->count) {
		bucket = bucket->next;
		i = 0;

		return get_next_entry(ht, 0);
	}

	return &bucket->entry[i++];
}

void ht_destroy()
{
	struct bucket *current, *next;
	int b, e;

	/* Record List */
	records_destroy();

	/* Countries Hash Table */
	for (b = 0; b < countries_ht->entries; ++b) {
		current = countries_ht->bucket[b];

		while (current) {
			next = current->next;

			for (e = 0; e < current->count; ++e) {
				free(current->entry[e].name);
				tree_destroy(current->entry[e].tree);
			}

			free(current);
			current = next;
		}
	}
	free(countries_ht);

	/* Diseases Hash Table */
	for (b = 0; b < diseases_ht->entries; ++b) {
		current = diseases_ht->bucket[b];

		while (current) {
			next = current->next;

			for (e = 0; e < current->count; ++e) {
				free(current->entry[e].name);
				tree_destroy(current->entry[e].tree);
			}

			free(current);
			current = next;
		}
	}
	free(diseases_ht);
}

/* Tree Functions */
struct tree_node *find_disease_tree(char *disease_id)
{
	struct bucket *bucket;
	int hash = string_hash(diseases_ht, disease_id);
	int i;

	bucket = diseases_ht->bucket[hash];
	while (bucket) {
		for (i = 0; i < bucket->count; ++i) {
			if (!strcmp(bucket->entry[i].name, disease_id))
				return bucket->entry[i].tree;
		}

		bucket = bucket->next;
	}

	return NULL;
}

struct tree_node *find_country_tree(char *country)
{
	struct bucket *bucket;
	int hash = string_hash(countries_ht, country);
	int i;

	bucket = countries_ht->bucket[hash];
	while (bucket) {
		for (i = 0; i < bucket->count; ++i) {
			if (!strcmp(bucket->entry[i].name, country))
				return bucket->entry[i].tree;
		}

		bucket = bucket->next;
	}

	return NULL;
}

int country_num_patient_admissions(struct tree_node *country, char *disease, struct date *date1, struct date *date2, int *age_group)
{
	struct record *record;

	age_group[0] = 0;
	age_group[1] = 0;
	age_group[2] = 0;
	age_group[3] = 0;

	if (!(country = tree_find_gte_node(country, date1)))
		return 0;

	record = tree_get_next_record(country);
	while (record) {
		/* Stop when we surpass date2 */
		if (datecmp(&record->entry_date, date2) > 0)
			break;

		if (!strcmp(disease, record->disease_id)) {
			if (record->age <= 20)
				age_group[0]++;
			else if (record->age <= 40)
				age_group[1]++;
			else if (record->age <= 60)
				age_group[2]++;
			else
				age_group[3]++;
		}

		record = tree_get_next_record(NULL);
	}

	return age_group[0] + age_group[1] + age_group[2] + age_group[3];
}

int country_num_patient_discharges(struct tree_node *country, char *disease, struct date *date1, struct date *date2, int *age_group)
{
	struct record *record;

	age_group[0] = 0;
	age_group[1] = 0;
	age_group[2] = 0;
	age_group[3] = 0;

	record = tree_get_next_record(country);
	while (record) {
		if (!null_date(&record->exit_date)) {
			if (datecmp(&record->exit_date, date1) >= 0 &&
			    datecmp(&record->exit_date, date2) <= 0 &&
			    !strcmp(disease, record->disease_id)) {
				if (record->age <= 20)
					age_group[0]++;
				else if (record->age <= 40)
					age_group[1]++;
				else if (record->age <= 60)
					age_group[2]++;
				else
					age_group[3]++;
			}
		}

		record = tree_get_next_record(NULL);
	}

	return age_group[0] + age_group[1] + age_group[2] + age_group[3];
}

/* Commands Implementation */
int insert_record(struct record *tmp)
{
	struct record *patient_record = record_add(tmp);

	if (!patient_record)
		return DA_INVALID_RECORD;                    /* Syntax errors */

	if (patient_record == tmp)
		return DA_OK;                               /* Record updated */

	/* Record added
	 * Update the data structures (buckets, country/disease trees) */
	ht_insert(diseases_ht, patient_record);
	ht_insert(countries_ht, patient_record);

	return DA_OK;
}

int file_statistics(char *country, char *file, int response_fd)
{
	struct bucket_entry *disease;
	struct tree_node *tree, *node;
	struct record *record;

	struct date date = to_date(file);
	int age_group[4], i;
	char *str_age_group[4] = {"0-20", "21-40", "41-60", "60+"};

	char buf[100];

	if (!valid_date(&date))
		return DA_INVALID_DATE;

	if (!(tree = find_country_tree(country))) {
		fprintf(stderr, "%s %s: no such country\n", country, file);
		return DA_INVALID_COUNTRY;
	}

	/* Subtree rooted at first record with entry date >= file */
	if (!(tree = tree_find_gte_node(tree, &date))) {
		fprintf(stderr, "%s %s: no such date with enter\n", country, file);
		return DA_INVALID_DATE;
	}

	msg_write_line(response_fd, file);
	msg_write_line(response_fd, country);

	/* For every disease, iterate though the records with entry_date = date */
	disease = get_next_entry(diseases_ht, 1);
	while (disease) {
		age_group[0] = 0;
		age_group[1] = 0;
		age_group[2] = 0;
		age_group[3] = 0;

		node = tree;
		while (node) {
			record = node->patient_record;

			/* Stop when we surpass this date */
			if (datecmp(&record->entry_date, &date) > 0)
				break;

			if (!strcmp(record->disease_id, disease->name)) {
				if (record->age <= 20)
					age_group[0]++;
				else if (record->age <= 40)
					age_group[1]++;
				else if (record->age <= 60)
					age_group[2]++;
				else
					age_group[3]++;
			}

			node = node->right;               /* for >=, go right */
		}

		msg_write_line(response_fd, disease->name);

		for (i = 0; i < 4; ++i) {
			snprintf(buf, sizeof(buf),
			         "Age range %s years: %d cases",
				 str_age_group[i], age_group[i]);
			msg_write_line(response_fd, buf);
		}

		msg_write(response_fd, MSG_DELIMITER, 1);

		disease = get_next_entry(diseases_ht, 0);
	}

	msg_done(response_fd);

	return DA_OK;
}

int list_countries(int response_fd)
{
	struct bucket_entry *entry;
	char tag[16];

	snprintf(tag, sizeof(tag)," %d", getpid());

	entry = get_next_entry(countries_ht, 1);
	while (entry) {
		msg_write(response_fd, entry->name, strlen(entry->name));
		msg_write_line(response_fd, tag);

		entry = get_next_entry(countries_ht, 0);
	}

	msg_done(response_fd);

	return DA_OK;
}

int topk_age_ranges(int k, char *country, char *disease, struct date *date1, struct date *date2, int response_fd)
{
	struct tree_node *tree;
	int age_group[4], all;
	char *str_age_group[4] = {"0-20", "21-40", "41-60", "60+"};

	int i, max;
	char buf[1024];

	if (!(tree = find_country_tree(country)))
		return DA_INVALID_COUNTRY;

	if (!valid_interval(date1, date2))
		return DA_INVALID_DATE;

	all = country_num_patient_admissions(tree, disease, date1, date2, age_group);

	if (!all)
		return DA_OK;

	if (k > 4)
		k = 4;

	while (k--) {
		max = 0;
		for (i = 1; i < 4; ++i) {
			if (age_group[i] > age_group[max])
				max = i;
		}

		snprintf(buf, sizeof(buf), "%s: %.2f%%",
			str_age_group[max],
			100.0 * ((double) age_group[max]) / ((double) all));

		msg_write_line(response_fd, buf);

		age_group[max] = -1;
	}

	msg_done(response_fd);
	return DA_OK;
}

int num_patient_admissions(char *disease, struct date *date1, struct date *date2, char *country, int response_fd)
{
	struct bucket_entry *entry;
	struct tree_node *tree;
	int age_group[4];
	char buf[1024];

	if (country) {
		if (!(tree = find_country_tree(country)))
			return DA_INVALID_COUNTRY;

		if (!valid_interval(date1, date2))
			return DA_INVALID_DATE;

		snprintf(buf, sizeof(buf), "%s %d\n",
		         country,
		         country_num_patient_admissions(tree, disease, date1, date2, age_group));
		msg_write(response_fd, buf, strlen(buf) + 1);

		return DA_OK;
	}

	if (!valid_interval(date1, date2))
		return DA_INVALID_DATE;

	/* All countries */
	entry = get_next_entry(countries_ht, 1);
	while (entry) {
		snprintf(buf, sizeof(buf), "%s %d",
		         entry->name,
		         country_num_patient_admissions(entry->tree, disease, date1, date2, age_group));
		msg_write_line(response_fd, buf);

		entry = get_next_entry(countries_ht, 0);
	}

	msg_done(response_fd);
	return DA_OK;
}

int num_patient_discharges(char *disease, struct date *date1, struct date *date2, char *country, int response_fd)
{
	struct bucket_entry *entry;
	struct tree_node *tree;
	int age_group[4];
	char buf[1024];

	if (country) {
		if (!(tree = find_country_tree(country)))
			return DA_INVALID_COUNTRY;

		if (!valid_interval(date1, date2))
			return DA_INVALID_DATE;

		snprintf(buf, sizeof(buf), "%s %d\n",
		         country,
		         country_num_patient_discharges(tree, disease, date1, date2, age_group));
		msg_write(response_fd, buf, strlen(buf) + 1);

		return DA_OK;
	}

	if (!valid_interval(date1, date2))
		return DA_INVALID_DATE;

	/* All countries */
	entry = get_next_entry(countries_ht, 1);
	while (entry) {
		snprintf(buf, sizeof(buf), "%s %d",
		         entry->name,
		         country_num_patient_discharges(entry->tree, disease, date1, date2, age_group));
		msg_write_line(response_fd, buf);

		entry = get_next_entry(countries_ht, 0);
	}

	msg_done(response_fd);
	return DA_OK;
}

/* rest */
int have_date_records(struct tree_node *country, char *file)
{
	struct record *record;
	struct date date = to_date(file);

	if (!valid_date(&date))
		return 1;

	/* See if there is presence of this date in our records */
	record = tree_get_next_record(country);
	while (record) {
		if (!datecmp(&record->entry_date, &date))
			return 1;
		else if (!datecmp(&record->exit_date, &date))
			return 1;

		record = tree_get_next_record(NULL);
	}

	return 0;
}

struct bucket_entry *get_next_country(int reset)
{
	return get_next_entry(countries_ht, reset);
}
