#include <stdlib.h>
#include <string.h>
#include <stdio.h>

#include "common.h"
#include "hashtable.h"
#include "heap.h"
#include "record.h"
#include "tree.h"

/* Hash Tables */
struct bucket_entry {
	char *name;
	struct tree_node* tree;
};

struct bucket {
	struct bucket *next;
	int count;
	struct bucket_entry entry[];
};

static struct hash_table *countries_ht;
static struct hash_table *diseases_ht;

static int bucket_size, max_bucket_entries;
static const int cases_entries = 10;

/* README[6] */
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
		return DM_INVALID_PARAMETER;
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

	return DM_OK;
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

		/* README[5] */
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

	fputs("No such diseaseID exists.\n", stderr);
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

	fputs("No such country exists.\n", stderr);
	return NULL;
}

int count_cases(struct tree_node *tree)
{
	struct record *record;
	int N = 0;

	record = tree_get_next_record(tree);
	while (record) {
		N++;
		record = tree_get_next_record(NULL);
	}

	return N;
}

/* README[4] */
int count_cases_in_range(struct tree_node *tree, struct date *date1, struct date *date2)
{
	struct record *record;
	int N = 0;

	/* Start from the first node >= date1 */
	record = tree_get_next_record(tree_find_gte_node(tree, date1));
	while (record) {
		/* Stop when we surpass date2 */
		if (datecmp(&record->entry_date, date2) > 0)
			break;

		N++;
		record = tree_get_next_record(NULL);
	}

	return N;
}

int count_cases_in_range_country(struct tree_node *tree, struct date *date1, struct date *date2, char *country)
{
	struct record *record;
	int N = 0;

	/* Start from the subtree that rooted at date1 */
	record = tree_get_next_record(tree_find_gte_node(tree, date1));
	while (record) {
		/* Stop when we surpass date2 */
		if (datecmp(&record->entry_date, date2) > 0)
			break;

		if (!strcmp(country, record->country))
			N++;

		record = tree_get_next_record(NULL);
	}

	return N;
}

/* README[3] */
int count_current_patients(struct tree_node *tree)
{
	struct record *record;
	int N = 0;

	record = tree_get_next_record(tree);
	while (record) {
		if (null_date(&record->exit_date))
			N++;

		record = tree_get_next_record(NULL);
	}

	return N;
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
		if (++hash == ht->entries)
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

/* Cases Hash Table for topk */

struct case_entry *make_cases_entry(char *name)
{
	struct case_entry *entry = malloc(sizeof(*entry));

	entry->name = name;
	entry->count = 1;
	entry->next = NULL;

	return entry;
}

struct case_entry *cases_get(struct hash_table *ht, char *name)
{
	struct case_entry *current;

	current = ht->bucket[string_hash(ht, name)];
	while (current) {
		if (!strcmp(name, current->name))
			break;

		current = current->next;
	}

	return current;
}

void cases_increment(struct hash_table *ht, char *name)
{
	struct case_entry *entry = cases_get(ht, name);
	int hash = string_hash(ht, name);

	/* If entry for this name exists, just increment the case count */
	if (entry) {
		entry->count++;
		return;
	}

	/* Insert at beginning of list */
	entry = make_cases_entry(name);
	entry->next = ht->bucket[hash];
	ht->bucket[hash] = entry;
}

/* Commands Implementation */

int global_disease_stats(struct date *date1, struct date *date2)
{
	struct bucket_entry *entry;

	if (null_date(date1)) {
		entry = get_next_entry(diseases_ht, 1);
		while (entry) {
			printf("%s %d\n", entry->name, count_cases(entry->tree));
			entry = get_next_entry(diseases_ht, 0);
		}

		return DM_OK;
	}

	if (!valid_interval(date1, date2))
		return DM_INVALID_DATE;

	entry = get_next_entry(diseases_ht, 1);
	while (entry) {
		printf("%s %d\n",
			entry->name,
			count_cases_in_range(entry->tree, date1, date2));

		entry = get_next_entry(diseases_ht, 0);
	}

	return DM_OK;
}

int disease_frequency(char *disease_id, struct date *date1, struct date *date2, char *country)
{
	struct tree_node *tree;

	if (!valid_interval(date1, date2))
		return DM_INVALID_DATE;

	tree = find_disease_tree(disease_id);
	if (!tree)
		return DM_INVALID_DISEASE;

	if (country[0]) {
		printf("%s %d\n",
	               disease_id,
		       count_cases_in_range_country(tree, date1, date2, country));
	} else {
		printf("%s %d\n",
	               disease_id, count_cases_in_range(tree, date1, date2));
	}

	return DM_OK;
}

//TODO improve stuff
int topk_diseases(int k, char *country, struct date *date1, struct date *date2)
{
	struct tree_node *tree;
	struct record *record;
	struct case_entry *entry;
	struct hash_table *cases_ht;
	struct heap heap = {0};
	int hash;

	cases_ht = malloc(sizeof(*cases_ht) + cases_entries*(sizeof(cases_ht[0])));
	cases_ht->entries = cases_entries;
	memset(cases_ht->bucket, 0, cases_entries*(sizeof(cases_ht[0])));

	/* Count cases of disease in country */
	if (null_date(date1)) {
		tree = find_country_tree(country);
		record = tree_get_next_record(tree);
		while (record) {
			cases_increment(cases_ht, record->disease_id);
			record = tree_get_next_record(NULL);
		}
	} else {              /* Only count for cases between date1 and date2 */
		if (!valid_interval(date1, date2))
			return DM_INVALID_DATE;

		tree = tree_find_gte_node(find_country_tree(country), date1);
		record = tree_get_next_record(tree);
		while (record) {
			/* Stop when we surpass date2 */
			if (datecmp(&record->entry_date, date2) > 0)
				break;

			cases_increment(cases_ht, record->disease_id);
			record = tree_get_next_record(NULL);
		}
	}

	/* Insert results into heap */
	for (hash = 0; hash < cases_ht->entries; hash++) {
		entry = cases_ht->bucket[hash];
		while (entry) {
			heap_insert(&heap, entry);
			entry = entry->next;
		}
	}

	free(cases_ht);   /* free the hash_table, entries will be freed later */

	/* Print results from max heap */
	while (k-- && heap_top(&heap)) {
		entry = heap_top(&heap);
		printf("%s %d\n", entry->name, entry->count);

		heap_pop(&heap);
	}

	heap_destroy(&heap);           /* free the heap we created on-the-fly */

	return DM_OK;
}

int topk_countries(int k, char *disease_id, struct date *date1, struct date *date2)
{
	struct tree_node *tree;
	struct record *record;
	struct case_entry *entry;
	struct hash_table *cases_ht;
	struct heap heap = {0};
	int hash;

	cases_ht = malloc(sizeof(*cases_ht) + cases_entries*(sizeof(cases_ht[0])));
	cases_ht->entries = cases_entries;
	memset(cases_ht->bucket, 0, cases_entries*(sizeof(cases_ht[0])));

	/* Count cases of countries that experienced this disease */
	if (null_date(date1)) {
		tree = find_disease_tree(disease_id);
		record = tree_get_next_record(tree);
		while (record) {
			cases_increment(cases_ht, record->country);
			record = tree_get_next_record(NULL);
		}
	} else {              /* Only count for cases between date1 and date2 */
		if (!valid_interval(date1, date2))
			return DM_INVALID_DATE;

		tree = tree_find_gte_node(find_disease_tree(disease_id), date1);
		record = tree_get_next_record(tree);
		while (record) {
			/* Stop when we surpass date2 */
			if (datecmp(&record->entry_date, date2) > 0)
				break;

			cases_increment(cases_ht, record->country);
			record = tree_get_next_record(NULL);
		}
	}

	/* Insert results into heap */
	for (hash = 0; hash < cases_ht->entries; hash++) {
		entry = cases_ht->bucket[hash];
		while (entry) {
			heap_insert(&heap, entry);
			entry = entry->next;
		}
	}

	free(cases_ht);   /* free the hash_table, entries will be freed later */

	/* Print results from max heap */
	while (k-- && heap_top(&heap)) {
		entry = heap_top(&heap);
		printf("%s %d\n", entry->name, entry->count);

		heap_pop(&heap);
	}

	heap_destroy(&heap);           /* free the heap we created on-the-fly */

	return DM_OK;
}

int insert_record(struct record *tmp)
{
	struct record *patient_record = record_add(tmp);

	if (!patient_record)
		return DM_INVALID_RECORD;

	if (!valid_interval(&tmp->entry_date, &tmp->exit_date))
		return DM_INVALID_DATE;

	/* Update the data structures (buckets, country/disease trees) */
	ht_insert(diseases_ht, patient_record);
	ht_insert(countries_ht, patient_record);

	return DM_OK;
}

int record_patient_exit(char *record_id, struct date *exit_date)
{
	struct record *record = record_get(record_id);

	if (!record) {
		puts("Not found");
		return DM_INVALID_RECORD;
	}

	if (!valid_interval(&record->entry_date, exit_date))
		return DM_INVALID_DATE;

	record->exit_date = *exit_date;
	puts("Record updated");

	return DM_OK;
}

int num_current_patients(char *disease_id)
{
	struct bucket_entry *entry;
	struct tree_node *tree;

	/* Enumerate patients for all diseases */
	if (!disease_id[0]) {
		entry = get_next_entry(diseases_ht, 1);
		while (entry) {
			printf("%s %d\n",
	     		       entry->name, count_current_patients(entry->tree));

			entry = get_next_entry(diseases_ht, 0);
		}

		return DM_OK;
	}

	/* Enumerate patients for specific disease */

	tree = find_disease_tree(disease_id);

	if (!tree)
		return DM_INVALID_DISEASE;

	printf("%s %d\n", disease_id, count_current_patients(tree));

	return DM_OK;
}
