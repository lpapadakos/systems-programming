/* HashTable Implementation */

#ifndef HASHTABLE_H
#define HASHTABLE_H

#include "record.h"

struct hash_table {
	int entries;
	void *bucket[];
};

/* For topk-X */
struct case_entry {
	struct case_entry *next;
	char *name;
	int count;
};

/* Interface */
int string_hash(struct hash_table *ht, char *_str);
int ht_init(int disease_entries, int country_entries, int bucket_size);

/* All commands depend on the hash_tables (disease, countries) */
int global_disease_stats(struct date *date1, struct date *date2);
int disease_frequency(char *disease_id, struct date *date1, struct date *date2, char *country);
int topk_diseases(int k, char *country, struct date *date1, struct date *date2);
int topk_countries(int k, char *disease_id, struct date *date1, struct date *date2);
int insert_record(struct record *tmp);
int record_patient_exit(char *record_id,  struct date *exit_date);
int num_current_patients(char *disease_id);

void ht_destroy();

#endif /* HASHTABLE_H */
