/* HashTable Implementation */

#ifndef HASHTABLE_H
#define HASHTABLE_H

#include "record.h"

struct hash_table {
	int entries;
	void *bucket[];
};

struct bucket_entry {
	char *name;
	struct tree_node* tree;
};

/* Interface */
int string_hash(struct hash_table *ht, char *_str);

int ht_init(int disease_entries, int country_entries, int bucket_size);
void ht_destroy();

/* Worker commands implementation */
int insert_record(struct record *tmp);
int file_statistics(char *country, char *file, int response_fd);
int list_countries(int response_fd);
int topk_age_ranges(int k, char *country, char *disease, struct date *date1, struct date *date2, int response_fd);
int num_patient_admissions(char *disease, struct date *date1, struct date *date2, char *country, int response_fd);
int num_patient_discharges(char *disease, struct date *date1, struct date *date2, char *country, int response_fd);

struct bucket_entry *get_next_country(int reset);
int have_date_records(struct tree_node *country, char *file);

#endif /* HASHTABLE_H */
