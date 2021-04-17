#ifndef RECORD_H
#define RECORD_H

/* Internal date format: YYYYMMDD */
struct date {
	char year[4];
	char month[2];
	char day[2];
};

int valid_date(struct date *date);
static inline int null_date(struct date *date) {return !((char*) date)[0];}
int datecmp(struct date *date1, struct date *date2);
int valid_interval(struct date *date1, struct date *date2);

struct record {
	struct record* next;                     /* Forms the list of records */
	char *record_id;
	char *first_name;
	char *last_name;
	char *disease_id;          /* Points to the same place the buckets do */
	char *country;
	struct date entry_date;
	struct date exit_date;
};

void records_init(int record_entries);

struct record *record_get(char *record_id);
struct record *record_add(struct record*);

void records_destroy(void);

#endif /* RECORD_H */
