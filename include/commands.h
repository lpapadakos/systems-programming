/* Command Line Interface */

#ifndef COMMANDS_H
#define COMMANDS_H

int cmd_init(int disease_entries, int country_entries, int bucket_size);
void cmd_print_usage(void);

/* String manipulation leads from stdin to arguments ready for the next level */
int cmd_global_disease_stats(char *args);
int cmd_disease_frequency(char *args);
int cmd_topk_diseases(char *args);
int cmd_topk_countries(char *args);
int cmd_insert_record(char *args);
int cmd_record_patient_exit(char *args);
int cmd_num_current_patients(char *args);

int cmd_exit(int status);

#endif /* COMMANDS_H */
