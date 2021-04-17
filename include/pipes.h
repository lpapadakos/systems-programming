#ifndef PIPES_H
#define PIPES_H

#define CMD_DIRECTORIES "/directories"
#define CMD_LIST_COUNTRIES "/listCountries"
#define CMD_DISEASE_FREQUENCY "/diseaseFrequency"
#define CMD_TOPK_AGE_RANGES "/topk-AgeRanges"
#define CMD_SEARCH_RECORD "/searchPatientRecord"
#define CMD_NUM_ADMISSIONS "/numPatientAdmissions"
#define	CMD_NUM_DISCHARGES "/numPatientDischarges"
#define CMD_EXIT "/exit"

#define MSG_DELIMITER "\n"
#define MSG_DONE ""
#define MSG_READY "READY"
#define MSG_INVALID "INVALID"

#define TIMEOUT 10000                            /* For poll() (milliseconds) */

struct p_msg {
	char buffer[8192];
	char *pos;
	int consumed;
};

void pipes_init(int _buffer_size);

int msg_read(int fd, struct p_msg*);
int msg_write(int fd, char *msg, size_t nbyte);

/* Wrapper: Write string, without null byte, newline-terminated */
int msg_write_line(int fd, char *line);

int msg_done(int fd);
int msg_ready(int fd);
int msg_invalid(int fd);

#endif /* PIPES_H */
