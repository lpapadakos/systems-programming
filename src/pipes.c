#include <errno.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "common.h"
#include "pipes.h"

static int buffer_size;

size_t min(size_t a, size_t b)
{
	return (a < b ? a : b);
}

void pipes_init(int _buffer_size)
{
	buffer_size = _buffer_size;
}

int msg_read(int fd, struct p_msg *msg)
{
	ssize_t n_read, space_left;
	char *c;

	/* msg->consumed means all the strings in the buffer have been read.
	   shift over the remaining (unfinished) messages to the start and
	   adjust msg->pos accoordingly */
	if (msg->pos != msg->buffer && msg->consumed) {
		c = msg->pos - 1;

		/* Find last NULL byte */
		while (*c && c > msg->buffer)
			c--;

		n_read = msg->pos - c - 1;/* n_read: bytes of partial message */
		memmove(msg->buffer, c + 1, n_read);
		msg->pos = msg->buffer + n_read;

		msg->consumed = 0;
	}

	while ((space_left = (msg->buffer + sizeof(msg->buffer)) - msg->pos) > 0) {
		n_read = read(fd, msg->pos, min(space_left, buffer_size));

		if (n_read == -1 && (errno == EAGAIN || errno == EINTR))
			return -1;     /* Interrupted/Pipe is empty (for now) */

		if (n_read <= 0) {
			perror("read");
			exit(DA_PIPE_ERROR);
		}

		msg->pos += n_read;

		if (!*(msg->pos - 1))           /* Last read byte is MSG_DONE */
			break;
	};

	return 0;
}

int msg_write(int fd, char *msg, size_t nbyte)
{
	ssize_t n_write;

	do {
		do {
			n_write = write(fd, msg, min(nbyte, buffer_size));
		} while (n_write == -1 && (errno == EAGAIN || errno == EINTR));

		if (n_write <= 0) {
			perror("write");
			exit(DA_PIPE_ERROR);
		}

		nbyte -= n_write;
		msg += n_write;
	} while (nbyte > 0);                    /* We have sent <nbyte> bytes */

	return 0;
}

int msg_write_line(int fd, char *line)
{
	msg_write(fd, line, strlen(line));
	return msg_write(fd, MSG_DELIMITER, 1);
}

int msg_done(int fd)
{
	return msg_write(fd, MSG_DONE, 1);
}

int msg_ready(int fd)
{
	return msg_write(fd, MSG_READY, strlen(MSG_READY) + 1);
}

int msg_invalid(int fd)
{
	return msg_write(fd, MSG_INVALID, strlen(MSG_INVALID) + 1);
}
