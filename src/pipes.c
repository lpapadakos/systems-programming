#include <errno.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "common.h"
#include "pipes.h"

static int buffer_size;

void msg_init(struct p_msg *msg)
{
	msg->pos = msg->buffer;
	msg->consumed = 0;
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
	   adjust msg->pos accordingly */
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
		n_read = read(fd, msg->pos, MIN(space_left, buffer_size));

		if (n_read == -1) {
			/* Pipe is empty (for now) or Interrupted by Signal */
			if (errno == EAGAIN || errno == EINTR)
				return -1;

			perror("read");
			return DA_SOCK_ERROR;
		} else if (!n_read) {
			fputs("read: got EOF()!\n", stderr);
			return 0;
		}

		msg->pos += n_read;

		if (!msg->pos[-1])              /* Last read byte is MSG_DONE */
			break;
	};

	return 0;
}

int msg_write(int fd, char *msg, size_t nbyte)
{
	ssize_t n_write;

	do {
		do {
			n_write = write(fd, msg, MIN(nbyte, buffer_size));
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
