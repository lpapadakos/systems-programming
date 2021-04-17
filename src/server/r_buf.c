#include <stdlib.h>

#include "server/r_buf.h"

struct ring_buffer {
	int *buffer;
	size_t head;
	size_t tail;
	size_t capacity;
	int full;
};

struct ring_buffer *make_r_buf(size_t capacity)
{
	struct ring_buffer *r_buf = malloc(sizeof(*r_buf));

	if (r_buf) {
		r_buf->buffer = malloc(capacity * sizeof(r_buf->buffer[0]));
		r_buf->capacity = capacity;
		r_buf->head = 0;
		r_buf->tail = 0;
		r_buf->full = 0;
	}

	return r_buf;
}

int r_buf_full(struct ring_buffer *r_buf)
{
	return r_buf->full;
}

int r_buf_empty(struct ring_buffer *r_buf)
{
	return (!r_buf->full && r_buf->head == r_buf->tail);
}

void r_buf_push(struct ring_buffer *r_buf, int data)
{
	if (r_buf->full)
		return;

	r_buf->buffer[r_buf->head++] = data;

	if (r_buf->head >= r_buf->capacity)
		r_buf->head = 0;

	r_buf->full = (r_buf->head == r_buf->tail);
}

int r_buf_pop(struct ring_buffer *r_buf)
{
	int data;

	if (r_buf_empty(r_buf))
		return -1;

	data = r_buf->buffer[r_buf->tail++];

	if (r_buf->tail >= r_buf->capacity)
		r_buf->tail = 0;

	r_buf->full = 0;

	return data;
}

int r_buf_peek(struct ring_buffer *r_buf)
{
	return r_buf->buffer[r_buf->tail];
}

void r_buf_destroy(struct ring_buffer *r_buf)
{
	free(r_buf->buffer);
	free(r_buf);
}
