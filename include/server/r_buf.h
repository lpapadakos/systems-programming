#include <stdlib.h>

/* Circular (ring) buffer implementation
   Data being pushed on a full buffer is discarded */
struct ring_buffer;

struct ring_buffer *make_r_buf(size_t capacity);

int r_buf_full(struct ring_buffer *r_buf);
int r_buf_empty(struct ring_buffer *r_buf);

void r_buf_push(struct ring_buffer *r_buf, int data);
int r_buf_pop(struct ring_buffer *r_buf);
int r_buf_peek(struct ring_buffer *r_buf);

void r_buf_destroy(struct ring_buffer *r_buf);
