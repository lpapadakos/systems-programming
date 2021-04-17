/* Binary Heap Implementation */

#ifndef HEAP_H
#define HEAP_H

#include "hashtable.h"

/* Must be initialized as {0} */
struct heap {
	unsigned int node_count;
	struct heap_node *root;
};

/* Interface */

void heap_insert(struct heap *heap, struct case_entry *entry);
struct case_entry *heap_top(struct heap *heap);
void heap_pop(struct heap *heap);

void heap_destroy(struct heap *heap);

#endif /* HEAP_H */
