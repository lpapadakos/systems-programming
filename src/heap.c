#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "heap.h"

struct heap_node {
	struct case_entry *entry;
	struct heap_node *parent;
	struct heap_node *left;
	struct heap_node *right;
};

struct heap_node *make_heap_node(struct case_entry *entry)
{
	struct heap_node *node = malloc(sizeof(*node));

	node->entry = entry;
	node->parent = NULL;
	node->left = NULL;
	node->right = NULL;

	return node;
}

/* README[7] */
struct heap_node *get_last_node_parent(struct heap *heap)
{
	struct heap_node *current;
	unsigned int nodes = heap->node_count;
	unsigned int bit = sizeof(heap->node_count)*CHAR_BIT - 1;
	const unsigned int mask = 1U << bit;

	// Skip zeroes
	while (!(nodes & mask)) {
		nodes <<= 1;
		bit--;
	}

	// Skip first 1
	nodes <<= 1;
	bit--;

	current = heap->root;
	while (bit) {
		//printf("%x\n", nodes);
		if (nodes & mask)
			current = current->right; // 1
		else
			current = current->left;  // 0

		nodes <<= 1;
		bit--;
	}

	return current;
}

struct heap_node *make_last_node(struct heap *heap, struct case_entry *entry)
{
	struct heap_node *current = get_last_node_parent(heap);

	// current is second-to-last-node. Has only one child.
	if (current->left) {
		current->right = make_heap_node(entry);
		current->right->parent = current;

		return current->right;
	} else {
		current->left = make_heap_node(entry);
		current->left->parent = current;

		return current->left;
	}
}

struct heap_node *get_last_node(struct heap *heap)
{
	struct heap_node *node = get_last_node_parent(heap);

	/* Heap is a complete binary tree.
	 * If there is a right child, it's the last node on this level */
	if (node->right)
		return node->right;
	else
		return node->left;
}

void swap(struct heap_node *a, struct heap_node *b)
{
	struct case_entry *temp = b->entry;
	b->entry = a->entry;
	a->entry = temp;
}

/* Pass last node to sift up after insert() */
void sift_up(struct heap_node *node)
{
	/* node->entry check in while loop because there are at least 2 nodes
	 * in the tree. first insert doesn't call heapify */
	while (node->parent && node->entry->count > node->parent->entry->count) {
		swap(node, node->parent);
		node = node->parent;
	}
}

/* Pass root as node to sift down after pop() */
void sift_down(struct heap_node *node)
{
	while (node->left || node->right) {              /* No children: Stop */
		/* One child (left, since it's a complete binary tree) */
		if (node->left && !node->right) {
			if (node->entry->count >= node->left->entry->count)
				break;

			swap(node, node->left);
			node = node->left;

			continue;
		}

		/* Two children:
		 * Compare root with the largest of the children.
		 * If root is larger, stop shifting down, otherwise swap and
		 * carry on */
		if (node->right->entry->count > node->left->entry->count) {
			if (node->entry->count >= node->right->entry->count)
				break;

			swap(node, node->right);
			node = node->right;
		} else {
			if (node->entry->count >= node->left->entry->count)
				break;

			swap(node, node->left);
			node = node->left;
		}
	}
}

/* Recursive function called by heap_destroy() */
void heap_node_destroy(struct heap_node *root)
{
	if (!root)
		return;

	heap_node_destroy(root->left);
	heap_node_destroy(root->right);

	free(root->entry);
	free(root);
}

/* Heap Interface */

void heap_insert(struct heap *heap, struct case_entry *entry)
{
	heap->node_count++;

	if (!heap->root)
		heap->root = make_heap_node(entry);
	else
		sift_up(make_last_node(heap, entry));
}

struct case_entry *heap_top(struct heap *heap)
{
	if (!heap->root)
		return NULL;

	return heap->root->entry;
}

void heap_pop(struct heap *heap)
{
	struct heap_node *last;

	if (!heap->root->left && !heap->root->right) {
		free(heap->root->entry);
		free(heap->root);

		heap->root = NULL;
		heap->node_count = 0;

		return;
	}

	last = get_last_node(heap);

	/* Replace root entry with last entry */
	free(heap->root->entry);
	heap->root->entry = last->entry;

	/* Free last node */
	if (last == last->parent->right)
		last->parent->right = NULL;
	else
		last->parent->left = NULL;

	free(last);

	heap->node_count--;

	sift_down(heap->root);
}

void heap_destroy(struct heap *heap)
{
	heap_node_destroy(heap->root);

	heap->node_count = 0;
	heap->root = NULL;
}

/* // Useful for debugging!
void heap_print(struct heap_node *node, int space)
{
	if (!node)
		return;

	space += 20;

	heap_print(node->right, space);

	putchar('\n');
	for (int i = 20; i < space; i++)
		putchar(' ');
	printf("%s %d\n", node->entry->name, node->entry->count);

	heap_print(node->left, space);
} */
