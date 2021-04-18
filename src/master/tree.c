/* Balanced BST (AVL) implementation */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "master/tree.h"

/* Small stack implementation */
struct stack_node {
	struct tree_node *node;
	struct stack_node *next;
};

void stack_push(struct stack_node **top, struct tree_node *node)
{
	struct stack_node *temp = malloc(sizeof(*temp));

	if (!temp) {
		perror("stack_node malloc()");
		return;
	}

	/* temp is the new top */
	temp->node = node;
	temp->next = *top;
	*top = temp;
}

struct tree_node *stack_pop(struct stack_node **top)
{
	struct stack_node *temp;
	struct tree_node *node;

	if (!*top) {
		return 0;
	}

	node = (*top)->node;

	temp = *top;
	*top = (*top)->next;
	free(temp);

	return node;
}

void stack_destroy(struct stack_node **top)
{
	while (stack_pop(top)) {
		continue;
	};
}

/* Tree Definition
 * The key is the patient_record->entryDate */
/* struct tree_node {
	struct record *patient_record;
	struct tree_node *left;
	struct tree_node *right;
}; */

struct tree_node *make_node(struct record *record)
{
	struct tree_node *node = malloc(sizeof(*node));

	node->patient_record = record;
	node->left = NULL;
	node->right = NULL;

	return node;
}

int less(struct record *a, struct record *b)
{
	return (datecmp(&a->entry_date, &b->entry_date) < 0);
}

/* Tree Interface */

struct tree_node *tree_insert(struct tree_node *root, struct record *record)
{
	if (!root)
		return make_node(record);

	if (less(record, root->patient_record))
		root->left = tree_insert(root->left, record);
	else
		root->right = tree_insert(root->right, record);

	return root;
}

struct tree_node *tree_find_gte_node(struct tree_node *root, struct date *entry_date)
{
	if (!root->left && !root->right &&
	    datecmp(entry_date, &root->patient_record->entry_date) > 0)
		return NULL;

	if (datecmp(entry_date, &root->patient_record->entry_date) <= 0) {
		if (!root->left)
			return root;
		else if (datecmp(entry_date, &root->left->patient_record->entry_date) < 0)
			return root;
	}

	if (datecmp(entry_date, &root->patient_record->entry_date) < 0)
		return tree_find_gte_node(root->left, entry_date);
	else
		return tree_find_gte_node(root->right, entry_date);
}

/* InOrder Traversal, using a stack */
struct record *tree_get_next_record(struct tree_node *root)
{
	static struct stack_node *stack = NULL;
	static struct tree_node *current;

	struct record *ret;

	if (root) {
		stack_destroy(&stack);
		current = root;
	}

	if (!current) {
		if (!stack)
			return NULL;

		current = stack_pop(&stack);
		ret = current->patient_record;

		current = current->right;

		return ret;
	}

	while (current) {
		stack_push(&stack, current);
		current = current->left;
	}

	return tree_get_next_record(NULL);
}

void tree_destroy(struct tree_node *root)
{
	if (!root)
		return;

	tree_destroy(root->left);
	tree_destroy(root->right);

	free(root);
}
