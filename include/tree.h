#ifndef TREE_H
#define TREE_H

#include "record.h"

/* Declarations */
struct tree_node;

/* Interface */
struct tree_node *tree_insert(struct tree_node *root, struct record *record);
struct tree_node *tree_find_gte_node(struct tree_node *root, struct date *entry_date);

/* Upon first use (or to reset), pass the root of your tree as argument.
 * Afterwards, pass NULL. It works like strtok in this regard. */
struct record *tree_get_next_record(struct tree_node *root);

void tree_destroy(struct tree_node *root);

#endif /* TREE_H */
