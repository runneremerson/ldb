#ifndef LDB_SLICE_LIST_H
#define LDB_SLICE_LIST_H

#include "ldb_slice.h"

#include <stddef.h>
#include <stdint.h>

struct ldb_slice_node_t {
  struct ldb_slice_node_t* next_;
  struct ldb_slice_node_t* prev_;
  ldb_slice_t* slice_;
};

struct ldb_slice_list_t {
  struct ldb_slice_node_t *head_;
  struct ldb_slice_node_t *tail_;
  size_t length_;
};

typedef struct ldb_slice_list_t         ldb_slice_list_t;
typedef struct ldb_slice_node_t         ldb_slice_node_t;

ldb_slice_node_t* ldb_slice_node_create();
void ldb_slice_node_destroy(ldb_slice_node_t* slice_node);

ldb_slice_list_t* ldb_slice_list_create();
void ldb_slice_list_destroy(ldb_slice_list_t* slice_list);

size_t rpush_ldb_slice_node(ldb_slice_list_t* list, ldb_slice_node_t* node);
size_t lpush_ldb_slice_node(ldb_slice_list_t* list, ldb_slice_node_t* node);

ldb_slice_node_t* rpop_ldb_slice_node(ldb_slice_list_t* list);
ldb_slice_node_t* lpop_ldb_slice_node(ldb_slice_list_t* list);

#endif //LDB_SLICE_LIST_H
