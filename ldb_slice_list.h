#ifndef LDB_SLICE_LIST_H
#define LDB_SLICE_LIST_H


#include <stddef.h>
#include <stdint.h>

struct ldb_list_node_t {
  struct ldb_list_node_t* next_;
  struct ldb_list_node_t* prev_;
  uint32_t type_;
  uint64_t value_;
  void* data_;
};

struct ldb_list_t {
  struct ldb_list_node_t *head_;
  struct ldb_list_node_t *tail_;
  size_t length_;
};

typedef struct ldb_list_t               ldb_list_t;
typedef struct ldb_list_node_t          ldb_list_node_t;

ldb_list_node_t* ldb_list_node_create();
void ldb_list_node_destroy(ldb_list_node_t* slice_node);

ldb_list_t* ldb_list_create();
void ldb_list_destroy(ldb_list_t* list);

size_t rpush_ldb_list_node(ldb_list_t* list, ldb_list_node_t* node);
size_t lpush_ldb_list_node(ldb_list_t* list, ldb_list_node_t* node);

ldb_list_node_t* rpop_ldb_slice_node(ldb_list_t* list);
ldb_list_node_t* lpop_ldb_slice_node(ldb_list_t* list);

#endif //LDB_SLICE_LIST_H
