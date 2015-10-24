#ifndef LDB_ITERATOR_H
#define LDB_ITERATOR_H

#include "ldb_context.h"
#include "ldb_slice.h"

#include <stdint.h>
#include <stddef.h>

enum {
    FORWARD = 0,
    BACKWARD = 1
};

typedef struct ldb_data_iterator_t ldb_zset_iterator_t;

typedef struct ldb_data_iterator_t ldb_hash_iterator_t;

typedef struct ldb_data_iterator_t ldb_kv_iterator_t;


ldb_zset_iterator_t* ldb_zset_iterator_create(ldb_context_t *context, const ldb_slice_t *name,
                                              ldb_slice_t *start, ldb_slice_t *end, uint64_t limit, int direction);

void ldb_zset_iterator_destroy(ldb_zset_iterator_t* iterator);

int ldb_zset_iterator_skip(ldb_zset_iterator_t *iterator, uint64_t offset);

int ldb_zset_iterator_next(ldb_zset_iterator_t *iterator);

void ldb_zset_iterator_val(const ldb_zset_iterator_t *iterator, ldb_slice_t **pslice);

void ldb_zset_iterator_key(const ldb_zset_iterator_t *iterator, ldb_slice_t **pslice); 



ldb_hash_iterator_t* ldb_hash_iterator_create(ldb_context_t *context, const ldb_slice_t *name,
                                              ldb_slice_t *start, ldb_slice_t *end, uint64_t limit, int direction);

void ldb_hash_iterator_destroy(ldb_hash_iterator_t* iterator);

int ldb_hash_iterator_next(ldb_hash_iterator_t *iterator);

void ldb_hash_iterator_val(const ldb_hash_iterator_t *iterator, ldb_slice_t **pslice);

void ldb_hash_iterator_key(const ldb_hash_iterator_t *iterator, ldb_slice_t **pslice); 


ldb_kv_iterator_t* ldb_kv_iterator_create(ldb_context_t *context, ldb_slice_t *start, ldb_slice_t *end, uint64_t limit, int direction);

void ldb_kv_iterator_destroy(ldb_kv_iterator_t* iterator);

int ldb_kv_iterator_next(ldb_kv_iterator_t *iterator);

void ldb_kv_iterator_val(const ldb_kv_iterator_t *iterator, ldb_slice_t **pslice);

void ldb_kv_iterator_key(const ldb_kv_iterator_t *iterator, ldb_slice_t **pslice); 

#endif //LDB_ITERATOR_H

