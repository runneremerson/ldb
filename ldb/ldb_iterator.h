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

typedef struct ldb_data_iterator_t ldb_string_iterator_t;

typedef struct ldb_data_iterator_t ldb_set_iterator_t;


ldb_zset_iterator_t* ldb_zset_iterator_create(ldb_context_t *context, const ldb_slice_t *name,
                                              ldb_slice_t *start, ldb_slice_t *end, uint64_t limit, int direction);

void ldb_zset_iterator_destroy(ldb_zset_iterator_t* iterator);

int ldb_zset_iterator_skip(ldb_zset_iterator_t *iterator, uint64_t offset);

int ldb_zset_iterator_next(ldb_zset_iterator_t *iterator);

void ldb_zset_iterator_val(const ldb_zset_iterator_t *iterator, ldb_slice_t **pslice);

void ldb_zset_iterator_key(const ldb_zset_iterator_t *iterator, ldb_slice_t **pslice); 

const char* ldb_zset_iterator_val_raw(const ldb_zset_iterator_t *iterator, size_t* vlen);
const char* ldb_zset_iterator_key_raw(const ldb_zset_iterator_t *iterator, size_t* klen);

int ldb_zset_iterator_valid(const ldb_zset_iterator_t *iterator);



ldb_hash_iterator_t* ldb_hash_iterator_create(ldb_context_t *context, const ldb_slice_t *name,
                                              ldb_slice_t *start, ldb_slice_t *end, uint64_t limit, int direction);

void ldb_hash_iterator_destroy(ldb_hash_iterator_t* iterator);

int ldb_hash_iterator_next(ldb_hash_iterator_t *iterator);

void ldb_hash_iterator_val(const ldb_hash_iterator_t *iterator, ldb_slice_t **pslice);

void ldb_hash_iterator_key(const ldb_hash_iterator_t *iterator, ldb_slice_t **pslice); 

const char* ldb_hash_iterator_val_raw(const ldb_hash_iterator_t *iterator, size_t* vlen);

const char* ldb_hash_iterator_key_raw(const ldb_hash_iterator_t *iterator, size_t* klen);



ldb_set_iterator_t* ldb_set_iterator_create(ldb_context_t *context, const ldb_slice_t *name,
                                              ldb_slice_t *start, ldb_slice_t *end, uint64_t limit, int direction);

void ldb_set_iterator_destroy(ldb_set_iterator_t* iterator);

int ldb_set_iterator_skip(ldb_zset_iterator_t *iterator, uint64_t offset);

int ldb_set_iterator_next(ldb_set_iterator_t *iterator);

void ldb_set_iterator_val(const ldb_set_iterator_t *iterator, ldb_slice_t **pslice);

void ldb_set_iterator_key(const ldb_set_iterator_t *iterator, ldb_slice_t **pslice); 

const char* ldb_set_iterator_val_raw(const ldb_set_iterator_t *iterator, size_t* vlen);

const char* ldb_set_iterator_key_raw(const ldb_set_iterator_t *iterator, size_t* klen);


ldb_string_iterator_t* ldb_string_iterator_create(ldb_context_t *context, ldb_slice_t *start, ldb_slice_t *end, uint64_t limit, int direction);

void ldb_string_iterator_destroy(ldb_string_iterator_t* iterator);

int ldb_string_iterator_next(ldb_string_iterator_t *iterator);

void ldb_string_iterator_val(const ldb_string_iterator_t *iterator, ldb_slice_t **pslice);

void ldb_string_iterator_key(const ldb_string_iterator_t *iterator, ldb_slice_t **pslice); 

const char* ldb_string_iterator_val_raw(const ldb_string_iterator_t *iterator, size_t* vlen);

const char* ldb_string_iterator_key_raw(const ldb_string_iterator_t *iterator, size_t* klen);

int ldb_string_iterator_valid(const ldb_string_iterator_t *iterator);

#endif //LDB_ITERATOR_H

