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

typedef struct ldb_data_iterator_t ldb_data_iterator_t;


ldb_data_iterator_t* ldb_data_iterator_create(ldb_context_t *context, 
                                              ldb_slice_t *type, ldb_slice_t *start, ldb_slice_t *end, uint64_t limit, int direction);
void ldb_data_iterator_destroy(ldb_data_iterator_t* iterator);

int ldb_data_iterator_skip(ldb_data_iterator_t *iterator, uint64_t offset);

int ldb_data_iterator_next(ldb_data_iterator_t *iterator);

void ldb_data_iterator_val(const ldb_data_iterator_t *iterator, ldb_slice_t **pslice);

void ldb_data_iterator_key(const ldb_data_iterator_t *iterator, ldb_slice_t **pslice); 



#endif //LDB_ITERATOR_H

