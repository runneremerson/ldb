#ifndef LDB_SLICE_H
#define LDB_SLICE_H

#include <stddef.h>
#include <stdint.h>


struct ldb_slice_t {
    size_t size_;
    char *data_;
};

typedef struct ldb_slice_t  ldb_slice_t;


ldb_slice_t* ldb_slice_create(const char* data, size_t size);
void ldb_slice_destroy(ldb_slice_t* slice); 

void ldb_slice_push_back(ldb_slice_t* slice, const char* data, size_t size);

void ldb_slice_push_front(ldb_slice_t* slice, const char* data, size_t size);



#endif //LDB_SLICE_H
