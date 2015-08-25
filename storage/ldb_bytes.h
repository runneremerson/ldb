#ifndef LDB_BYTES_H
#define LDB_BYTES_H

#include "ldb_slice.h"

#include <stddef.h>
#include <stdint.h>

typedef struct  ldb_bytes_t     ldb_bytes_t;

ldb_bytes_t* ldb_bytes_create(const char* data, size_t size);
void ldb_bytes_destroy(ldb_bytes_t* bytes);

int ldb_bytes_skip(ldb_bytes_t* bytes, size_t n);

int ldb_bytes_read_int64(ldb_bytes_t* bytes, int64_t* val);

int ldb_bytes_read_uint64(ldb_bytes_t* bytes, uint64_t* val);

int ldb_bytes_read_slice_size_left(ldb_bytes_t* bytes, ldb_slice_t** pslice);

int ldb_bytes_read_slice_size_uint8(ldb_bytes_t* bytes, ldb_slice_t** pslice);

int ldb_bytes_read_slice_size_uint16(ldb_bytes_t* bytes, ldb_slice_t** pslice);

#endif //LDB_BYTES_H
