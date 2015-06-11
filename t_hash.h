#ifndef LDB_T_HASH_H
#define LDB_T_HASH_H

#include "ldb_define.h"
#include "ldb_slice.h"

#include <stdlib.h>
#include <unistd.h>
#include <assert.h>

static ldb_slice_t* encode_hsize_key(const char* name, size_t size){
    ldb_slice_t* slice = ldb_slice_create(LDB_DATA_TYPE_HSIZE, 1);
    return ldb_slice_push_back(slice, name, size);
}





#endif //LDB_T_HASH_H
