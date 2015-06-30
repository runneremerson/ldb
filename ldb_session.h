#ifndef LDB_SESSION_H
#define LDB_SESSION_H

#include "ldb_context.h"
#include "ldb_slice.h"
#include "ldb_meta.h"

#include "cgo_util/base.h"


#include <stdint.h>


typedef struct value_item_t{
  uint64_t version_;
  size_t data_len_;
  char* data_;
} value_item_t;

void fill_value_item(value_item_t* item, uint64_t version, const char* data, size_t size);

void free_value_item(value_item_t* item);

void destroy_value_item_array( value_item_t* array, size_t size);

value_item_t* create_value_item_array( size_t size);




int set_ldb_signal_handler(const char* name);



//string
int ldb_set(ldb_context_t* context,
            uint32_t area, 
            char* key, 
            size_t keylen, 
            uint64_t lastver, 
            int vercare, 
            long exptime, 
            value_item_t* item, 
            int en);
int ldb_mset(ldb_context_t* context, 
             uint32_t area, 
             uint64_t lastver, 
             int vercare, 
             long exptime, 
             GoByteSlice* keyvals, 
             GoUint64Slice* versions, 
             int length,
             GoUint64Slice* retvals,
             int en);

int ldb_get(ldb_context_t* context, 
            uint32_t area, 
            char* key, 
            size_t keylen, 
            value_item_t** item);

int ldb_mget(ldb_context_t* context,
             uint32_t area,
             GoByteSlice* slice,
             int length,
             GoByteSliceSlice* items,
             GoUint64Slice* versions,
             int* number);

int ldb_del(ldb_context_t* context, 
            uint32_t area, 
            char* key, 
            size_t keylen, 
            int vercare, 
            uint64_t version);

int ldb_incrby(ldb_context_t* context,
               uint32_t area,
               char* key,
               size_t keylen,
               uint64_t lastver,
               int vercare,
               long exptime,
               uint64_t version,
               int64_t initval,
               int64_t by,
               int64_t* reault);



//hash

//zset





#endif //LDB_SESSION_H
