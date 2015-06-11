#include "ldb_define.h"
#include "ldb_slice.h"
#include "ldb_context.h"

#include <leveldb/c.h>
#include <stdlib.h>
#include <stdio.h>



static ldb_slice_t* encode_kv_key(const char* key, size_t size){
    ldb_slice_t* slice = ldb_slice_create(LDB_DATA_TYPE_KV, 1);
    ldb_slice_push_back(slice, key, size);
    return slice;
}

static ldb_slice_t* decode_kv_key(const char* key, size_t size){
    if(size>1){
        ldb_slice_t* slice = ldb_slice_create(key + 1, size - 1 );
        return slice;
    }
    return NULL;
}

int string_set(ldb_context_t* context, const ldb_slice_t* key, const ldb_slice_t* value){
  char *errptr = NULL;
  leveldb_writeoptions_t* writeoptions = leveldb_writeoptions_create();
  ldb_slice_t *slice = encode_kv_key(key->data_, key->size_);
  leveldb_put(context->database_, writeoptions, slice->data_, slice->size_, value->data_, value->size_, &errptr);
  leveldb_writeoptions_destroy(writeoptions);
  ldb_slice_destroy(slice);
  if(errptr != NULL){
    fprintf(stderr, "leveldb_put fail %s.\n", errptr);
    leveldb_free(errptr);
    return -1;
  }
  return 0;
}

int string_get(ldb_context_t* context, const ldb_slice_t* key, ldb_slice_t** pvalue){
  char *val, *errptr = NULL;
  size_t vallen = 0;
  leveldb_readoptions_t* readoptions = leveldb_readoptions_create();
  ldb_slice_t *slice = encode_kv_key(key->data_, key->size_);
  val = leveldb_get(context->database_, readoptions, slice->data_, slice->size_, &vallen, &errptr);
  leveldb_readoptions_destroy(readoptions);
  ldb_slice_destroy(slice);
  if(errptr != NULL){
    fprintf(stderr, "leveldb_get fail %s.\n", errptr);
    leveldb_free(errptr);
    return -1;
  }
  if(val != NULL){
    *pvalue = ldb_slice_create(val, vallen);
    leveldb_free(val);
  }
  return 0;
}
