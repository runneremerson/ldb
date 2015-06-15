#include "ldb_define.h"
#include "ldb_slice.h"
#include "ldb_context.h"

#include <leveldb/c.h>
#include <stdlib.h>
#include <stdio.h>



static void encode_kv_key(const char* key, size_t keylen, ldb_slice_t** pslice){
  ldb_slice_t* slice = ldb_slice_create(LDB_DATA_TYPE_KV, 1);
  ldb_slice_push_back(slice, key, keylen);
  *pslice =  slice;
}

static void decode_kv_key(const char* ldbkey, size_t ldbkeylen, ldb_slice_t** pslice){
  if(ldbkeylen>1){
    ldb_slice_t* slice = ldb_slice_create(ldbkey + 1, ldbkeylen - 1 );
    *pslice = slice;
  }else{
    *pslice = NULL;
  }
}

int string_set(ldb_context_t* context, const ldb_slice_t* key, const ldb_slice_t* value){
  char *errptr = NULL;
  leveldb_writeoptions_t* writeoptions = leveldb_writeoptions_create();
  ldb_slice_t *slice = NULL;
  encode_kv_key(ldb_slice_data(key), ldb_slice_size(key), &slice);
  leveldb_put(context->database_, writeoptions, ldb_slice_data(slice), ldb_slice_size(slice), ldb_slice_data(value), ldb_slice_size(value), &errptr);
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
  ldb_slice_t *slice = NULL;
  encode_kv_key(ldb_slice_data(key), ldb_slice_size(key), &slice);
  val = leveldb_get(context->database_, readoptions, ldb_slice_data(slice), ldb_slice_size(slice), &vallen, &errptr);
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
