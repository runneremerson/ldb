#include "ldb_define.h"
#include "ldb_slice.h"
#include "ldb_context.h"
#include "ldb_meta.h"
#include "ldb_bytes.h"

#include <leveldb/c.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>



static void encode_kv_key(const char* key, size_t keylen, ldb_slice_t** pslice){
  ldb_slice_t* slice = ldb_slice_create(LDB_DATA_TYPE_KV, 1);
  ldb_slice_push_back(slice, key, keylen);
  *pslice =  slice;
}

static int decode_kv_key(const char* ldbkey, size_t ldbkeylen, ldb_slice_t** pslice){
  int retval = 0;
  ldb_bytes_t* bytes = ldb_bytes_create(ldbkey, ldbkeylen);
  if(ldb_bytes_skip(bytes, strlen(LDB_DATA_TYPE_KV)) == -1 ){
    goto err;
  }
  ldb_slice_t *slice_key = NULL;
  if(ldb_bytes_read_slice_size_left(bytes, &slice_key) == -1){
    goto err;
  }
  goto nor;

nor:
  *pslice = slice_key;
  retval = 0;
  goto end;
err:
  if(slice_key != NULL){
    ldb_slice_destroy(slice_key);
  }
  retval = -1;
  goto end; 
end: 
  ldb_bytes_destroy(bytes);
  return retval;
}

int string_set(ldb_context_t* context, const ldb_slice_t* key, const ldb_slice_t* value, const ldb_meta_t* meta){
  if(ldb_slice_size(key) == 0){
    fprintf(stderr, "empty key!\n");
    return 0;
  }
  char *errptr = NULL;
  leveldb_writeoptions_t* writeoptions = leveldb_writeoptions_create();
  ldb_slice_t *slice_key = NULL;
  encode_kv_key(ldb_slice_data(key), ldb_slice_size(key), &slice_key);
  ldb_slice_t *slice_val = NULL;
  slice_val = ldb_meta_slice_create(meta);
  ldb_slice_push_back( slice_val , ldb_slice_data(value), ldb_slice_size(value));
  leveldb_put(context->database_, 
              writeoptions, 
              ldb_slice_data(slice_key), 
              ldb_slice_size(slice_key), 
              ldb_slice_data(slice_val), 
              ldb_slice_size(slice_val), 
              &errptr);
  leveldb_writeoptions_destroy(writeoptions);
  ldb_slice_destroy(slice_key);
  ldb_slice_destroy(slice_val);
  if(errptr != NULL){
    fprintf(stderr, "leveldb_put fail %s.\n", errptr);
    leveldb_free(errptr);
    return -1;
  }
  return 1;
}

int string_setnx(ldb_context_t* context, const ldb_slice_t* key, const ldb_slice_t* value, const ldb_meta_t* meta){
  
}

int string_setxx(ldb_context_t* context, const ldb_slice_t* key, const ldb_slice_t* value, const ldb_meta_t* meta){
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
  return 1;
}
