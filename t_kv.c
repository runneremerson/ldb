#include "ldb_define.h"
#include "ldb_slice.h"
#include "ldb_context.h"
#include "ldb_meta.h"
#include "ldb_bytes.h"
#include "t_kv.h"

#include <leveldb-ldb/c.h>
#include <string.h>
#include <stdlib.h>
#include <assert.h>
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
  ldb_slice_destroy(slice_key);
  retval = -1;
  goto end; 
end: 
  ldb_bytes_destroy(bytes);
  return retval;
}

int string_set(ldb_context_t* context, const ldb_slice_t* key, const ldb_slice_t* value, const ldb_meta_t* meta){
  leveldb_mutex_lock(context->mutex_);
  int retval = LDB_OK;
  if(ldb_slice_size(key) == 0){
    fprintf(stderr, "empty key!\n");
    retval = LDB_ERR;
    goto end;
  }
  char *errptr = NULL;
  leveldb_writeoptions_t* writeoptions = leveldb_writeoptions_create();
  ldb_slice_t *slice_key = NULL;
  encode_kv_key(ldb_slice_data(key), ldb_slice_size(key), &slice_key);
  ldb_slice_t *slice_mat = ldb_meta_slice_create(meta);
  ldb_slice_push_front( slice_key , ldb_slice_data(slice_mat), ldb_slice_size(slice_mat));
  leveldb_put(context->database_, 
              writeoptions, 
              ldb_slice_data(slice_key), 
              ldb_slice_size(slice_key), 
              ldb_slice_data(value), 
              ldb_slice_size(value), 
              &errptr);
  leveldb_writeoptions_destroy(writeoptions);
  ldb_slice_destroy(slice_key);
  ldb_slice_destroy(slice_mat);
  if(errptr != NULL){
    fprintf(stderr, "leveldb_put fail %s.\n", errptr);
    leveldb_free(errptr);
    retval = LDB_ERR;
    goto end;
  }
  retval = LDB_OK;

end:
  leveldb_mutex_unlock(context->mutex_);
  return retval;
}

int string_setnx(ldb_context_t* context, const ldb_slice_t* key, const ldb_slice_t* value, const ldb_meta_t* meta){
  leveldb_mutex_lock(context->mutex_);
  int retval = LDB_OK;
  if(ldb_slice_size(key) == 0){
    fprintf(stderr, "empty key!\n");
    retval = LDB_ERR;
    goto end;
  }
  //get
  ldb_slice_t *slice_value = NULL;
  int found = string_get(context, key, &slice_value);
  if(found == LDB_OK){
    retval = LDB_OK_BUT_ALREADY_EXIST;
    goto end;
  }
  //set
  char *errptr = NULL;
  leveldb_writeoptions_t* writeoptions = leveldb_writeoptions_create();
  ldb_slice_t *slice_key = NULL;
  encode_kv_key(ldb_slice_data(key), ldb_slice_size(key), &slice_key);
  ldb_slice_t *slice_mat = ldb_meta_slice_create(meta);
  ldb_slice_push_front( slice_key , ldb_slice_data(slice_mat), ldb_slice_size(slice_mat));
  leveldb_put(context->database_, 
              writeoptions, 
              ldb_slice_data(slice_key), 
              ldb_slice_size(slice_key), 
              ldb_slice_data(value), 
              ldb_slice_size(value), 
              &errptr);
  leveldb_writeoptions_destroy(writeoptions);
  ldb_slice_destroy(slice_key);
  ldb_slice_destroy(slice_mat);
  if(errptr != NULL){
    fprintf(stderr, "leveldb_put fail %s.\n", errptr);
    leveldb_free(errptr);
    retval = LDB_ERR;
    goto end;
  }
  retval = LDB_OK; 

end:
  leveldb_mutex_unlock(context->mutex_);
  return retval;
}

int string_setxx(ldb_context_t* context, const ldb_slice_t* key, const ldb_slice_t* value, const ldb_meta_t* meta){
  leveldb_mutex_lock(context->mutex_);
  int retval = LDB_OK;
  if(ldb_slice_size(key) == 0){
    fprintf(stderr, "empty key!\n");
    retval = LDB_ERR;
    goto end;
  }
  //get
  ldb_slice_t *slice_value = NULL;
  int found = string_get(context, key, &slice_value);
  if(found != LDB_OK){
    retval = LDB_OK_NOT_EXIST;
    goto end;
  }
  //set
  char *errptr = NULL;
  leveldb_writeoptions_t* writeoptions = leveldb_writeoptions_create();
  ldb_slice_t *slice_key = NULL;
  encode_kv_key(ldb_slice_data(key), ldb_slice_size(key), &slice_key);
  ldb_slice_t *slice_mat = ldb_meta_slice_create(meta);
  ldb_slice_push_front( slice_key , ldb_slice_data(slice_mat), ldb_slice_size(slice_mat));
  leveldb_put(context->database_, 
              writeoptions, 
              ldb_slice_data(slice_key), 
              ldb_slice_size(slice_key), 
              ldb_slice_data(value), 
              ldb_slice_size(value), 
              &errptr);
  leveldb_writeoptions_destroy(writeoptions);
  ldb_slice_destroy(slice_key);
  ldb_slice_destroy(slice_mat);
  if(errptr != NULL){
    fprintf(stderr, "leveldb_put fail %s.\n", errptr);
    leveldb_free(errptr);
    retval = LDB_ERR;
    goto end;
  }
  retval = LDB_OK; 
end:
  leveldb_mutex_unlock(context->mutex_);
  return retval;
}

int string_get(ldb_context_t* context, const ldb_slice_t* key, ldb_slice_t** pvalue){
  char *val, *errptr = NULL;
  size_t vallen = 0;
  leveldb_readoptions_t* readoptions = leveldb_readoptions_create();
  ldb_slice_t *slice_key = NULL;
  encode_kv_key(ldb_slice_data(key), ldb_slice_size(key), &slice_key);
  val = leveldb_get(context->database_, readoptions, ldb_slice_data(slice_key), ldb_slice_size(slice_key), &vallen, &errptr);
  leveldb_readoptions_destroy(readoptions);
  ldb_slice_destroy(slice_key);
  int retval = LDB_OK;
  if(errptr != NULL){
    fprintf(stderr, "leveldb_get fail %s.\n", errptr);
    leveldb_free(errptr);
    retval = LDB_ERR;
    goto end;
  }
  if(val != NULL){
    size_t mat_size = 1+8;
    assert(vallen>= mat_size);
    uint8_t type = leveldb_decode_fixed8(val);
    if(type == LDB_VALUE_TYPE_VAL){
      *pvalue = ldb_slice_create(val+1, vallen-1);
      retval = LDB_OK;
    }else{
      retval = LDB_OK_NOT_EXIST;
    }
    leveldb_free(val);
    goto end;
  }else{
    retval = LDB_OK_NOT_EXIST;
  }

end:
  return retval;
}

