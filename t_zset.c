#include "ldb_define.h"
#include "ldb_slice.h"
#include "ldb_bytes.h"
#include "ldb_context.h"

#include <leveldb/c.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>

static void encode_zsize_key(const char* name, size_t namelen, ldb_slice_t** pslice){
  ldb_slice_t* slice = ldb_slice_create(LDB_DATA_TYPE_ZSIZE, strlen(LDB_DATA_TYPE_ZSIZE));
  ldb_slice_push_back(slice, name, namelen);
  *pslice = slice;
}

static void decode_zsize_key(const char* ldbkey, size_t ldbkeylen, ldb_slice_t** pslice){
  if(ldbkeylen>strlen(LDB_DATA_TYPE_ZSIZE)){
    ldb_slice_t* slice = ldb_slice_create(ldbkey + strlen(LDB_DATA_TYPE_ZSIZE), ldbkeylen - strlen(LDB_DATA_TYPE_ZSIZE)); 
    *pslice = slice;
  }else{
    *pslice = NULL;
  }
}

static void encode_zset_key(const char* name, size_t namelen, const char* key, size_t keylen, ldb_slice_t** pslice){
  uint8_t len = 0;
  ldb_slice_t* slice = ldb_slice_create(LDB_DATA_TYPE_ZSET, strlen(LDB_DATA_TYPE_ZSET));
  len = (uint8_t)namelen;
  ldb_slice_push_back(slice, (const char*)(&len), sizeof(uint8_t));
  ldb_slice_push_back(slice, name, namelen);
  len = (uint8_t)keylen;
  ldb_slice_push_back(slice, (const char*)(&len), sizeof(uint8_t));
  ldb_slice_push_back(slice, key, keylen);
  *pslice = slice;
}

static void decode_zset_key(const char* ldbkey, size_t ldbkeylen, ldb_slice_t** pslice_name, ldb_slice_t** pslice_key){
  ldb_bytes_t *bytes = ldb_bytes_create(ldbkey, ldbkeylen);
  if(ldb_bytes_skip(bytes, strlen(LDB_DATA_TYPE_ZSET)) == -1){
    goto err;
  }
  ldb_slice_t *slice_name = NULL;
  if(ldb_bytes_read_slice_size_uint8(bytes, &slice_name) == -1){
    goto err;
  }
  ldb_slice_t *slice_key = NULL;
  if(ldb_bytes_read_slice_size_uint8(bytes, &slice_key) == -1){
    goto err;
  }
  goto nor;

err:
  if(slice_name != NULL){
    ldb_slice_destroy(slice_name);
  }
  if(slice_key != NULL){
    ldb_slice_destroy(slice_key);
  }
  goto end;

nor:
  *pslice_name = slice_name;
  *pslice_key = slice_key;
  goto end;
end:
  return;
}

static void encode_zscore_key(const char* name, size_t namelen, const char* key, size_t keylen, const char* score, size_t scorelen){
  uint8_t len = 0;
  ldb_slice_t *slice = ldb_slice_create(LDB_DATA_TYPE_ZSCORE, strlen(LDB_DATA_TYPE_ZSCORE));
  len = (uint8_t)namelen;
  ldb_slice_push_back(slice, (const char*)(&len), sizeof(uint8_t));
  ldb_slice_push_back(slice, name, namelen);
  int64_t s = leveldb_decode_fixed64(score);
  if(s < 0){
    ldb_slice_push_back(slice, "-", 1);
  }else{
    ldb_slice_push_back(slice, "=", 1);
  }
  ldb_slice_push_back(slice, score, scorelen);
  ldb_slice_push_back(slice, "=", 1);
  ldb_slice_push_back(slice, key, keylen); 
}

static void decode_zscore_key(const char* ldbkey, size_t ldbkeylen, ldb_slice_t** pslice_name, ldb_slice_t** pslice_key, ldb_slice_t** pslice_score){
  ldb_bytes_t *bytes = ldb_bytes_create(ldbkey, ldbkeylen);
  if(ldb_bytes_skip(bytes, strlen(LDB_DATA_TYPE_ZSCORE) == -1)){
    goto err;
  }
  ldb_slice_t *slice_name = NULL;
  if(ldb_bytes_read_slice_size_uint8(bytes, &slice_name) == -1){
    goto err;
  }
  if(ldb_bytes_skip(bytes, 1) == -1){
    goto err;
  }
  int64_t s = 0;
  ldb_slice_t *slice_score = NULL;
  if(ldb_bytes_read_int64(bytes, &s) == -1){
    goto err;
  }else{
    char sbuff[sizeof(uint64_t)] = {0};
    leveldb_encode_fixed64(sbuff, s);
    slice_score = ldb_slice_create(sbuff, sizeof(uint64_t));
  }
  if(ldb_bytes_skip(bytes, 1) == -1){
    goto err;
  }
  ldb_slice_t *slice_key = NULL;
  if(ldb_bytes_read_slice_size_left(bytes, &slice_key) == -1){
    goto err;
  }
  goto nor;

err:
  if(slice_name != NULL){
    ldb_slice_destroy(slice_name);
  }
  if(slice_key != NULL){
    ldb_slice_destroy(slice_key);
  }
  if(slice_score != NULL){
    ldb_slice_destroy(slice_score);
  }
  goto end;

nor:
  *pslice_name = slice_name;
  *pslice_key = slice_key;
  *pslice_score = slice_score;
  goto end;

end:
  return;
}
