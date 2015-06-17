#include "ldb_define.h"
#include "ldb_slice.h"
#include "ldb_bytes.h"
#include "ldb_context.h"

#include <leveldb/c.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>

static const char* LDB_SCORE_MIN = "-9223372036854775808";
static const char* LDB_SCORE_MAX = "+9223372036854775807";

static int zset_one(ldb_context_t *context, const ldb_slice_t* name, const ldb_slice_t* key, int64_t score);

static int zset_del(ldb_context_t *context, const ldb_slice_t* name, const ldb_slice_t* key);

static int zset_incr_size(ldb_context_t *context, const ldb_slice_t* name, int64_t by);

static void encode_zsize_key(const char* name, size_t namelen, ldb_slice_t** pslice){
  ldb_slice_t* slice = ldb_slice_create(LDB_DATA_TYPE_ZSIZE, strlen(LDB_DATA_TYPE_ZSIZE));
  ldb_slice_push_back(slice, name, namelen);
  *pslice = slice;
}

static int decode_zsize_key(const char* ldbkey, size_t ldbkeylen, ldb_slice_t** pslice){
  int retval = 0;
  ldb_bytes_t* bytes = ldb_bytes_create(ldbkey, ldbkeylen); 
  if(ldb_bytes_skip(bytes, strlen(LDB_DATA_TYPE_ZSIZE)) == -1){
    goto err;
  }
  ldb_slice_t *key_slice = NULL;
  if(ldb_bytes_read_slice_size_left(bytes, &key_slice) == -1){
    goto err;
  }
  goto nor;

nor:
  *pslice = key_slice;
  retval = 0;
  goto end;

err:
  retval = -1;
  ldb_slice_destroy(key_slice);
  goto end;

end:
  ldb_bytes_destroy(bytes);
  return retval; 
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

static int decode_zset_key(const char* ldbkey, size_t ldbkeylen, ldb_slice_t** pslice_name, ldb_slice_t** pslice_key){
  int retval = 0;
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
  ldb_slice_destroy(slice_name);
  ldb_slice_destroy(slice_key);
  retval = -1;
  goto end;

nor:
  *pslice_name = slice_name;
  *pslice_key = slice_key;
  retval = 0;
  goto end;

end:
  ldb_bytes_destroy(bytes);
  return retval;
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

static int decode_zscore_key(const char* ldbkey, size_t ldbkeylen, ldb_slice_t** pslice_name, ldb_slice_t** pslice_key, ldb_slice_t** pslice_score){
  int retval = 0;
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
  ldb_slice_destroy(slice_name);
  ldb_slice_destroy(slice_key);
  ldb_slice_destroy(slice_score);
  retval = -1;
  goto end;

nor:
  *pslice_name = slice_name;
  *pslice_key = slice_key;
  *pslice_score = slice_score;
  retval = 0;
  goto end;

end:
  ldb_bytes_destroy(bytes);
  return retval;
}

static int zset_one(ldb_context_t *context, const ldb_slice_t* name, const ldb_slice_t* key, int64_t score){
  if(ldb_slice_size(name)==0 || ldb_slice_size(key)==0){
    //fprintf(stderr, "");
    return 0;
  }
  return 0; 
}



static int zset_del(ldb_context_t *context, const ldb_slice_t* name, const ldb_slice_t* key){
  return 0;
}

static int zset_incr_size(ldb_context_t *context, const ldb_slice_t* name, int64_t by){
  return 0;
}
