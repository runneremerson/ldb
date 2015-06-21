#include "ldb_define.h"
#include "ldb_slice.h"
#include "ldb_bytes.h"
#include "ldb_context.h"
#include "t_zset.h"

#include <leveldb-ldb/c.h>
#include <stdlib.h>
#include <stdio.h>
#include <assert.h>
#include <string.h>

static const char* LDB_SCORE_MIN = "-9223372036854775808";
static const char* LDB_SCORE_MAX = "+9223372036854775807";

static int zset_one(ldb_context_t *context, const ldb_slice_t* name, const ldb_slice_t* key, const ldb_slice_t* meta, int64_t score);

static int zdel_one(ldb_context_t *context, const ldb_slice_t* name, const ldb_slice_t* key, const ldb_slice_t* meta);

static int zset_incr_size(ldb_context_t *context, const ldb_slice_t* name, int64_t by);

static void encode_zsize_key(const char* name, size_t namelen, ldb_slice_t** pslice){
  ldb_slice_t* slice = ldb_slice_create(LDB_DATA_TYPE_ZSIZE, strlen(LDB_DATA_TYPE_ZSIZE));
  ldb_slice_push_back(slice, name, namelen);
  *pslice = slice;
}

static int decode_zsize_key(const char* ldbkey, size_t ldbkeylen, ldb_slice_t** pslice){
  int retval = 0;
  ldb_slice_t *key_slice = NULL;
  ldb_bytes_t* bytes = ldb_bytes_create(ldbkey, ldbkeylen); 
  if(ldb_bytes_skip(bytes, strlen(LDB_DATA_TYPE_ZSIZE)) == -1){
    goto err;
  }
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
  ldb_slice_t *slice_name, *slice_key = NULL;
  ldb_bytes_t *bytes = ldb_bytes_create(ldbkey, ldbkeylen);
  if(ldb_bytes_skip(bytes, strlen(LDB_DATA_TYPE_ZSET)) == -1){
    goto err;
  }
  if(ldb_bytes_read_slice_size_uint8(bytes, &slice_name) == -1){
    goto err;
  }
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

static void encode_zscore_key(const char* name, size_t namelen, const char* key, size_t keylen, int64_t score, ldb_slice_t** pslice){
  uint8_t len = 0;
  ldb_slice_t *slice = ldb_slice_create(LDB_DATA_TYPE_ZSCORE, strlen(LDB_DATA_TYPE_ZSCORE));
  len = (uint8_t)namelen;
  ldb_slice_push_back(slice, (const char*)(&len), sizeof(uint8_t));
  ldb_slice_push_back(slice, name, namelen);
  if(score < 0){
    ldb_slice_push_back(slice, "-", 1);
  }else{
    ldb_slice_push_back(slice, "=", 1);
  }
  char buf[sizeof(int64_t)] = {0};
  leveldb_encode_fixed64(buf, score);
  ldb_slice_push_back(slice, buf, sizeof(int64_t));
  ldb_slice_push_back(slice, "=", 1);
  ldb_slice_push_back(slice, key, keylen); 
}

static int decode_zscore_key(const char* ldbkey, size_t ldbkeylen, ldb_slice_t** pslice_name, ldb_slice_t** pslice_key,  int64_t *pscore){
  int retval = 0;
  ldb_slice_t *slice_name, *slice_key=NULL;
  ldb_bytes_t *bytes = ldb_bytes_create(ldbkey, ldbkeylen);
  if(ldb_bytes_skip(bytes, strlen(LDB_DATA_TYPE_ZSCORE) == -1)){
    goto err;
  }
  if(ldb_bytes_read_slice_size_uint8(bytes, &slice_name) == -1){
    goto err;
  }
  if(ldb_bytes_skip(bytes, 1) == -1){
    goto err;
  }
  int64_t s = 0;
  if(ldb_bytes_read_int64(bytes, &s) == -1){
    goto err;
  }else{
    *pscore = s;
  }
  if(ldb_bytes_skip(bytes, 1) == -1){
    goto err;
  }
  if(ldb_bytes_read_slice_size_left(bytes, &slice_key) == -1){
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

int zset_get(ldb_context_t* context, const ldb_slice_t* name, const ldb_slice_t* key, int64_t* score){ 
  char *val, *errptr = NULL;
  size_t vallen = 0;
  leveldb_readoptions_t* readoptions = leveldb_readoptions_create();
  ldb_slice_t *slice_key = NULL;
  encode_zset_key(ldb_slice_data(name), ldb_slice_size(name), ldb_slice_data(key), ldb_slice_size(key), &slice_key); 
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
    assert(vallen >= mat_size);
    if(vallen < sizeof(int64_t)+mat_size){
      fprintf(stderr, "score with size=%ld, but vall=%ld\n", sizeof(int64_t), vallen-mat_size);
      retval = LDB_ERR;
    }else{
      uint8_t type = leveldb_decode_fixed8(val);
      if(type == LDB_VALUE_TYPE_VAL){
        *score = leveldb_decode_fixed64(val);
        retval = LDB_OK;
      }else{
        retval = LDB_OK_NOT_EXIST;
      }
    }
    leveldb_free(val);
    goto end;
  }else{
    retval = LDB_OK_NOT_EXIST;
  }
end:
  return retval;
}



//TODO meta slice
static int zset_one(ldb_context_t *context, const ldb_slice_t* name, const ldb_slice_t* key, const ldb_slice_t* meta, int64_t score){
  if(ldb_slice_size(name)==0 || ldb_slice_size(key)==0){
    fprintf(stderr, "empty name or key!");
    return 0;
  }
  if(ldb_slice_size(name) > LDB_KEY_LEN_MAX){
    fprintf(stderr, "name too long!");
    return -1;
  }
  if(ldb_slice_size(key) > LDB_KEY_LEN_MAX){
    fprintf(stderr, "key too long!");
    return -1;
  } 
  int64_t old_score = 0;
  int found = zset_get(context, name, key, &old_score);
  if(found == LDB_OK_NOT_EXIST || old_score != score){
    ldb_slice_t *slice_key0, *slice_key1, *slice_key2 = NULL;

    if(found != LDB_OK_NOT_EXIST){ 
      //delete zscore key
      encode_zscore_key(ldb_slice_data(name), 
                        ldb_slice_size(name), 
                        ldb_slice_data(key), 
                        ldb_slice_size(key),
                        old_score,
                        &slice_key1);

      ldb_context_writebatch_delete(context,
                                    ldb_slice_data(slice_key1),
                                    ldb_slice_size(slice_key1));
    }
    //add zscore key
    encode_zscore_key(ldb_slice_data(name),
                      ldb_slice_size(name),
                      ldb_slice_data(key),
                      ldb_slice_size(key),
                      score,
                      &slice_key2);

    //add zscore key
    ldb_context_writebatch_put(context,
                               ldb_slice_data(slice_key2),
                               ldb_slice_size(slice_key2),
                               NULL,
                               0);
    //update zset
    encode_zset_key(ldb_slice_data(name),
                    ldb_slice_size(name),
                    ldb_slice_data(key),
                    ldb_slice_size(key),
                    &slice_key0);

    char buf[sizeof(int64_t)] = {0};
    leveldb_encode_fixed64(buf, score);
    ldb_context_writebatch_put(context,
                               ldb_slice_data(slice_key0),
                               ldb_slice_size(slice_key0),
                               buf,
                               sizeof(int64_t));
    return found ? 0 : 1;
  }
  return 0; 
}



static int zdel_one(ldb_context_t *context, const ldb_slice_t* name, const ldb_slice_t* key, const ldb_slice_t* meta){
  if(ldb_slice_size(name) > LDB_KEY_LEN_MAX){
    fprintf(stderr, "name too long!");
    return -1;
  }
  if(ldb_slice_size(key) > LDB_KEY_LEN_MAX){
    fprintf(stderr, "key too long!");
    return -1;
  }
  int64_t old_score = 0;
  int found = zset_get(context, name, key, &old_score);
  if(found != LDB_OK){
    return 0;
  }
  ldb_slice_t *slice_key0, *slice_key1 = NULL;
  encode_zscore_key(ldb_slice_data(name),
                    ldb_slice_size(name),
                    ldb_slice_data(key),
                    ldb_slice_size(key),
                    old_score,
                    &slice_key1);

  ldb_context_writebatch_delete(context,
                                ldb_slice_data(slice_key1),
                                ldb_slice_size(slice_key1));
  
  encode_zset_key(ldb_slice_data(name),
                  ldb_slice_size(name),
                  ldb_slice_data(key),
                  ldb_slice_size(key),
                  &slice_key0);

  ldb_context_writebatch_delete(context,
                                ldb_slice_data(slice_key0),
                                ldb_slice_size(slice_key0)); 
  return 1;
}

static int zset_incr_size(ldb_context_t *context, const ldb_slice_t* name, int64_t by){
  int64_t size = zset_size(context, name);
  size += by;
  ldb_slice_t *slice_key = NULL;

  encode_zsize_key(ldb_slice_data(name),
                   ldb_slice_size(name),
                   &slice_key);
  if(size == 0){
    ldb_context_writebatch_delete(context,
                                  ldb_slice_data(slice_key),
                                  ldb_slice_size(slice_key));
  }else{
    char buf[sizeof(int64_t)] = {0};
    leveldb_encode_fixed64(buf, size);
    ldb_context_writebatch_put(context,
                               ldb_slice_data(slice_key),
                               ldb_slice_size(slice_key),
                               buf,
                               sizeof(int64_t));
  } 
  return 0;
}
