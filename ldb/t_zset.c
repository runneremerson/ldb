#include "t_zset.h"
#include "ldb_define.h"
#include "ldb_slice.h"
#include "ldb_bytes.h"
#include "ldb_meta.h"
#include "ldb_list.h"
#include "ldb_iterator.h"
#include "ldb_context.h"
#include "util.h"

#include <leveldb/c.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <stdint.h>
#include <assert.h>

int64_t LDB_SCORE_MIN = INT64_MIN;
int64_t LDB_SCORE_MAX = INT64_MAX;

static int zset_one(ldb_context_t *context, const ldb_slice_t* name, 
                    const ldb_slice_t* key, const ldb_meta_t* meta, int64_t score);

static int zdel_one(ldb_context_t *context, const ldb_slice_t* name, 
                    const ldb_slice_t* key, const ldb_meta_t* meta);

static int zset_incr_size(ldb_context_t *context, 
                          const ldb_slice_t* name, int64_t by);

static ldb_zset_iterator_t* ziterator(ldb_context_t *context, const ldb_slice_t *name,
                                      const ldb_slice_t *kstart, int64_t sstart, int64_t send, uint64_t limit,int direction);

static int zrange(ldb_context_t* context, const ldb_slice_t* name,
        uint64_t offset, uint64_t limit, int reverse, ldb_zset_iterator_t **piterator);

static int zscan(ldb_context_t* context, const ldb_slice_t* name, 
        const ldb_slice_t* key, int64_t start, int64_t end, int reverse, ldb_zset_iterator_t **piterator); 


void encode_zsize_key(const char* name, size_t namelen, const ldb_meta_t* meta, ldb_slice_t** pslice){
  ldb_slice_t* slice = ldb_meta_slice_create(meta);
  ldb_slice_push_back(slice, LDB_DATA_TYPE_ZSIZE, strlen(LDB_DATA_TYPE_ZSIZE));
  ldb_slice_push_back(slice, name, namelen);
  *pslice = slice;
}

int decode_zsize_key(const char* ldbkey, size_t ldbkeylen, ldb_slice_t** pslice){
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

void encode_zset_key(const char* name, size_t namelen, const char* key, size_t keylen, const ldb_meta_t* meta, ldb_slice_t** pslice){
  uint8_t len = 0;
  ldb_slice_t* slice = ldb_meta_slice_create(meta);
  ldb_slice_push_back(slice, LDB_DATA_TYPE_ZSET, strlen(LDB_DATA_TYPE_ZSET));
  len = (uint8_t)namelen;
  ldb_slice_push_back(slice, (const char*)(&len), sizeof(uint8_t));
  ldb_slice_push_back(slice, name, namelen);
  len = (uint8_t)keylen;
  ldb_slice_push_back(slice, (const char*)(&len), sizeof(uint8_t));
  ldb_slice_push_back(slice, key, keylen);
  *pslice = slice;
}

int decode_zset_key(const char* ldbkey, size_t ldbkeylen, ldb_slice_t** pslice_name, ldb_slice_t** pslice_key){
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


void encode_zscore_key(const char* name, size_t namelen, const char* key, size_t keylen, const ldb_meta_t* meta, int64_t score, ldb_slice_t** pslice){
  uint8_t len = 0;
  ldb_slice_t *slice = ldb_meta_slice_create(meta);
  ldb_slice_push_back(slice, LDB_DATA_TYPE_ZSCORE, strlen(LDB_DATA_TYPE_ZSCORE));
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
  *pslice = slice;
}

int decode_zscore_key(const char* ldbkey, size_t ldbkeylen, ldb_slice_t** pslice_name, ldb_slice_t** pslice_key,  int64_t *pscore){
  int retval = 0;
  ldb_slice_t *slice_name = NULL;
  ldb_slice_t *slice_key=NULL;
  ldb_bytes_t *bytes = ldb_bytes_create(ldbkey, ldbkeylen);
  if(ldb_bytes_skip(bytes, strlen(LDB_DATA_TYPE_ZSCORE))== -1){
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


int zset_add(ldb_context_t* context, const ldb_slice_t* name, 
             const ldb_slice_t* key, const ldb_meta_t* meta, int64_t score){
  int ret = zset_one(context, name, key, meta, score);
  int retval = LDB_OK;
  if(ret >= 0){
    if(ret > 0){
      if(zset_incr_size(context, name, ret) == -1){
        retval = LDB_ERR;
        goto end;
      }
    }
    char* errptr = NULL;
    ldb_context_writebatch_commit(context, &errptr);
    if( errptr != NULL){
      fprintf(stderr, "leveldb write fail %s.\n", errptr);
      leveldb_free(errptr);
      retval = LDB_ERR;
      goto end; 
    }
    retval = LDB_OK;
  }else{
    retval = LDB_ERR;
  }

end:
  return retval;
}

int zset_del(ldb_context_t* context, const ldb_slice_t* name, 
             const ldb_slice_t* key, const ldb_meta_t* meta){
  int ret = zdel_one(context, name, key, meta);
  int retval = LDB_OK; 
  if(ret >= 0){
    if(ret > 0){
      if(zset_incr_size(context, name, -ret) == -1){
        retval = LDB_ERR;
        goto end;
      }
    }
    char* errptr = NULL;
    ldb_context_writebatch_commit(context, &errptr);
    if( errptr != NULL ){
      fprintf(stderr, "write writebatch fail %s.\n", errptr);
      leveldb_free(errptr);
      retval = LDB_ERR;
      goto end; 
    }
    retval = LDB_OK;
  }else{
    retval = LDB_ERR;
  }
end:
  return retval;
}


int zset_del_range_by_rank(ldb_context_t* context, const ldb_slice_t* name,
                           const ldb_meta_t* meta, int rank_start, int rank_end, uint64_t *deleted){
  int retval = 0;
  ldb_zset_iterator_t *iterator = NULL;
  uint64_t offset, limit, size = 0;
  retval = zset_size(context, name, &size);
  if(retval != LDB_OK && retval != LDB_OK_NOT_EXIST){
    goto end;
  }
  if(rank_start < 0){
    rank_start = rank_start + size; 
  }
  if(rank_end < 0 ){
    rank_end = rank_end + size;
  }
  if(rank_start < 0){
    rank_start = 0;
  }
  if(rank_start > rank_end || rank_start >= (int)size){
    retval = LDB_OK_RANGE_HAVE_NONE;
    goto end;
  }
  if(rank_end >= (int)size){
    rank_end = (int)(size - 1);
  }
  offset = rank_start;
  limit = (rank_end - rank_start) + 1;
  if(zrange(context, name, offset, limit, 0, &iterator)<0){
    retval = LDB_OK_RANGE_HAVE_NONE;
    goto end;
  }
  (*deleted) = 0;
  do{
    ldb_slice_t *slice_key, *slice_name, *key = NULL;
    uint64_t value = 0;
    ldb_zset_iterator_key(iterator, &slice_key);
    if(decode_zscore_key( ldb_slice_data(slice_key),
                       ldb_slice_size(slice_key),
                       &slice_name,
                       &key,
                       &value)== 0){ 
      if(zset_del(context, name, key, meta) == LDB_OK){
        *deleted += 1;
      }
      ldb_slice_destroy(key);
    }
    ldb_slice_destroy(slice_key);
  }while(!ldb_zset_iterator_next(iterator));

  retval = LDB_OK;

end:
  ldb_zset_iterator_destroy(iterator);
  return retval; 
}


int zset_del_range_by_score(ldb_context_t* context, const ldb_slice_t* name,
                            const ldb_meta_t* meta, int64_t score_start, int64_t score_end, uint64_t *deleted){
  int retval = 0;
  ldb_zset_iterator_t *iterator = NULL;
  if(zscan(context, name, NULL, score_start, score_end, 0, &iterator) < 0){
    retval = LDB_OK_RANGE_HAVE_NONE;
    goto end;
  }

  (*deleted) = 0;
  do{
    ldb_slice_t *slice_key, *slice_name, *key = NULL;
    uint64_t value = 0;
    ldb_zset_iterator_key(iterator, &slice_key);
    if(decode_zscore_key(ldb_slice_data(slice_key),
                         ldb_slice_size(slice_key),
                         &slice_name,
                         &key,
                         &value) == 0){ 
      if(zset_del(context, name, key, meta) == LDB_OK){
        *deleted += 1;
      }
      ldb_slice_destroy(key);
    }
    ldb_slice_destroy(slice_key);
    ldb_slice_destroy(slice_name);
  }while(!ldb_zset_iterator_next(iterator));

  ldb_zset_iterator_destroy(iterator);
  retval = LDB_OK;

end:
  return retval;
}

int zset_get(ldb_context_t* context, const ldb_slice_t* name, 
             const ldb_slice_t* key, int64_t* score){ 
  char *val, *errptr = NULL;
  size_t vallen = 0;
  leveldb_readoptions_t* readoptions = leveldb_readoptions_create();
  ldb_slice_t *slice_key = NULL;
  encode_zset_key(ldb_slice_data(name), ldb_slice_size(name), ldb_slice_data(key), ldb_slice_size(key), NULL, &slice_key); 
  val = leveldb_get(context->database_, readoptions, ldb_slice_data(slice_key), ldb_slice_size(slice_key), &vallen, &errptr);
  leveldb_readoptions_destroy(readoptions);
  ldb_slice_destroy(slice_key);
  int retval = 0;
  if(errptr != NULL){
    fprintf(stderr, "leveldb_get fail %s.\n", errptr);
    leveldb_free(errptr);
    retval = LDB_ERR;
    goto end;
  }
  if(val != NULL){
    assert(vallen >= sizeof(int64_t) + LDB_VAL_META_SIZE);
    uint8_t type = leveldb_decode_fixed8(val);
    if(type & LDB_VALUE_TYPE_VAL){
        if(type & LDB_VALUE_TYPE_LAT){
          retval = LDB_OK_NOT_EXIST;
          goto end;
        }
        *score = leveldb_decode_fixed64(val + LDB_VAL_META_SIZE);
        retval = LDB_OK;
      }else{
        retval = LDB_OK_NOT_EXIST;
      }
  }else{
    retval = LDB_OK_NOT_EXIST;
  }

end:
  if(val != NULL){
    leveldb_free(val); 
  }
  return retval;
}


int zset_rank(ldb_context_t* context, const ldb_slice_t* name, 
              const ldb_slice_t* key, int reverse, int64_t* rank){
  int retval = 0;
  ldb_zset_iterator_t *iterator = NULL; 
  if(reverse == 0){
    iterator = ziterator(context, name, NULL, LDB_SCORE_MIN, LDB_SCORE_MAX, INT32_MAX, FORWARD); 
  }else{
    iterator = ziterator(context, name, NULL, LDB_SCORE_MAX, LDB_SCORE_MIN, INT32_MAX, BACKWARD); 
  }

  uint64_t tmp = 0;
  ldb_slice_t *slice_key = NULL;
  ldb_slice_t *slice_name = NULL;
  while(1){
    slice_name = NULL;
    slice_key = NULL;
    int64_t score = 0;
    size_t raw_klen = 0;
    const char* raw_key = ldb_zset_iterator_key_raw(iterator, &raw_klen);
    if(decode_zscore_key(raw_key, raw_klen, &slice_name, &slice_key, &score) < 0){
        if(tmp == 0){
            retval = LDB_OK_NOT_EXIST;
            goto end;
        }
        break;
    }

    if(compare_with_length(ldb_slice_data(key),
                           ldb_slice_size(key),
                           ldb_slice_data(slice_key),
                           ldb_slice_size(slice_key))==0){
      
      retval = LDB_OK;
      break;
    }
    ldb_slice_destroy(slice_name);
    ldb_slice_destroy(slice_key);

    if(ldb_zset_iterator_next(iterator)!=0){
      retval = LDB_OK_NOT_EXIST;
      goto end; 
    }
    ++tmp;
  }
  *rank = tmp;

end:
  ldb_slice_destroy(slice_name);
  ldb_slice_destroy(slice_key);
  ldb_zset_iterator_destroy(iterator);
  return retval;
}

int zset_count(ldb_context_t* context, const ldb_slice_t* name,
        int64_t score_start, int64_t score_end, uint64_t *count){
  int retval = 0;
  ldb_zset_iterator_t *iterator = NULL;
  if(zscan(context, name, NULL, score_start, score_end, 0, &iterator) < 0){
    retval = LDB_OK_RANGE_HAVE_NONE;
    goto end;
  }
  *count = 0;
  while(!ldb_zset_iterator_next(iterator)){
    (*count) += 1;
  }
  retval = LDB_OK;

end:
  ldb_zset_iterator_destroy(iterator);
  return retval;
}

int zset_range(ldb_context_t* context, const ldb_slice_t* name, 
               int rank_start, int rank_end, int reverse, ldb_list_t **pkeylist, ldb_list_t** pmetalist){
  int retval = 0;
  uint64_t offset, limit, size = 0;
  retval = zset_size(context, name, &size);
  if(retval != LDB_OK && retval != LDB_OK_NOT_EXIST){
    goto end;
  }
  if(rank_start < 0){
    rank_start = rank_start + size; 
  }
  if(rank_end < 0 ){
    rank_end = rank_end + size;
  }
  if(rank_start < 0){
    rank_start = 0;
  }
  if(rank_start > rank_end || rank_start >= (int)size){
    retval = LDB_OK_RANGE_HAVE_NONE;
    goto end;
  }
  if(rank_end >= (int)size){
    rank_end = (int)(size - 1);
  }
  offset = rank_start;
  limit = (rank_end - rank_start) + 1;
  ldb_zset_iterator_t *iterator = NULL;
  if(zrange(context, name, offset, limit, reverse, &iterator)<0){
    retval = LDB_OK_RANGE_HAVE_NONE;
    goto end;
  }
  ldb_list_t *keylist = ldb_list_create();
  ldb_list_t *metlist = ldb_list_create();
  do{
    ldb_slice_t *slice_key, *slice_val, *slice_name, *slice_node = NULL;
    int64_t value = 0;
    ldb_zset_iterator_key(iterator, &slice_key);
    if(decode_zscore_key( ldb_slice_data(slice_key),
                       ldb_slice_size(slice_key),
                       &slice_name,
                       &slice_node,
                       &value)==0){ 
      //first node
      ldb_list_node_t* key_node = ldb_list_node_create();
      key_node->type_ = LDB_LIST_NODE_TYPE_SLICE;
      key_node->value_ = value;
      key_node->data_ = slice_node;
      rpush_ldb_list_node(keylist, key_node);
      //second
      ldb_zset_iterator_val(iterator, &slice_val);
      uint64_t version = leveldb_decode_fixed64(ldb_slice_data(slice_val));
      ldb_list_node_t* met_node = ldb_list_node_create();
      met_node->type_ = LDB_LIST_NODE_TYPE_BASE;
      met_node->value_ = version;
      met_node->data_ = NULL;
      rpush_ldb_list_node(metlist, met_node);
    }
    ldb_slice_destroy(slice_key);
    ldb_slice_destroy(slice_val);
  }while(!ldb_zset_iterator_next(iterator));
  *pkeylist = keylist;
  *pmetalist = metlist;
  ldb_zset_iterator_destroy(iterator);
  retval = LDB_OK;
 
end:
  return retval;
}


int zset_scan(ldb_context_t* context, const ldb_slice_t* name,
              int64_t score_start, int64_t score_end, int reverse, ldb_list_t **pkeylist, ldb_list_t **pmetalist){
  int retval = 0;
  ldb_zset_iterator_t *iterator = NULL;
  if(zscan(context, name, NULL, score_start, score_end, reverse, &iterator) < 0){
    retval = LDB_OK_RANGE_HAVE_NONE;
    goto end;
  }

  ldb_list_t *keylist = ldb_list_create();
  ldb_list_t *metlist = ldb_list_create();
  do{
    ldb_slice_t *slice_key, *slice_val, *slice_name, *slice_node = NULL;
    int64_t value = 0;
    ldb_zset_iterator_key(iterator, &slice_key);
    if(decode_zscore_key( ldb_slice_data(slice_key),
                       ldb_slice_size(slice_key),
                       &slice_name,
                       &slice_node,
                       &value) == 0){
      //first
      ldb_list_node_t* key_node = ldb_list_node_create();
      key_node->type_ = LDB_LIST_NODE_TYPE_SLICE;
      key_node->value_ = value;
      key_node->data_ = slice_node;
      rpush_ldb_list_node(keylist, key_node);
      //second
      ldb_zset_iterator_val(iterator, &slice_val);
      uint64_t version = leveldb_decode_fixed64(ldb_slice_data(slice_val));
      ldb_list_node_t* met_node = ldb_list_node_create();
      met_node->type_ = LDB_LIST_NODE_TYPE_BASE;
      met_node->value_ = version;
      met_node->data_ = NULL;
      rpush_ldb_list_node(metlist, met_node);
    }
    ldb_slice_destroy(slice_key);
    ldb_slice_destroy(slice_val);
  }while(!ldb_zset_iterator_next(iterator));

  *pkeylist = keylist;
  *pmetalist = metlist;
  ldb_zset_iterator_destroy(iterator);
  retval = LDB_OK;

end:
  return retval;
}


int zset_incr(ldb_context_t* context, const ldb_slice_t* name, 
              const ldb_slice_t* key, const ldb_meta_t* meta, int64_t by, int64_t* val){
  int64_t old_score = 0;
  int ret = zset_get(context, name, key, &old_score);
  int retval = LDB_OK;
  if(ret == LDB_OK){
    *val = old_score + by;
  }else if(ret = LDB_OK_NOT_EXIST){
    *val = by;
  }else{
    retval = ret;
    goto end;
  }
  ret = zset_one(context, name, key, meta, *val);
  if(ret >= 0){
    if(ret > 0){
      if(zset_incr_size(context, name, ret) == -1){
        retval = LDB_ERR;
        goto end;
      }
    }
    char* errptr = NULL;
    ldb_context_writebatch_commit(context, &errptr);
    if(errptr!=NULL){
      fprintf(stderr, "leveldb_write fail %s.\n", errptr);
      leveldb_free(errptr);
      retval = LDB_ERR;
      goto end;
    }
    retval = LDB_OK;
  }else{
    retval = LDB_ERR;
  }
  
end:
  return retval;
}


int zset_size(ldb_context_t* context, const ldb_slice_t* name, 
              uint64_t* size){
  ldb_slice_t *slice_key = NULL;
  encode_zsize_key(ldb_slice_data(name),
                   ldb_slice_size(name),
                   NULL,
                   &slice_key);
 
  char *val, *errptr = NULL;
  size_t vallen = 0;
  leveldb_readoptions_t* readoptions = leveldb_readoptions_create();
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
    assert(vallen >= (sizeof(uint64_t) + LDB_VAL_META_SIZE));
    uint8_t type = leveldb_decode_fixed8(val);
    if(type & LDB_VALUE_TYPE_VAL){
      *size = leveldb_decode_fixed64(val + LDB_VAL_META_SIZE);
      retval = LDB_OK;
    }else{
      retval = LDB_OK_NOT_EXIST;
    }
  }else{
    retval = LDB_OK_NOT_EXIST;
  }
end:
  if (val !=NULL){
    leveldb_free(val);   
  }
  return retval;
}


static int zset_one(ldb_context_t *context, const ldb_slice_t* name, 
                    const ldb_slice_t* key, const ldb_meta_t* meta, int64_t score){
  if(ldb_slice_size(name)==0 || ldb_slice_size(key)==0){
    fprintf(stderr, "empty name or key!");
    return 0;
  }
  if(ldb_slice_size(name) > LDB_DATA_TYPE_KEY_LEN_MAX){
    fprintf(stderr, "name too long!");
    return -1;
  }
  if(ldb_slice_size(key) > LDB_DATA_TYPE_KEY_LEN_MAX){
    fprintf(stderr, "key too long!");
    return -1;
  } 
  int64_t old_score = 0;
  int found = zset_get(context, name, key, &old_score);
  if(found == LDB_OK_NOT_EXIST || old_score != score){
    if(found != LDB_OK_NOT_EXIST){ 

      ldb_slice_t *slice_key1 = NULL;
      //delete zscore key
      encode_zscore_key(ldb_slice_data(name), 
                        ldb_slice_size(name), 
                        ldb_slice_data(key), 
                        ldb_slice_size(key),
                        meta,
                        old_score,
                        &slice_key1);

      ldb_context_writebatch_delete(context,
                                    ldb_slice_data(slice_key1),
                                    ldb_slice_size(slice_key1));
      ldb_slice_destroy(slice_key1);
    }
    ldb_slice_t *slice_key2 = NULL;
    //add zscore key
    encode_zscore_key(ldb_slice_data(name),
                      ldb_slice_size(name),
                      ldb_slice_data(key),
                      ldb_slice_size(key),
                      meta,
                      score,
                      &slice_key2);
    //add zscore key
    char buf0[sizeof(uint64_t)] = {0};
    leveldb_encode_fixed64(buf0, ldb_meta_nextver(meta));
    ldb_context_writebatch_put(context,
                               ldb_slice_data(slice_key2),
                               ldb_slice_size(slice_key2),
                               buf0,
                               sizeof(buf0));
    ldb_slice_destroy(slice_key2);

    ldb_slice_t *slice_key0 = NULL;
    //update zset
    encode_zset_key(ldb_slice_data(name),
                    ldb_slice_size(name),
                    ldb_slice_data(key),
                    ldb_slice_size(key),
                    meta,
                    &slice_key0);

    char buf1[sizeof(int64_t)] = {0};
    leveldb_encode_fixed64(buf1, score);
    ldb_context_writebatch_put(context,
                               ldb_slice_data(slice_key0),
                               ldb_slice_size(slice_key0),
                               buf1,
                               sizeof(int64_t));

   ldb_slice_destroy(slice_key0);

    return (found==LDB_OK) ? 0 : 1;
  }
  return 0; 
}



static int zdel_one(ldb_context_t *context, const ldb_slice_t* name, 
                    const ldb_slice_t* key, const ldb_meta_t* meta){
  if(ldb_slice_size(name) > LDB_DATA_TYPE_KEY_LEN_MAX){
    fprintf(stderr, "name too long!");
    return -1;
  }
  if(ldb_slice_size(key) > LDB_DATA_TYPE_KEY_LEN_MAX){
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
                    NULL,
                    old_score,
                    &slice_key1);

  ldb_context_writebatch_delete(context,
                                ldb_slice_data(slice_key1),
                                ldb_slice_size(slice_key1));
  
  encode_zset_key(ldb_slice_data(name),
                  ldb_slice_size(name),
                  ldb_slice_data(key),
                  ldb_slice_size(key),
                  meta,
                  &slice_key0);

  ldb_context_writebatch_delete(context,
                                ldb_slice_data(slice_key0),
                                ldb_slice_size(slice_key0)); 
  return 1;
}

static int zset_incr_size(ldb_context_t *context, 
                          const ldb_slice_t* name, int64_t by){
  uint64_t length = 0;
  zset_size(context, name, &length);
  int64_t size = (int64_t)length;
  size += by;
  ldb_slice_t *slice_key = NULL;

  encode_zsize_key(ldb_slice_data(name),
                   ldb_slice_size(name),
                   NULL,
                   &slice_key);
  if(size <= 0){
    ldb_context_writebatch_delete(context,
                                  ldb_slice_data(slice_key),
                                  ldb_slice_size(slice_key));
  }else{
    char buff[sizeof(uint64_t)] = {0};
    length = size;
    leveldb_encode_fixed64(buff, length);
    ldb_context_writebatch_put(context,
                               ldb_slice_data(slice_key),
                               ldb_slice_size(slice_key),
                               buff,
                               sizeof(uint64_t));
  } 
  ldb_slice_destroy(slice_key);
  return 0;
}


static ldb_zset_iterator_t* ziterator(ldb_context_t *context, const ldb_slice_t *name,
                                      const ldb_slice_t *kstart, int64_t sstart, int64_t send, uint64_t limit,int direction){
    ldb_slice_t *key_end = NULL;
    ldb_slice_t *key_start = NULL;
   if(direction == FORWARD){
       encode_zscore_key(ldb_slice_data(name),
                         ldb_slice_size(name),
                         ldb_slice_data(kstart),
                         ldb_slice_size(kstart),
                         NULL,
                         sstart,
                         &key_start);

       encode_zscore_key(ldb_slice_data(name),
                         ldb_slice_size(name),
                         "\xff",
                         strlen("\xff"),
                         NULL,
                         send,
                         &key_end);
   }else{
       if(ldb_slice_size(kstart)==0){
           encode_zscore_key(ldb_slice_data(name),
                             ldb_slice_size(name),
                             "\xff",
                             strlen("\xff"),
                             NULL,
                             sstart,
                             &key_start);
       }else{
           encode_zscore_key(ldb_slice_data(name),
                             ldb_slice_size(name),
                             ldb_slice_data(kstart),
                             ldb_slice_size(kstart),
                             NULL,
                             sstart,
                             &key_start);
       }
       encode_zscore_key(ldb_slice_data(name),
                         ldb_slice_size(name),
                         NULL,
                         0,
                         NULL,
                         send,
                         &key_end);
    }

    ldb_zset_iterator_t* iterator = ldb_zset_iterator_create(context, name,  key_start, key_end, limit, direction);

end:
    ldb_slice_destroy(key_start);
    ldb_slice_destroy(key_end);
    return iterator;
}

static int zrange(ldb_context_t* context, const ldb_slice_t* name,
        uint64_t offset, uint64_t limit, int reverse, ldb_zset_iterator_t **piterator){
  uint64_t start, end = 0;
  start = LDB_SCORE_MIN;
  if(offset < LDB_SCORE_MAX-limit){
    end = offset + limit;
  }else{
    end = LDB_SCORE_MAX;
  }
  if(reverse == 0){
    *piterator = ziterator(context, name, NULL, start, end, limit, FORWARD);  
  }else{
    *piterator = ziterator(context, name, NULL, end, start, limit, BACKWARD);
  }
  int retval = 0;
  if(ldb_zset_iterator_skip(*piterator, offset)<0){
    retval = -1;
    goto end;
  }
  retval = 0;

end:
  return retval;
}

static int zscan(ldb_context_t* context, const ldb_slice_t* name, 
        const ldb_slice_t* key, int64_t start, int64_t end, int reverse, ldb_zset_iterator_t **piterator){
  int64_t score = 0;
  if(ldb_slice_size(key)==0 || zset_get(context, name, key, &score) != LDB_OK ){
    score= start;
  }
  if(reverse == 0){
    *piterator = ziterator(context, name, key, score, end, INT32_MAX, FORWARD);
  }else{
    *piterator = ziterator(context, name, key, score, end, INT32_MAX, BACKWARD);
  }
  return 0; 
}
