#include "t_hash.h"
#include "ldb_define.h"
#include "ldb_slice.h"
#include "ldb_bytes.h"
#include "ldb_meta.h"
#include "ldb_list.h"
#include "ldb_iterator.h"
#include "ldb_context.h"
#include "util.h"


#include <leveldb-ldb/c.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <stdint.h>

static int hset_one(ldb_context_t* context, const ldb_slice_t* name,
                    const ldb_slice_t* key, const ldb_slice_t* value, const ldb_meta_t* meta); 

static int hdel_one(ldb_context_t* context, const ldb_slice_t* name,
                    const ldb_slice_t* key, const ldb_meta_t* meta);

static int hash_incr_size(ldb_context_t* context, const ldb_slice_t* name,
                    int64_t by);

static int hscan(ldb_context_t* context, const ldb_slice_t* name,
                 const ldb_slice_t* kstart, const ldb_slice_t* kend, uint64_t limit, int reverse, ldb_hash_iterator_t** piterator);



void encode_hsize_key(const char* name, size_t namelen, const ldb_meta_t* meta, ldb_slice_t** pslice){
  ldb_slice_t* slice = ldb_meta_slice_create(meta);
  ldb_slice_push_back(slice, LDB_DATA_TYPE_HSIZE, strlen(LDB_DATA_TYPE_HSIZE));
  ldb_slice_push_back(slice, name, namelen);
  *pslice = slice;
}


int decode_hsize_key(const char* ldbkey, size_t ldbkeylen, ldb_slice_t** pslice){
  int retval = 0;
  ldb_slice_t* key_slice = NULL;
  ldb_bytes_t* bytes = ldb_bytes_create(ldbkey, ldbkeylen);
  if(ldb_bytes_skip(bytes, strlen(LDB_DATA_TYPE_HSIZE))==-1){
    goto err;
  }
  if(ldb_bytes_read_slice_size_left(bytes, &key_slice)==-1){
    goto err;
  }
  goto nor;

nor:
  *pslice = key_slice;
  retval = 0;

err:
  retval = -1;
  ldb_slice_destroy(key_slice);
  goto end;

end:
  ldb_bytes_destroy(bytes);
  return retval;
}

void encode_hash_key(const char* name, size_t namelen, const char* key, size_t keylen, const ldb_meta_t* meta, ldb_slice_t** pslice){
  uint8_t len = 0;
  ldb_slice_t *slice = ldb_meta_slice_create(meta);
  ldb_slice_push_back(slice, LDB_DATA_TYPE_HASH, strlen(LDB_DATA_TYPE_HASH));
  len = (uint8_t)namelen;
  ldb_slice_push_back(slice, (const char*)(&len), sizeof(uint8_t)); 
  ldb_slice_push_back(slice, name, namelen);
  ldb_slice_push_back(slice, "=", 1);
  ldb_slice_push_back(slice, key, keylen);
}

int decode_hash_key(const char* ldbkey, size_t ldbkeylen, ldb_slice_t **pslice_name, ldb_slice_t **pslice_key){
  int retval = 0;
  ldb_slice_t *slice_name, *slice_key = NULL;
  ldb_bytes_t *bytes = ldb_bytes_create(ldbkey, ldbkeylen);
  if(ldb_bytes_skip(bytes, strlen(LDB_DATA_TYPE_HASH))==-1){
    goto err;
  }
  if(ldb_bytes_read_slice_size_uint8(bytes, &slice_name)==-1){
    goto err;
  }
  if(ldb_bytes_skip(bytes, 1)==-1){
    goto err;
  }
  if(ldb_bytes_read_slice_size_left(bytes, &slice_key)==-1){
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

int hash_get(ldb_context_t* context, const ldb_slice_t* name, const ldb_slice_t* key, ldb_slice_t** pslice){
  int retval = 0;
  char *val, *errptr = NULL;
  size_t vallen = 0;
  leveldb_readoptions_t *readoptions = leveldb_readoptions_create();
  ldb_slice_t* slice_key = NULL;
  encode_hash_key(ldb_slice_data(name), ldb_slice_size(name), ldb_slice_data(key), ldb_slice_size(key), NULL, &slice_key);
  val = leveldb_get(context->database_, readoptions, ldb_slice_data(slice_key), ldb_slice_size(slice_key), &vallen, &errptr);
  leveldb_readoptions_destroy(readoptions);
  ldb_slice_destroy(slice_key);
  if(errptr!=NULL){
    fprintf(stderr, "leveldb_get fail %s.\n", errptr);
    leveldb_free(errptr);
    retval = LDB_ERR;
    goto end;
  }
  if(val!=NULL){
    if(vallen < LDB_VAL_META_SIZE){
      fprintf(stderr, "meta with size=%d, but vall=%ld\n", LDB_VAL_META_SIZE, vallen);  
      retval = LDB_ERR;
    }else{
      uint8_t type = leveldb_decode_fixed8(val);
      if(type == LDB_VALUE_TYPE_VAL){
        *pslice = ldb_slice_create(val + LDB_VAL_TYPE_SIZE, vallen - LDB_VAL_TYPE_SIZE);
        retval = LDB_OK;
      }else{
        retval = LDB_OK_NOT_EXIST;
      }
    }
    leveldb_free(val);
  }else{
    retval = LDB_OK_NOT_EXIST;
  }

end:
  return retval;
}

int hash_mget(ldb_context_t* context, const ldb_slice_t* name, const ldb_list_t* keylist, ldb_list_t** pvallist, ldb_list_t** pmetalist){
  int retval = 0;
  ldb_list_iterator_t *keyiterator = ldb_list_iterator_create(keylist);
  *pvallist = ldb_list_create();
  *pmetalist = ldb_list_create();
  while(1){
    ldb_list_node_t *node_key = ldb_list_next(&keyiterator);
    if(node_key == NULL){
      retval = LDB_OK;
      break;
    }
    ldb_slice_t *slice_val = NULL;
    ldb_list_node_t *node_val = ldb_list_node_create();
    ldb_list_node_t *node_meta = ldb_list_node_create();
    if(hash_get(context, name, (ldb_slice_t*)node_key->data_, &slice_val)== LDB_OK){
      node_val->data_ = ldb_slice_create(ldb_slice_data(slice_val)+sizeof(uint64_t), ldb_slice_size(slice_val)-sizeof(uint64_t));
      node_val->type_ = LDB_LIST_NODE_TYPE_SLICE;
      node_meta->value_ = leveldb_decode_fixed64(ldb_slice_data(slice_val));
      node_meta->type_ = LDB_LIST_NODE_TYPE_BASE;
    }else{
      node_val->type_ = LDB_LIST_NODE_TYPE_NONE;
      node_meta->type_ = LDB_LIST_NODE_TYPE_NONE;
    }
    rpush_ldb_list_node(*pvallist, node_val);
    rpush_ldb_list_node(*pmetalist, node_meta);
  }

end:
  ldb_list_iterator_destroy(keyiterator);
  return retval;
}

int hash_getall(ldb_context_t* context, const ldb_slice_t* name, ldb_list_t** pkeylist, ldb_list_t** pvallist, ldb_list_t** pmetalist){
  int retval = 0;
  ldb_hash_iterator_t* iterator = NULL;
  if(hscan(context, name, NULL, NULL, 20000000, 0, &iterator)!=0){
    retval = LDB_OK_RANGE_HAVE_NONE;
    goto end;
  }
  do{
    ldb_slice_t *slice_key, *key, *slice_val, *slice_name = NULL;
    ldb_hash_iterator_key(iterator, &slice_key);
    if(decode_hash_key(ldb_slice_data(slice_key),
                       ldb_slice_size(slice_key),
                       &slice_name,
                       &key) == 0){
    }

  }while(!ldb_hash_iterator_next(iterator));
  
  
  

end:
  ldb_hash_iterator_destroy(iterator);
  return retval;
}


int hash_exists(ldb_context_t* context, const ldb_slice_t* name, const ldb_slice_t* key){ 
  ldb_slice_t* slice_val = NULL;
  int retval = hash_get(context, name, key, &slice_val); 

  ldb_slice_destroy(slice_val);
  return retval;
}




int hash_length(ldb_context_t* context, const ldb_slice_t* name, uint64_t* length){
  int retval = 0;
  char *val, *errptr = NULL;
  size_t vallen = 0;
  leveldb_readoptions_t *readoptions = leveldb_readoptions_create();
  ldb_slice_t* slice_key = NULL;
  encode_hsize_key(ldb_slice_data(name), ldb_slice_size(name), NULL, &slice_key);
  val = leveldb_get(context->database_, readoptions, ldb_slice_data(slice_key), ldb_slice_size(slice_key), &vallen, &errptr);
  leveldb_readoptions_destroy(readoptions);
  ldb_slice_destroy(slice_key);
  if(errptr!=NULL){
    fprintf(stderr, "leveldb_get fail %s.\n", errptr);
    leveldb_free(errptr);
    retval = LDB_ERR;
    goto end;
  }
  if(val!=NULL){
    if(vallen < (sizeof(uint64_t) + LDB_VAL_META_SIZE)){
      fprintf(stderr, "meta with size=%d, but vall=%ld\n", LDB_VAL_META_SIZE, vallen);  
      retval = LDB_ERR;
    }else{
      uint8_t type = leveldb_decode_fixed8(val);
      if(type == LDB_VALUE_TYPE_VAL){
        *length = leveldb_decode_fixed64(val + LDB_VAL_META_SIZE);
        retval = LDB_OK;
      }else{
        retval = LDB_OK_NOT_EXIST;
      }
    }
    leveldb_free(val);
  }else{
    retval = LDB_OK_NOT_EXIST;
  }

end:
  return retval;
}

int hash_incr(ldb_context_t* context, const ldb_slice_t* name, const ldb_slice_t* key, const ldb_meta_t* meta, int64_t by, int64_t* val){
  leveldb_mutex_lock(context->mutex_);

  int retval = 0;
  ldb_slice_t *slice_old_val, *slice_new_val= NULL;
  int64_t old_val = 0;
  int ret = hash_get(context, name, key, &slice_old_val);
  if(ret == LDB_OK){
    old_val = leveldb_decode_fixed64(ldb_slice_data(slice_old_val) + sizeof(uint64_t));
    old_val += by;
  }else if(ret == LDB_OK_NOT_EXIST){
    old_val = by;
  }else{
    retval = ret;
    goto end;
  }
  char buff[sizeof(uint64_t)] = {0};
  leveldb_encode_fixed64(buff, old_val);
  slice_new_val = ldb_slice_create(buff, sizeof(buff));
  ret = hset_one(context, name, key, slice_new_val, meta);
  if(ret >=0){
    if(ret > 0){
      if(hash_incr_size(context, name, 1) < 0){
        retval = LDB_ERR;
        goto end;
      }
    }
    char *errptr = NULL;
    ldb_context_writebatch_commit(context, &errptr);
    if(errptr != NULL){
      fprintf(stderr, "leveldb_write fail %s.\n", errptr);
      leveldb_free(errptr);
      retval = LDB_ERR;
      goto  end;
    }
    retval = LDB_OK;

  }else{
    retval = LDB_ERR;
  }
 
end:
  leveldb_mutex_unlock(context->mutex_);
  ldb_slice_destroy(slice_old_val);
  ldb_slice_destroy(slice_new_val);
  return retval;
}



static int hset_one(ldb_context_t* context, const ldb_slice_t* name,
                    const ldb_slice_t* key, const ldb_slice_t* value, const ldb_meta_t* meta){
  if(ldb_slice_size(name)==0 || ldb_slice_size(key)==0){
    fprintf(stderr, "empty name or key!");
    return 0;
  }
  if(ldb_slice_size(name) > LDB_KEY_LEN_MAX){
    fprintf(stderr, "name too long!");
    return -1;
  }
  if(ldb_slice_size(key) > LDB_KEY_LEN_MAX){
    fprintf(stderr, "name too long!");
    return -1;
  }
  int retval = 0;
  ldb_slice_t *slice_key,  *slice_val = NULL;
  if(hash_get(context, name, key, &slice_val) == LDB_OK_NOT_EXIST){
    encode_hash_key(ldb_slice_data(name),
                    ldb_slice_size(name),
                    ldb_slice_data(key),
                    ldb_slice_size(key),
                    meta,
                    &slice_key);

    ldb_context_writebatch_put(context,
                               ldb_slice_data(slice_key),
                               ldb_slice_size(slice_key),
                               ldb_slice_data(value),
                               ldb_slice_size(value));
    return 1;
  }else{
      if(compare_with_length(ldb_slice_data(slice_val),
                             ldb_slice_size(slice_val),
                             ldb_slice_data(value),
                             ldb_slice_size(value)) != 0){
        encode_hash_key(ldb_slice_data(name),
                        ldb_slice_size(name),
                        ldb_slice_data(key),
                        ldb_slice_size(key),
                        meta,
                        &slice_key);

        ldb_context_writebatch_put(context,
                                   ldb_slice_data(slice_key),
                                   ldb_slice_size(slice_key),
                                   ldb_slice_data(value),
                                   ldb_slice_size(value));
       
      }
      return 0;
  }
  return retval;
}


static int hdel_one(ldb_context_t* context, const ldb_slice_t* name,
                    const ldb_slice_t* key, const ldb_meta_t* meta){

  if(ldb_slice_size(name) > LDB_KEY_LEN_MAX){
    fprintf(stderr, "name too long!");
    return -1;
  }
  if(ldb_slice_size(key) > LDB_KEY_LEN_MAX){
    fprintf(stderr, "name too long!");
    return -1;
  }

  ldb_slice_t *slice_val = NULL;
  if(hash_get(context, name, key, &slice_val) == LDB_OK_NOT_EXIST){
    return 0;
  }

  ldb_slice_t *slice_key = NULL;
  encode_hash_key(ldb_slice_data(name),
                  ldb_slice_size(name),
                  ldb_slice_data(key),
                  ldb_slice_size(key),
                  meta,
                  &slice_key);
  ldb_context_writebatch_delete(context,
                                ldb_slice_data(slice_key),
                                ldb_slice_size(slice_key));
  return 1;
}


static int hash_incr_size(ldb_context_t* context, const ldb_slice_t* name,
                    int64_t by){
  uint64_t length = 0;
  if(hash_length(context, name, &length)!=LDB_OK){
    return -1;
  }
  length += by;

  ldb_slice_t* slice_key = NULL;
  encode_hsize_key(ldb_slice_data(name),
                   ldb_slice_size(name),
                   NULL,
                   &slice_key);
  if(length == 0){
    ldb_context_writebatch_delete(context,
                                  ldb_slice_data(slice_key),
                                  ldb_slice_size(slice_key));
  }else{
    char buff[sizeof(uint64_t)] = {0};
    leveldb_encode_fixed64(buff, length);
    ldb_context_writebatch_put(context,
                               ldb_slice_data(slice_key),
                               ldb_slice_size(slice_key),
                               buff,
                               sizeof(buff));
  }
  return 0;
}


static int hscan(ldb_context_t* context, const ldb_slice_t* name,
                 const ldb_slice_t* kstart, const ldb_slice_t* kend, uint64_t limit, int reverse, ldb_hash_iterator_t** piterator){

  ldb_slice_t *slice_start, *slice_end = NULL;
  if(!reverse){

    encode_hash_key(ldb_slice_data(name),
                    ldb_slice_size(name),
                    ldb_slice_data(kstart),
                    ldb_slice_size(kstart),
                    NULL,
                    &slice_start);
    encode_hash_key(ldb_slice_data(name),
                    ldb_slice_size(name),
                    ldb_slice_data(kend),
                    ldb_slice_size(kend),
                    NULL,
                    &slice_end);
    if(ldb_slice_size(kend) == 0){
      ldb_slice_push_back(slice_end, "\xff", strlen("\xff"));
    }

    *piterator = ldb_hash_iterator_create(context, name, slice_start, slice_end, limit, FORWARD);
  }else{
    encode_hash_key(ldb_slice_data(name),
                    ldb_slice_size(name),
                    ldb_slice_data(kstart),
                    ldb_slice_size(kstart),
                    NULL,
                    &slice_start);
    if(ldb_slice_size(kstart) == 0){
      ldb_slice_push_back(slice_start, "\xff", strlen("\xff"));
    }
    encode_hash_key(ldb_slice_data(name),
                    ldb_slice_size(name),
                    ldb_slice_data(kend),
                    ldb_slice_size(kend),
                    NULL,
                    &slice_end);
    *piterator = ldb_hash_iterator_create(context, name, slice_start, slice_end, limit, BACKWARD);
  }
  return 0;
}
