#include "t_set.h"
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
#include <time.h>



static int sset_one(ldb_context_t *context, const ldb_slice_t* name, 
                    const ldb_slice_t* key, const ldb_meta_t* meta);

static int sget_one(ldb_context_t *context, const ldb_slice_t* name,
                    const ldb_slice_t* key, ldb_meta_t** pmeta);

static int sdel_one(ldb_context_t *context, const ldb_slice_t* name, 
                    const ldb_slice_t* key, const ldb_meta_t* meta);

static int set_incr_size(ldb_context_t *context, 
                          const ldb_slice_t* name, int64_t by);

static int sscan(ldb_context_t* context, const ldb_slice_t* name, 
                 const ldb_slice_t *kstart, const ldb_slice_t *kend, uint64_t limit, int reverse, ldb_set_iterator_t **piterator); 



void encode_ssize_key(const char* name, size_t namelen, const ldb_meta_t* meta, ldb_slice_t** pslice){
  ldb_slice_t* slice = ldb_meta_slice_create(meta);
  ldb_slice_push_back(slice, LDB_DATA_TYPE_SSIZE, strlen(LDB_DATA_TYPE_SSIZE));
  ldb_slice_push_back(slice, name, namelen);
  *pslice = slice;
}

int decode_ssize_key(const char* ldbkey, size_t ldbkeylen, ldb_slice_t** pslice){
  int retval = 0;
  ldb_slice_t *key_slice = NULL;
  ldb_bytes_t* bytes = ldb_bytes_create(ldbkey, ldbkeylen); 
  if(ldb_bytes_skip(bytes, strlen(LDB_DATA_TYPE_SSIZE)) == -1){
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

void encode_set_key(const char* name, size_t namelen, const char* key, size_t keylen, const ldb_meta_t* meta, ldb_slice_t** pslice){
  uint8_t len = 0;
  ldb_slice_t* slice = ldb_meta_slice_create(meta);
  ldb_slice_push_back(slice, LDB_DATA_TYPE_SET, strlen(LDB_DATA_TYPE_SET));
  len = (uint8_t)namelen;
  ldb_slice_push_back(slice, (const char*)(&len), sizeof(uint8_t));
  ldb_slice_push_back(slice, name, namelen);
  ldb_slice_push_back(slice, "=", 1);
  ldb_slice_push_back(slice, key, keylen);
  *pslice = slice;
}

int decode_set_key(const char* ldbkey, size_t ldbkeylen, ldb_slice_t** pslice_name, ldb_slice_t** pslice_key){
  int retval = 0;
  ldb_slice_t *slice_name, *slice_key = NULL;
  ldb_bytes_t *bytes = ldb_bytes_create(ldbkey, ldbkeylen);
  if(ldb_bytes_skip(bytes, strlen(LDB_DATA_TYPE_SET)) == -1){
    goto err;
  }
  if(ldb_bytes_read_slice_size_uint8(bytes, &slice_name) == -1){
    goto err;
  }
  if(ldb_bytes_skip(bytes, 1)==-1){
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
  if(pslice_name != NULL){
    *pslice_name = slice_name;     
  }else{
    ldb_slice_destroy(slice_name);
  }
  if(pslice_key != NULL){
    *pslice_key = slice_key;
  }else{
    ldb_slice_destroy(slice_key);
  }
  retval = 0;
  goto end;

end:
  ldb_bytes_destroy(bytes);
  return retval;
}


int set_card(ldb_context_t* context, const ldb_slice_t* name, uint64_t *length){
  int retval = 0;
  char *val = NULL, *errptr = NULL;
  size_t vallen = 0;
  leveldb_readoptions_t *readoptions = leveldb_readoptions_create();
  ldb_slice_t* slice_key = NULL;
  encode_ssize_key(ldb_slice_data(name), ldb_slice_size(name), NULL, &slice_key);
  val = leveldb_get(context->database_, readoptions, ldb_slice_data(slice_key), ldb_slice_size(slice_key), &vallen, &errptr);
  leveldb_readoptions_destroy(readoptions);
  ldb_slice_destroy(slice_key);
  if(errptr!=NULL){
    fprintf(stderr, "%s leveldb_get fail %s.\n", errptr, __func__);
    leveldb_free(errptr);
    retval = LDB_ERR;
    goto end;
  }
  if(val!=NULL){
    assert(vallen >= (sizeof(uint64_t) + LDB_VAL_META_SIZE));
    uint8_t type = leveldb_decode_fixed8(val);
    if(type & LDB_VALUE_TYPE_VAL){
      if(type & LDB_VALUE_TYPE_LAT){
        retval = LDB_OK_NOT_EXIST;
        goto end;
      }
      *length = leveldb_decode_fixed64(val + LDB_VAL_META_SIZE);
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

int set_members(ldb_context_t* context, const ldb_slice_t* name, ldb_list_t** pkeylist, ldb_list_t** pmetalist){
  int retval = 0;
  ldb_set_iterator_t* iterator = NULL;
  if(sscan(context, name, NULL, NULL, 20000000, 0, &iterator)!=0){
    retval = LDB_OK_RANGE_HAVE_NONE;
    goto end;
  }
  *pkeylist = ldb_list_create();
  *pmetalist = ldb_list_create();
  do{
    ldb_slice_t *key = NULL;
    size_t raw_klen = 0;
    const char* raw_key = ldb_set_iterator_key_raw(iterator, &raw_klen);
    if(decode_set_key( raw_key,
                       raw_klen,
                       NULL,
                       &key) == 0){
      ldb_meta_t* meta = NULL;
      if(sget_one(context, name, key, &meta) == LDB_OK){
        ldb_list_node_t *node_key = ldb_list_node_create();
        ldb_list_node_t *node_meta = ldb_list_node_create();
        node_key->data_ = key;
        node_key->type_ = LDB_LIST_NODE_TYPE_SLICE;
        node_meta->value_ = ldb_meta_nextver(meta);
        node_meta->type_ = LDB_LIST_NODE_TYPE_BASE;
        
        rpush_ldb_list_node(*pkeylist, node_key);
        rpush_ldb_list_node(*pmetalist, node_meta);
      }  
      ldb_meta_destroy(meta);
    }
  }while(!ldb_set_iterator_next(iterator));
  retval = LDB_OK;
  
end:
  ldb_set_iterator_destroy(iterator);
  return retval;
}

int set_add(ldb_context_t* context, const ldb_slice_t* name, const ldb_slice_t* key, const ldb_meta_t* meta){
    int retval = 0, ret = 0;

    ret = sset_one(context, name, key, meta); 
    if(ret >=0){
        if(ret > 0){
            if(set_incr_size(context, name, 1) < 0){
                retval = LDB_ERR;
                goto end;
            }
        }
        char *errptr = NULL;
        ldb_context_writebatch_commit(context, &errptr);
        if(errptr != NULL){
            fprintf(stderr, "%s leveldb_write fail %s.\n", __func__, errptr);
            leveldb_free(errptr);
            retval = LDB_ERR;
            goto  end;
        }
        retval = LDB_OK;
    }else{
        retval = LDB_ERR;
    }
end:
    return retval;
}

int set_pop(ldb_context_t* context, const ldb_slice_t* name, const ldb_meta_t* meta, ldb_slice_t** pslice){
    uint64_t length = 0;
    int retval = set_card(context, name, &length);
    if(retval!=LDB_OK){
        goto end;
    }

    ldb_set_iterator_t* iterator = NULL;
    if(sscan(context, name, NULL, NULL, 20000000, 0, &iterator)!=0){
        retval = LDB_OK_NOT_EXIST;
        goto end;
    }

    srandom(time(NULL));
    uint64_t offset = random()%length;
    if(ldb_set_iterator_skip(iterator, offset)<0){
        retval = LDB_ERR;
        goto end;
    }

    ldb_slice_t *key = NULL;
    size_t raw_klen = 0;
    const char* raw_key = ldb_set_iterator_key_raw(iterator, &raw_klen);
    if(decode_set_key(raw_key, raw_klen, NULL, &key) != 0){
        retval = LDB_ERR;
        goto end;
    }
    retval = set_rem(context, name, key, meta);
    if(retval == LDB_OK){
        *pslice = key;
    }else{
        ldb_slice_destroy(key);
    }

end:
    ldb_set_iterator_destroy(iterator);
    return retval;
}

int set_rem(ldb_context_t* context, const ldb_slice_t* name, const ldb_slice_t* key, const ldb_meta_t* meta){
    int retval, ret = 0;
    ret = sdel_one(context, name, key, meta);
    if(ret >=0){
        if(ret > 0){
            if(set_incr_size(context, name, -1) < 0){
                retval = LDB_ERR;
                goto end;
            }
            char *errptr = NULL;
            ldb_context_writebatch_commit(context, &errptr);
            if(errptr != NULL){
                fprintf(stderr, "%s leveldb_write fail %s.\n", errptr, __func__);
                leveldb_free(errptr);
                retval = LDB_ERR;
                goto  end;
            }
            retval = LDB_OK;
            goto end;
        }
        retval = LDB_OK_NOT_EXIST;
    }else{
        retval = LDB_ERR;
    }

end:
    return retval;
}


int set_ismember(ldb_context_t* context, const ldb_slice_t* name, const ldb_slice_t* key){
    ldb_meta_t *meta = NULL;
    int retval = sget_one(context, name, key, &meta);
    if(retval == LDB_OK){
        ldb_meta_destroy(meta);
    }
    return retval;
}


static int set_incr_size(ldb_context_t *context, 
                         const ldb_slice_t* name, int64_t by){
  uint64_t length = 0;
  int retval = set_card(context, name, &length);
  if(retval!=LDB_OK && retval != LDB_OK_NOT_EXIST){
    return -1;
  }
  int64_t size = (int64_t)length;
  size += by;

  ldb_slice_t* slice_key = NULL;
  encode_ssize_key(ldb_slice_data(name),
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
                               sizeof(buff));
  }
  return 0;
}

static int sdel_one(ldb_context_t *context, const ldb_slice_t* name, 
                    const ldb_slice_t* key, const ldb_meta_t* meta){
  
  if(ldb_slice_size(name) > LDB_DATA_TYPE_KEY_LEN_MAX){
    fprintf(stderr, "%s name too long!", __func__);
    return -1;
  }
  if(ldb_slice_size(key) > LDB_DATA_TYPE_KEY_LEN_MAX){
    fprintf(stderr, "%s key too long!", __func__);
    return -1;
  }

  ldb_meta_t *old_meta = NULL;
  if(sget_one(context, name, key, &old_meta) == LDB_OK_NOT_EXIST){
    return 0;
  }

  ldb_slice_t *slice_key = NULL;
  encode_set_key( ldb_slice_data(name),
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


static int sget_one(ldb_context_t *context, const ldb_slice_t* name,
                    const ldb_slice_t* key, ldb_meta_t** pmeta){ 
  int retval = 0;
  char *val = NULL, *errptr = NULL;
  size_t vallen = 0;
  leveldb_readoptions_t *readoptions = leveldb_readoptions_create();
  ldb_slice_t* slice_key = NULL;
  encode_set_key(ldb_slice_data(name), ldb_slice_size(name), ldb_slice_data(key), ldb_slice_size(key), NULL, &slice_key);
  val = leveldb_get(context->database_, readoptions, ldb_slice_data(slice_key), ldb_slice_size(slice_key), &vallen, &errptr);
  leveldb_readoptions_destroy(readoptions);
  ldb_slice_destroy(slice_key);
  if(errptr!=NULL){
    fprintf(stderr, "%s leveldb_get fail %s.\n", errptr, __func__);
    leveldb_free(errptr);
    retval = LDB_ERR;
    goto end;
  }
  if(val!=NULL){
    assert(vallen>= LDB_VAL_META_SIZE);
    uint8_t type = leveldb_decode_fixed8(val);
    if(type & LDB_VALUE_TYPE_VAL){
      if(type & LDB_VALUE_TYPE_LAT){
        retval = LDB_OK_NOT_EXIST;
        goto end;
      }
      uint64_t version = leveldb_decode_fixed64(val + LDB_VAL_TYPE_SIZE);
      *pmeta = ldb_meta_create(0, 0, version);
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


static int sset_one(ldb_context_t* context, const ldb_slice_t* name,
                    const ldb_slice_t* key, const ldb_meta_t* meta){
  if(ldb_slice_size(name)==0 || ldb_slice_size(key)==0){
    fprintf(stderr, "%s empty name or key!", __func__);
    return 0;
  }
  if(ldb_slice_size(name) > LDB_DATA_TYPE_KEY_LEN_MAX){
    fprintf(stderr, "%s name too long!", __func__);
    return -1;
  }
  if(ldb_slice_size(key) > LDB_DATA_TYPE_KEY_LEN_MAX){
    fprintf(stderr, "%s name too long!", __func__);
    return -1;
  }
  int retval = 0;
  ldb_slice_t *slice_key = NULL;
  ldb_meta_t *old_meta = NULL;
  if(sget_one(context, name, key, &old_meta) == LDB_OK_NOT_EXIST){
    encode_set_key(ldb_slice_data(name),
                    ldb_slice_size(name),
                    ldb_slice_data(key),
                    ldb_slice_size(key),
                    meta,
                    &slice_key);

    ldb_context_writebatch_put(context,
                               ldb_slice_data(slice_key),
                               ldb_slice_size(slice_key),
                               NULL,
                               0);
    retval = 1;
  }else{
    encode_set_key(ldb_slice_data(name),
                    ldb_slice_size(name),
                    ldb_slice_data(key),
                    ldb_slice_size(key),
                    meta,
                    &slice_key);

    ldb_context_writebatch_put(context,
                               ldb_slice_data(slice_key),
                               ldb_slice_size(slice_key),
                               NULL,
                               0); 

    ldb_meta_destroy(old_meta);
    retval = 0;
  }
end:
  ldb_slice_destroy(slice_key);
  return retval;
}


static int sscan(ldb_context_t* context, const ldb_slice_t* name, 
                 const ldb_slice_t *kstart, const ldb_slice_t *kend, uint64_t limit, int reverse, ldb_set_iterator_t **piterator){

  ldb_slice_t *slice_start, *slice_end = NULL;
  if(!reverse){

    encode_set_key(ldb_slice_data(name),
                    ldb_slice_size(name),
                    ldb_slice_data(kstart),
                    ldb_slice_size(kstart),
                    NULL,
                    &slice_start);
    encode_set_key(ldb_slice_data(name),
                    ldb_slice_size(name),
                    ldb_slice_data(kend),
                    ldb_slice_size(kend),
                    NULL,
                    &slice_end);
    if(ldb_slice_size(kend) == 0){
      ldb_slice_push_back(slice_end, "\xff", strlen("\xff"));
    }

    *piterator = ldb_set_iterator_create(context, name, slice_start, slice_end, limit, FORWARD);
  }else{
    encode_set_key(ldb_slice_data(name),
                    ldb_slice_size(name),
                    ldb_slice_data(kstart),
                    ldb_slice_size(kstart),
                    NULL,
                    &slice_start);
    if(ldb_slice_size(kstart) == 0){
      ldb_slice_push_back(slice_start, "\xff", strlen("\xff"));
    }
    encode_set_key(ldb_slice_data(name),
                    ldb_slice_size(name),
                    ldb_slice_data(kend),
                    ldb_slice_size(kend),
                    NULL,
                    &slice_end);
    *piterator = ldb_hash_iterator_create(context, name, slice_start, slice_end, limit, BACKWARD);
  }

end:
  ldb_slice_destroy(slice_start);
  ldb_slice_destroy(slice_end);
  return 0;
}
