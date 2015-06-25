#include "ldb_define.h"
#include "ldb_slice.h"
#include "ldb_context.h"
#include "ldb_meta.h"
#include "ldb_bytes.h"
#include "ldb_list.h"
#include "t_kv.h"

#include <leveldb-ldb/c.h>
#include <string.h>
#include <stdlib.h>
#include <assert.h>
#include <stdio.h>



static void encode_kv_key(const char* key, size_t keylen, const ldb_meta_t* meta, ldb_slice_t** pslice){
  ldb_slice_t* slice = ldb_meta_slice_create(meta);
  ldb_slice_push_back(slice, LDB_DATA_TYPE_KV, strlen(LDB_DATA_TYPE_KV));
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
  int retval = 0;
  if(ldb_slice_size(key) == 0){
    fprintf(stderr, "empty key!\n");
    retval = LDB_ERR;
    goto end;
  }
  char *errptr = NULL;
  leveldb_writeoptions_t* writeoptions = leveldb_writeoptions_create();
  ldb_slice_t *slice_key = NULL;
  encode_kv_key(ldb_slice_data(key), ldb_slice_size(key), meta, &slice_key);
  leveldb_put(context->database_, 
              writeoptions, 
              ldb_slice_data(slice_key), 
              ldb_slice_size(slice_key), 
              ldb_slice_data(value), 
              ldb_slice_size(value), 
              &errptr);
  leveldb_writeoptions_destroy(writeoptions);
  ldb_slice_destroy(slice_key);
  if(errptr != NULL){
    fprintf(stderr, "leveldb_put fail %s.\n", errptr);
    leveldb_free(errptr);
    retval = LDB_ERR;
    goto end;
  }
  retval = LDB_OK;

end:
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
  encode_kv_key(ldb_slice_data(key), ldb_slice_size(key), meta, &slice_key);
  leveldb_put(context->database_, 
              writeoptions, 
              ldb_slice_data(slice_key), 
              ldb_slice_size(slice_key), 
              ldb_slice_data(value), 
              ldb_slice_size(value), 
              &errptr);
  leveldb_writeoptions_destroy(writeoptions);
  ldb_slice_destroy(slice_key);
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
  encode_kv_key(ldb_slice_data(key), ldb_slice_size(key), meta, &slice_key);
  leveldb_put(context->database_, 
              writeoptions, 
              ldb_slice_data(slice_key), 
              ldb_slice_size(slice_key), 
              ldb_slice_data(value), 
              ldb_slice_size(value), 
              &errptr);
  leveldb_writeoptions_destroy(writeoptions);
  ldb_slice_destroy(slice_key);
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


int string_mset(ldb_context_t* context, const ldb_list_t* datalist, const ldb_list_t* metalist, ldb_list_t** plist){
  leveldb_mutex_lock(context->mutex_);
  int retval = 0; 
  ldb_list_iterator_t *dataiterator = ldb_list_iterator_create(datalist);
  ldb_list_iterator_t *metaiterator = ldb_list_iterator_create(metalist);
  ldb_list_t *retlist = ldb_list_create();
  
  
  while(1){
    ldb_list_node_t* node_key = ldb_list_next(&dataiterator);
    if(node_key== NULL){
      retval = LDB_OK;
      break;
    }
    //encode key
    ldb_list_node_t* node_meta = ldb_list_next(&metaiterator);
    ldb_slice_t *slice_key = NULL;
    ldb_slice_t *key = (ldb_slice_t*)(node_key->data_);
    ldb_meta_t *meta = (ldb_meta_t*)(node_meta->data_);
    encode_kv_key(ldb_slice_data(key), ldb_slice_size(key), meta, &slice_key);

    //put kv
    ldb_list_node_t* node_val = ldb_list_next(&dataiterator);
    ldb_slice_t *value = (ldb_slice_t*)(node_val->data_);
    ldb_context_writebatch_put(context,
                               ldb_slice_data(slice_key),
                               ldb_slice_size(slice_key),
                               ldb_slice_data(value),
                               ldb_slice_size(value));
    
    //push return value
    ldb_list_node_t* node_ret = ldb_list_node_create();
    node_ret->type_ = LDB_LIST_NODE_TYPE_BASE;
    node_ret->value_ = LDB_OK;
    rpush_ldb_list_node(retlist, node_ret); 

    //destroy slice_key
    ldb_slice_destroy(slice_key);
  } 
  char* errptr = NULL;
  ldb_context_writebatch_commit(context, &errptr);
  if(errptr != NULL){
    fprintf(stderr, "write writebatch fail %s.\n", errptr);
    leveldb_free(errptr);
    retval = LDB_ERR;
    goto err;
  }
  goto end;

err:
  ldb_list_destroy(retlist);
  retlist = NULL;
  goto end;
 
end:
  if(retlist != NULL){
    (*plist) = retlist;
  }
  ldb_list_iterator_destroy(dataiterator);
  ldb_list_iterator_destroy(metaiterator);
  leveldb_mutex_unlock(context->mutex_);
  return retval;
}


int string_msetnx(ldb_context_t* context, const ldb_list_t* datalist, const ldb_list_t* metalist, ldb_list_t** plist){
  leveldb_mutex_lock(context->mutex_);
  int retval = 0; 
  ldb_list_iterator_t *dataiterator = ldb_list_iterator_create(datalist);
  ldb_list_iterator_t *metaiterator = ldb_list_iterator_create(metalist);
  ldb_list_t *retlist = ldb_list_create();
  
  
  while(1){
    ldb_list_node_t* node_key = ldb_list_next(&dataiterator);
    if(node_key== NULL){
      retval = LDB_OK;
      break;
    }

    //encode key
    ldb_list_node_t* node_meta = ldb_list_next(&metaiterator);
    ldb_slice_t *slice_key = NULL;
    ldb_slice_t *key = (ldb_slice_t*)(node_key->data_);


    //check get result
    ldb_slice_t *slice_value = NULL;
    int found = string_get(context, key, &slice_value);
    if(found == LDB_OK){
      ldb_list_node_t* node = ldb_list_node_create();
      node->type_ = LDB_LIST_NODE_TYPE_BASE;
      node->value_ = LDB_OK_BUT_ALREADY_EXIST;
      rpush_ldb_list_node(retlist, node); 
      ldb_slice_destroy(slice_value);
      continue;
    }

    ldb_meta_t *meta = (ldb_meta_t*)(node_meta->data_);
    encode_kv_key(ldb_slice_data(key), ldb_slice_size(key), meta, &slice_key);

    //put kv
    ldb_list_node_t* node_val = ldb_list_next(&dataiterator);
    ldb_slice_t *value = (ldb_slice_t*)(node_val->data_);
    ldb_context_writebatch_put(context,
                               ldb_slice_data(slice_key),
                               ldb_slice_size(slice_key),
                               ldb_slice_data(value),
                               ldb_slice_size(value));
    
    //push return value
    ldb_list_node_t* node = ldb_list_node_create();
    node->type_ = LDB_LIST_NODE_TYPE_BASE;
    node->value_ = LDB_OK;
    rpush_ldb_list_node(retlist, node); 

    //destroy slice_key
    ldb_slice_destroy(slice_key);
  } 
  char* errptr = NULL;
  ldb_context_writebatch_commit(context, &errptr);
  if(errptr != NULL){
    fprintf(stderr, "write writebatch fail %s.\n", errptr);
    leveldb_free(errptr);
    retval = LDB_ERR;
    goto err;
  }
  goto end;

err:
  ldb_list_destroy(retlist);
  retlist = NULL;
  goto end;
 
end:
  if(retlist != NULL){
    (*plist) = retlist;
  }
  ldb_list_iterator_destroy(dataiterator);
  ldb_list_iterator_destroy(metaiterator);
  leveldb_mutex_unlock(context->mutex_);
  return retval;
}

int string_get(ldb_context_t* context, const ldb_slice_t* key, ldb_slice_t** pvalue){
  char *val, *errptr = NULL;
  size_t vallen = 0;
  leveldb_readoptions_t* readoptions = leveldb_readoptions_create();
  ldb_slice_t *slice_key = NULL;
  encode_kv_key(ldb_slice_data(key), ldb_slice_size(key), NULL, &slice_key);
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
    assert(vallen>= LDB_VAL_META_SIZE);
    uint8_t type = leveldb_decode_fixed8(val);
    if(type == LDB_VALUE_TYPE_VAL){
      *pvalue = ldb_slice_create(val+LDB_VAL_TYPE_SIZE, vallen-LDB_VAL_TYPE_SIZE);
      retval = LDB_OK;
    }else{
      retval = LDB_OK_NOT_EXIST;
    }
    leveldb_free(val);
  }else{
    retval = LDB_OK_NOT_EXIST;
  }

end:
  return retval;
}

int string_del(ldb_context_t* context, const ldb_slice_t* key, const ldb_meta_t* meta){
  int retval = 0;
  if(ldb_slice_size(key) == 0){
    fprintf(stderr, "empty key!\n");
    retval = LDB_ERR;
    goto end;
  }
  char *errptr = NULL;
  leveldb_writeoptions_t* writeoptions = leveldb_writeoptions_create();
  ldb_slice_t *slice_key = NULL;
  encode_kv_key(ldb_slice_data(key), ldb_slice_size(key), meta, &slice_key);
  leveldb_delete(context->database_, 
                 writeoptions, 
                 ldb_slice_data(slice_key), 
                 ldb_slice_size(slice_key), 
                 &errptr);
  leveldb_writeoptions_destroy(writeoptions);
  ldb_slice_destroy(slice_key);
  if(errptr != NULL){
    fprintf(stderr, "leveldb_delete fail %s.\n", errptr);
    leveldb_free(errptr);
    retval = LDB_ERR;
    goto end;
  }
  retval = LDB_OK;

end:
  return retval;
}
