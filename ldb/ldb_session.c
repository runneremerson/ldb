#include "ldb_session.h"
#include "ldb_context.h"
#include "ldb_slice.h"
#include "ldb_meta.h"
#include "ldb_define.h"
#include "ldb_list.h"
#include "ldb_expiration.h"
#include "ldb_recovery.h"

#include "trace.h"
#include "config.h"
#include "lmalloc.h"
#include "util.h"
#include "util/cgo_util_base.h"

#include "t_string.h"
#include "t_zset.h"
#include "t_hash.h"
#include "t_set.h"

#include <leveldb/c.h>
#include <string.h>
#include <assert.h>

void fill_value_item(value_item_t* item, uint64_t version, const char* data, size_t size){
  item->version_ = version;
  if(item->data_ != NULL){
    lfree(item->data_);
  }
  item->data_ = lmalloc(size);
  memcpy(item->data_, data, size);
}

void free_value_item(value_item_t* item){
  if(item != NULL){
    lfree(item->data_);
  }
  item->data_ = NULL;
}

void destroy_value_item_array( value_item_t* array, size_t size){
  for( size_t i=0; i<size; ++i){
    free_value_item(&array[i]);
  }
  lfree(array);
}

value_item_t* create_value_item_array( size_t size){
  if( size == 0){
    return NULL;
  }
  value_item_t *array = (value_item_t*)lmalloc(size * sizeof(value_item_t));
  memset(array, 0, size*sizeof(value_item_t));
  return array;
}

enum ExistOrNot
{
  IS_NOT_EXIST = 0, //nx
  IS_EXIST = 1,
  IS_NOT_EXIST_AND_EXPIRE = 2,
  IS_EXIST_AND_EXPIRE = 3
};


char g_programname[1024] = {0};



int set_ldb_signal_handler(const char* name){
  strcpy(g_programname, name);
  set_signal_handler();
  return 0;
}

static int decode_slice_value(const ldb_slice_t* slice_val, const ldb_meta_t* meta, value_item_t* item){
  uint64_t version = ldb_meta_nextver(meta);
  const char* data = ldb_slice_data(slice_val);
  size_t size = ldb_slice_size(slice_val);
  fill_value_item(item, version, data, size); 
  return 0;
}

static int decode_slice_expire(const ldb_slice_t* slice_expire, uint64_t expire, value_item_t* item){
  fill_value_item(item, expire, ldb_slice_data(slice_expire), ldb_slice_size(slice_expire));
  return 0;
}

static char* malloc_and_copy(const char* data, size_t size){
  char* result = lmalloc(size + 1);
  memcpy(result, data, size); 
  result[size] = '\0';
  return result;
}


int ldb_fetch_expire(ldb_context_t* context, ldb_expiration_t** expiration, value_item_t** items, size_t* itemnum){
    int retval = 0;

    if(*expiration == NULL){
        *expiration = ldb_expiration_create( context );
    }
    ldb_list_t *expire_list = NULL;
    size_t limit = 1000;
    retval = ldb_expiration_exp_batch(*expiration, &expire_list, limit);
    if(retval <0){
        retval = -1;
        goto end;
    }
    *itemnum = expire_list->length_;
    if(*itemnum == 0){
        goto end;
    }
    *items = create_value_item_array(*itemnum);
    ldb_list_iterator_t *iterator = ldb_list_iterator_create(expire_list);
    size_t now = 0;
    while(1){
        ldb_list_node_t *node = ldb_list_next( &iterator );
        if(node == NULL){
            retval = 0;
            goto end;
        }
        ldb_slice_t *key = (ldb_slice_t*)node->data_;
        uint64_t expire = node->value_;
        decode_slice_expire(key, expire, &((*items)[now]));
        ++now;
    }

end:
    ldb_list_destroy(expire_list);
    ldb_list_iterator_destroy(iterator);
    if(retval < 0){
        ldb_expiration_destroy(*expiration);
        *expiration = NULL;
    }
    return retval;
}

int ldb_recover_meta(ldb_context_t* context, ldb_recovery_t** recovery){
    int retval = 0;

    if(*recovery == NULL){
        *recovery = ldb_recovery_create(context);
    }
    size_t limit = 50000;
    retval = ldb_recovery_rec_batch(context, *recovery, limit);
    if(retval < 0){
        ldb_recovery_destroy(*recovery);
        ldb_context_release_recovering_snapshot(context);
        *recovery = NULL;
    }
    return retval; 
}


int ldb_set(ldb_context_t* context, 
            char* key, 
            size_t keylen, 
            uint64_t lastver, 
            int vercare, 
            uint64_t exptime, 
            value_item_t* item, 
            int en){
  int retval = 0;
  ldb_slice_t *slice_key, *slice_val , *slice_value = NULL;
  slice_key = ldb_slice_create(key, keylen);
  slice_val = ldb_slice_create(item->data_, item->data_len_);
  ldb_meta_t *meta = NULL;
  if(en == IS_NOT_EXIST){
    meta = ldb_meta_create(vercare, lastver, item->version_);
    retval = string_setnx(context, slice_key, slice_val, meta);
  } else if(en == IS_EXIST){
    meta = ldb_meta_create(vercare, lastver, item->version_);
    retval = string_set(context, slice_key, slice_val, meta);
  } else if(en == IS_EXIST_AND_EXPIRE){
    meta = ldb_meta_create_with_exp(vercare, lastver, item->version_, exptime);
    meta = ldb_meta_create(vercare, lastver, item->version_);
  } else if(en == IS_NOT_EXIST_AND_EXPIRE){
    meta = ldb_meta_create_with_exp(vercare, lastver, item->version_, exptime);
    retval = string_setnx(context, slice_key, slice_val, meta);
  }

  ldb_slice_destroy(slice_key);
  ldb_slice_destroy(slice_val);
  ldb_slice_destroy(slice_value);
  ldb_meta_destroy(meta);

  return retval;
}


int ldb_mset(ldb_context_t* context, 
             uint64_t lastver, 
             int vercare, 
             uint64_t exptime, 
             GoByteSlice* keyvals, 
             GoUint64Slice* versions, 
             size_t length,
             GoUint64Slice* results,
             int en){
  int retval = 0;
  ldb_list_t *datalist, *metalist, *retlist = NULL;
  datalist = ldb_list_create();
  metalist = ldb_list_create();
  for( size_t i=0, v=0; i<length;){
    //push key
    ldb_slice_t *slice_key = ldb_slice_create(keyvals[i].data, keyvals[i].data_len);
    ldb_list_node_t *node_key = ldb_list_node_create();
    node_key->type_ = LDB_LIST_NODE_TYPE_SLICE;
    node_key->data_ = slice_key;;
    rpush_ldb_list_node(datalist, node_key);
    ++i;

    //push value
    ldb_slice_t *slice_val = ldb_slice_create(keyvals[i].data, keyvals[i].data_len);
    ldb_list_node_t *node_val = ldb_list_node_create();
    node_val->type_ = LDB_LIST_NODE_TYPE_SLICE;
    node_val->data_ = slice_val;
    rpush_ldb_list_node(datalist, node_val);
    ++i;

    //push meta
    ldb_list_node_t *node_meta = ldb_list_node_create();
    node_meta->type_ = LDB_LIST_NODE_TYPE_META;
    if(exptime >0){
      node_meta->data_ = ldb_meta_create_with_exp(vercare, lastver, versions->data[v], exptime);      
    }else{
      node_meta->data_ = ldb_meta_create(vercare, lastver, versions->data[v]);
    }
    rpush_ldb_list_node(metalist, node_meta);
    ++v;
  }
  

  if(en == IS_NOT_EXIST){
    retval = string_msetnx(context, datalist, metalist, &retlist);
  } else {
    retval = string_mset(context, datalist, metalist, &retlist);
  }

  if(retval != LDB_OK){
    goto end;
  }
  ldb_list_iterator_t *iterator = ldb_list_iterator_create(retlist);
  size_t now = 0;
  while(1){
    ldb_list_node_t *node = ldb_list_next(&iterator);
    if(node == NULL){
      break;
    }
    results->data[now] = (int)(node->value_);
    ++now;
  }
  ldb_list_iterator_destroy(iterator);
  
end:
  ldb_list_destroy(datalist);
  ldb_list_destroy(metalist);
  ldb_list_destroy(retlist);
  return retval;
}

int ldb_expire(ldb_context_t* context,
              char* key,
              size_t keylen,
              uint64_t exptime,
              uint64_t version){
    int retval = 0;
    ldb_slice_t *slice_key = ldb_slice_create(key, keylen);
    ldb_slice_t *slice_val = NULL;
    ldb_meta_t *meta = ldb_meta_create_with_exp(0, 0, version, exptime);
    ldb_meta_t *old_meta = NULL;

    retval = string_get(context, slice_key, &slice_val, &old_meta);
    if(retval != LDB_OK){
        goto end;
    }
    retval = string_set(context, slice_key, slice_val, meta);
    

end:
    ldb_slice_destroy(slice_key);
    ldb_slice_destroy(slice_val);
    ldb_meta_destroy(meta);
    ldb_meta_destroy(old_meta);
    return retval;
}

int ldb_pexpire(ldb_context_t* context,
               char* key,
               size_t keylen,
               uint64_t exptime,
               uint64_t version){
    int retval = 0;
    ldb_slice_t *slice_key = ldb_slice_create(key, keylen);
    ldb_slice_t *slice_val = NULL;
    ldb_meta_t *meta = ldb_meta_create_with_exp(0, 0, version, exptime);
    ldb_meta_t *old_meta = NULL;

    retval = string_get(context, slice_key, &slice_val, &old_meta);
    if(retval != LDB_OK){
        goto end;
    }
    retval = string_set(context, slice_key, slice_val, meta);

end:
    ldb_slice_destroy(slice_key);
    ldb_slice_destroy(slice_val);
    ldb_meta_destroy(meta);
    ldb_meta_destroy(old_meta);
    return retval;
}

int ldb_ttl(ldb_context_t* context,
           char* key,
           size_t keylen,
           uint64_t* remain){
    int retval = 0;
    ldb_slice_t *slice_key, *slice_val = NULL;
    ldb_meta_t *meta = NULL;
    slice_key = ldb_slice_create(key, keylen);

    retval = string_get(context, slice_key, &slice_val, &meta);
    if(retval != LDB_OK){
        goto end;
    }
    int64_t ttl = (ldb_meta_exptime(meta)-time_ms());
    if(ttl < 0){
        ttl = 0;
    }else{
        ttl = ttl/1000;
    }
    *remain = (uint64_t)ttl;

end:
    ldb_slice_destroy(slice_key);
    ldb_slice_destroy(slice_val);
    ldb_meta_destroy(meta);
    return retval;
}

int ldb_pttl(ldb_context_t* context,
            char* key,
            size_t keylen,
            uint64_t* remain){
    int retval = 0;
    ldb_slice_t *slice_key, *slice_val = NULL;
    ldb_meta_t *meta = NULL;
    slice_key = ldb_slice_create(key, keylen);

    retval = string_get(context, slice_key, &slice_val, &meta);
    if(retval != LDB_OK){
        goto end;
    }
    int64_t ttl = (ldb_meta_exptime(meta)-time_ms());
    if(ttl < 0){
        ttl = 0;
    }
    *remain = (uint64_t)ttl;

end:
    ldb_slice_destroy(slice_key);
    ldb_slice_destroy(slice_val);
    ldb_meta_destroy(meta);
    return retval;
}


int ldb_persist(ldb_context_t* context,
               char* key,
               size_t keylen,
               uint64_t version){
    int retval = 0;
    ldb_slice_t *slice_key = ldb_slice_create(key, keylen);
    ldb_meta_t *meta = NULL;
    ldb_slice_t *slice_val = NULL;
    retval = string_get(context, slice_key, &slice_val, &meta);
    if(retval != LDB_OK){
        goto end;
    }
    ldb_meta_t* new_meta = ldb_meta_create_with_exp(0, 0, version, 0);
    retval = string_set(context, slice_key, slice_val, new_meta);
    ldb_meta_destroy(new_meta);

end:
    ldb_slice_destroy(slice_key);
    ldb_slice_destroy(slice_val);
    ldb_meta_destroy(meta);
    return retval;
}

int ldb_exists(ldb_context_t* context,
               char* key,
               size_t keylen){
    int retval = 0;
    ldb_slice_t *slice_key = ldb_slice_create(key, keylen);
    ldb_meta_t *meta = NULL;
    ldb_slice_t *slice_val = NULL;
    retval = string_get(context, slice_key, &slice_val, &meta);
                   
    ldb_slice_destroy(slice_key);
    ldb_slice_destroy(slice_val);
    ldb_meta_destroy(meta);
    return retval;
}

int ldb_get(ldb_context_t* context, 
            char* key, 
            size_t keylen, 
            value_item_t** item){
  int retval = 0;
  ldb_slice_t *slice_key, *slice_val = NULL;
  slice_key = ldb_slice_create(key, keylen);
  ldb_meta_t *meta = NULL;
  retval = string_get(context, slice_key, &slice_val, &meta);
  if(retval != LDB_OK){
    goto end;
  }
  *item = create_value_item_array(1);
  if(decode_slice_value(slice_val, meta, *item)){
    retval = LDB_ERR;
    goto end;
  }
  retval = LDB_OK; 
end:
  ldb_slice_destroy(slice_key);
  ldb_slice_destroy(slice_val);
  ldb_meta_destroy(meta);

  return retval;
}


int ldb_mget(ldb_context_t* context,
             GoByteSlice* slice,
             size_t length,
             GoByteSliceSlice* items,
             GoUint64Slice* versions,
             size_t* itemnum){
  int retval = 0;
  ldb_list_t *keylist, *vallist, *metalist = NULL;
  keylist = ldb_list_create();
  for( size_t i=0; i<length;){
    //push key
    ldb_slice_t *slice_key = ldb_slice_create(slice[i].data, slice[i].data_len);
    ldb_list_node_t *node_key = ldb_list_node_create();
    node_key->type_ = LDB_LIST_NODE_TYPE_SLICE;
    node_key->data_ = slice_key;;
    rpush_ldb_list_node(keylist, node_key);
    ++i;
  }

  retval = string_mget(context, keylist, &vallist, &metalist);
  if(retval != LDB_OK){
    goto end;
  }
  ldb_list_iterator_t *valiterator = ldb_list_iterator_create(vallist);
  ldb_list_iterator_t *metiterator = ldb_list_iterator_create(metalist);
  size_t now = 0;
  while(1){
    ldb_list_node_t* node_val = ldb_list_next(&valiterator);
    if(node_val == NULL){
      retval = LDB_OK;
      break;
    }
    ldb_list_node_t* node_met = ldb_list_next(&metiterator);
    if(node_met == NULL){
      retval = LDB_OK;
      break;
    }
    if( node_val->type_ == LDB_LIST_NODE_TYPE_NONE ){
      items->data[now].data_len = 0;
      items->data[now].data = NULL;
    }else{
      size_t data_len = ldb_slice_size((ldb_slice_t*)node_val->data_);
      items->data[now].data_len = data_len;
      char* data = malloc_and_copy(ldb_slice_data((ldb_slice_t*)node_val->data_),  data_len);
      items->data[now].data = data;
    }
    if( node_met->type_ == LDB_LIST_NODE_TYPE_NONE ){
      versions->data[now] = 0;
    }else{
      versions->data[now] = ldb_meta_nextver((ldb_meta_t*)(node_met->data_));
    }
    items->data[now].cap = items->data[now].data_len;
    ++now;
  }
  *itemnum = now;
  ldb_list_iterator_destroy(valiterator);
  ldb_list_iterator_destroy(metiterator);

end:
  ldb_list_destroy(keylist);
  ldb_list_destroy(vallist);
  ldb_list_destroy(metalist);
  return retval;
}

int ldb_del(ldb_context_t* context, 
            char* key, 
            size_t keylen, 
            int vercare, 
            uint64_t version){
  int retval = 0;
  ldb_slice_t *slice_key = ldb_slice_create(key, keylen);
  ldb_meta_t *meta = ldb_meta_create(vercare, 0, version);
  retval = string_del(context, slice_key, meta);

  ldb_slice_destroy(slice_key);
  ldb_meta_destroy(meta);
  return retval;
}

int ldb_incrby(ldb_context_t* context,
               char* key,
               size_t keylen,
               uint64_t lastver,
               int vercare,
               uint64_t exptime,
               uint64_t version,
               int64_t initval,
               int64_t by,
               int64_t* result){
  int retval = 0;
  ldb_slice_t *slice_key = ldb_slice_create(key, keylen);
  ldb_meta_t *meta = ldb_meta_create_with_exp(vercare, lastver, version, exptime);
  retval = string_incr(context, slice_key, meta, initval, by, result);

  ldb_slice_destroy(slice_key);
  ldb_meta_destroy(meta);
  return retval;
}

int ldb_hgetall(ldb_context_t* context,
                char* name,
                size_t namelen,
                value_item_t** items,
                size_t* itemnum){
    int retval = 0;
    ldb_slice_t *slice_name = ldb_slice_create(name, namelen);
    ldb_list_t *keylist, *vallist, *metalist = NULL;
    retval = hash_getall(context, slice_name, &keylist, &vallist, &metalist);
    if( retval != LDB_OK ){
        goto end;
    }
    *itemnum = 2*(keylist->length_);
    *items = create_value_item_array(*itemnum);
    ldb_list_iterator_t *keyiterator = ldb_list_iterator_create(keylist);
    ldb_list_iterator_t *valiterator = ldb_list_iterator_create(vallist);
    ldb_list_iterator_t *metiterator = ldb_list_iterator_create(metalist);
    size_t now = 0;
    while(1){
        ldb_list_node_t* node_key = ldb_list_next(&keyiterator);
        if(node_key == NULL){
            retval = LDB_OK;
            break;
        }
        ldb_list_node_t* node_val = ldb_list_next(&valiterator);
        assert(node_val != NULL);
        ldb_list_node_t* node_met = ldb_list_next(&metiterator);
        assert(node_met != NULL);
        size_t key_len = ldb_slice_size((ldb_slice_t*)node_key->data_);
        (*items)[now].data_len_ = key_len;
        (*items)[now].data_ = malloc_and_copy(ldb_slice_data((ldb_slice_t*)node_key->data_), key_len);
        ++now;
        size_t val_len = ldb_slice_size((ldb_slice_t*)node_val->data_);
        (*items)[now].data_len_ = val_len;
        (*items)[now].data_ = malloc_and_copy(ldb_slice_data((ldb_slice_t*)node_val->data_), val_len);
        (*items)[now].version_ = node_met->value_;
        ++now;
    }
    ldb_list_iterator_destroy(keyiterator);
    ldb_list_iterator_destroy(valiterator);
    ldb_list_iterator_destroy(metiterator);

end:
    ldb_slice_destroy(slice_name);
    ldb_list_destroy(keylist);
    ldb_list_destroy(vallist);
    ldb_list_destroy(metalist);
    return retval;
}

int ldb_hkeys(ldb_context_t* context,
              char* name,
              size_t namelen,
              value_item_t** items,
              size_t* itemnum){

    int retval = 0;
    ldb_slice_t *slice_name = ldb_slice_create(name, namelen);
    ldb_list_t *keylist= NULL;
    retval = hash_keys(context, slice_name, &keylist);
    if( retval != LDB_OK ){
        goto end;
    }

    *itemnum = keylist->length_;
    *items = create_value_item_array(*itemnum);
    ldb_list_iterator_t *keyiterator = ldb_list_iterator_create(keylist);
    size_t now = 0;
    while(1){
        ldb_list_node_t* node_key = ldb_list_next(&keyiterator);
        if(node_key == NULL){
            retval = LDB_OK;
            break;
        }
        size_t key_len = ldb_slice_size((ldb_slice_t*)node_key->data_);
        (*items)[now].data_len_ = key_len;
        (*items)[now].data_ = malloc_and_copy(ldb_slice_data((ldb_slice_t*)node_key->data_), key_len);
        ++now;
    }
    ldb_list_iterator_destroy(keyiterator);

end:
    ldb_slice_destroy(slice_name);
    ldb_list_destroy(keylist);
    return retval;
}

int ldb_hvals(ldb_context_t* context,
              char* name,
              size_t namelen,
              value_item_t** items,
              size_t* itemnum){

    int retval = 0;
    ldb_slice_t *slice_name = ldb_slice_create(name, namelen);
    ldb_list_t *vallist, *metalist = NULL;
    retval = hash_vals(context, slice_name, &vallist, &metalist);
    if( retval != LDB_OK ){
        goto end;
    }
    *itemnum = vallist->length_;
    *items = create_value_item_array(*itemnum);
    ldb_list_iterator_t *valiterator = ldb_list_iterator_create(vallist);
    ldb_list_iterator_t *metiterator = ldb_list_iterator_create(metalist);
    size_t now = 0;
    while(1){
        ldb_list_node_t* node_val = ldb_list_next(&valiterator);
        if(node_val == NULL){
            retval = LDB_OK;
            break;
        }
        ldb_list_node_t* node_met = ldb_list_next(&metiterator);
        assert(node_met != NULL);
        size_t val_len = ldb_slice_size((ldb_slice_t*)node_val->data_);
        (*items)[now].data_len_ = val_len;
        (*items)[now].data_ = malloc_and_copy(ldb_slice_data((ldb_slice_t*)node_val->data_), val_len);
        (*items)[now].version_ = node_met->value_;
        ++now;
    }
    ldb_list_iterator_destroy(valiterator);
    ldb_list_iterator_destroy(metiterator);

end:
    ldb_slice_destroy(slice_name);
    ldb_list_destroy(vallist);
    ldb_list_destroy(metalist);
    return retval;
}

int ldb_hget(ldb_context_t* context,
             GoByteSlice* name,
             GoByteSlice* key,
             value_item_t** items){
    int retval = 0;
    ldb_slice_t* slice_name = ldb_slice_create(name->data, name->data_len);
    ldb_slice_t* slice_key = ldb_slice_create(key->data, key->data_len);
    ldb_slice_t* slice_val = NULL;
    ldb_meta_t* meta = NULL;
    retval = hash_get(context, slice_name, slice_key, &slice_val, &meta);
    if(retval == LDB_OK){
        *items = create_value_item_array(1);
        (*items)[0].data_len_ = ldb_slice_size(slice_val);
        (*items)[0].data_ = malloc_and_copy(ldb_slice_data(slice_val) ,ldb_slice_size(slice_val));
        (*items)[0].version_ = ldb_meta_nextver(meta);
    }

    ldb_slice_destroy(slice_name);
    ldb_slice_destroy(slice_key);
    ldb_slice_destroy(slice_val);
    ldb_meta_destroy(meta);
    return retval;
}


int ldb_hmget(ldb_context_t* context,
              char* name,
              size_t namelen,
              value_item_t* keys,
              size_t keynum,
              value_item_t** items,
              size_t* itemnum){
    int retval = 0;
    ldb_slice_t *slice_name = ldb_slice_create(name, namelen);
    ldb_list_t *keylist, *vallist, *metalist = NULL;
    keylist = ldb_list_create();
    for( size_t i=0; i<keynum;){ 
        //push key
        ldb_slice_t *slice_key = ldb_slice_create(keys[i].data_, keys[i].data_len_);
        ldb_list_node_t *node_key = ldb_list_node_create();
        node_key->type_ = LDB_LIST_NODE_TYPE_SLICE;
        node_key->data_ = slice_key;;
        rpush_ldb_list_node(keylist, node_key);
        ++i;
    }
    retval = hash_mget(context, slice_name, keylist, &vallist, &metalist);
    if(retval != LDB_OK){
        goto end;
    }

    *itemnum = vallist->length_;
    *items = create_value_item_array(*itemnum);

    ldb_list_iterator_t *valiterator = ldb_list_iterator_create(vallist);
    ldb_list_iterator_t *metiterator = ldb_list_iterator_create(metalist);
    size_t now = 0;
    while(1){
        ldb_list_node_t* node_val = ldb_list_next(&valiterator);
        if(node_val == NULL){
            retval = LDB_OK;
            break;
        }
        ldb_list_node_t* node_met = ldb_list_next(&metiterator);
        assert(node_met == NULL);
        if( node_val->type_ == LDB_LIST_NODE_TYPE_NONE ){
            (*items)[now].data_len_ = 0;
            (*items)[now].data_ = NULL;
            (*items)[now].version_ = 0;
        }else{
            size_t data_len = ldb_slice_size((ldb_slice_t*)node_val->data_);
            (*items)[now].data_len_ = data_len;
            char* data = malloc_and_copy(ldb_slice_data((ldb_slice_t*)node_val->data_),  data_len);
            (*items)[now].data_ = data;
            (*items)[now].version_ = node_met->value_;
        }
        ++now;
    }
    ldb_list_iterator_destroy(valiterator);
    ldb_list_iterator_destroy(metiterator);


end:
    ldb_slice_destroy(slice_name);
    ldb_list_destroy(keylist);
    ldb_list_destroy(vallist);
    ldb_list_destroy(metalist);
    return retval;
}


int ldb_hincrby(ldb_context_t* context,
                char* name,
                size_t namelen,
                uint64_t lastver,
                int vercare,
                value_item_t* item,
                int64_t by,
                int64_t* result){
    int retval = 0;                    
    ldb_slice_t *slice_name = ldb_slice_create(name, namelen);
    ldb_slice_t *slice_key = ldb_slice_create(item->data_, item->data_len_);
    ldb_meta_t *meta = ldb_meta_create(vercare, lastver, item->version_);
    retval = hash_incr(context, slice_name, slice_key, meta, by, result);

    ldb_slice_destroy(slice_name);
    ldb_slice_destroy(slice_key);
    ldb_meta_destroy(meta);
    return retval;
}


int ldb_hset(ldb_context_t* context,
             char* name,
             size_t namelen,
             uint64_t lastver,
             int vercare,
             char* key,
             size_t keylen,
             value_item_t* item){
    int retval = 0;
    ldb_slice_t *slice_name = ldb_slice_create(name, namelen);
    ldb_slice_t *slice_key = ldb_slice_create(key, keylen);
    ldb_slice_t *slice_val = ldb_slice_create(item->data_, item->data_len_);
    ldb_meta_t *meta = ldb_meta_create(vercare, lastver, item->version_);

    retval = hash_set(context, slice_name, slice_key, slice_val, meta);
    if(retval != LDB_OK){
        goto end;                
    }

end:
    ldb_slice_destroy(slice_name);
    ldb_slice_destroy(slice_key);
    ldb_slice_destroy(slice_val);
    ldb_meta_destroy(meta);
    return retval;
}


int ldb_hmset(ldb_context_t* context,
              char* name,
              size_t namelen,
              uint64_t lastver,
              int vercare,
              value_item_t* items,
              size_t itemnum,
              int** results){

    int retval = 0;
    ldb_list_t *datalist, *metalist, *retlist = NULL;
    datalist = ldb_list_create();
    metalist = ldb_list_create();
    for( size_t i=0; i<itemnum;){
        //push key
        ldb_slice_t *slice_key = ldb_slice_create(items[i].data_, items[i].data_len_);
        ldb_list_node_t *node_key = ldb_list_node_create();
        node_key->type_ = LDB_LIST_NODE_TYPE_SLICE;
        node_key->data_ = slice_key;;
        rpush_ldb_list_node(datalist, node_key);
        ++i;

        //push value
        ldb_slice_t *slice_val = ldb_slice_create(items[i].data_, items[i].data_len_);
        ldb_list_node_t *node_val = ldb_list_node_create();
        node_val->type_ = LDB_LIST_NODE_TYPE_SLICE;
        node_val->data_ = slice_val;;
        rpush_ldb_list_node(datalist, node_val);

        //push meta
        ldb_meta_t *meta = ldb_meta_create(vercare, lastver, items[i].version_);
        ldb_list_node_t *node_meta = ldb_list_node_create();
        node_meta->type_ = LDB_LIST_NODE_TYPE_META;
        node_meta->data_ = meta;;
        rpush_ldb_list_node(metalist, node_meta);
        ++i;
  }
  ldb_slice_t *slice_name = ldb_slice_create(name, namelen);

  retval = hash_mset(context, slice_name, datalist, metalist, &retlist);  
  if(retval != LDB_OK){
    goto end;
  }

  ldb_list_iterator_t *iterator = ldb_list_iterator_create(retlist);
  *results = lmalloc(itemnum * sizeof(int));
  size_t now = 0;
  while(1){
    ldb_list_node_t *node = ldb_list_next(&iterator);
    if(node == NULL){
      break;
    }
    (*results)[now] = (int)(node->value_);
    ++now;
  }
  ldb_list_iterator_destroy(iterator);
  
end:
  ldb_slice_destroy(slice_name);
  ldb_list_destroy(datalist);
  ldb_list_destroy(metalist);
  ldb_list_destroy(retlist);
  return retval;
}


int ldb_hdel(ldb_context_t* context,
             char* name,
             size_t namelen,
             uint64_t lastver,
             int vercare,
             value_item_t* items,
             size_t itemnum,
             int** results){
    int retval = 0;
    ldb_slice_t *slice_name = ldb_slice_create(name, namelen); 
    *results = lmalloc(itemnum * sizeof(int));
    for(size_t i=0; i<itemnum; ++i){
        ldb_slice_t *slice_key = ldb_slice_create(items[i].data_, items[i].data_len_);
        ldb_meta_t *meta = ldb_meta_create(vercare, lastver, items[i].version_);
        (*results)[i] = hash_del(context,slice_name, slice_key, meta);
        ldb_slice_destroy(slice_key);
        ldb_meta_destroy(meta);
    }
    retval = LDB_OK;

  ldb_slice_destroy(slice_name);
  return retval;
}


int ldb_hlen(ldb_context_t* context,
             char* name,
             size_t namelen,
             uint64_t* length){
    int retval = 0;
    ldb_slice_t* slice_name = ldb_slice_create(name, namelen);
    retval = hash_length(context, slice_name, length);

    ldb_slice_destroy(slice_name);
    return retval;
}


int ldb_hexists(ldb_context_t* context,
                char* name,
                size_t namelen,
                char* key,
                size_t keylen){
    int retval = 0;
    ldb_slice_t* slice_name = ldb_slice_create(name, namelen);
    ldb_slice_t* slice_key = ldb_slice_create(key, keylen);
    retval = hash_exists(context, slice_name, slice_key);

    ldb_slice_destroy(slice_name);
    ldb_slice_destroy(slice_key);
    return retval;
}

int ldb_sadd(ldb_context_t* context,
                char* name,
                size_t namelen,
                uint64_t version,
                int vercare,
                value_item_t* keys,
                size_t keynum,
                int **results){
  int retval = 0;
  ldb_slice_t *slice_name = ldb_slice_create(name, namelen);
  *results = (int*)lmalloc(sizeof(int) * keynum);
  size_t now = 0; 
  while(now < keynum){
    ldb_meta_t *meta = ldb_meta_create(vercare, version, keys[now].version_);
    ldb_slice_t *slice_key = ldb_slice_create(keys[now].data_, keys[now].data_len_);
    (*results)[now] = set_add(context, slice_name, slice_key, meta);
    ldb_meta_destroy(meta);
    ldb_slice_destroy(slice_key);
    ++now;
  }
  retval = LDB_OK;

  ldb_slice_destroy(slice_name);
  return retval;
}

int ldb_smembers(ldb_context_t* context,
                 char* name,
                 size_t namelen,
                 value_item_t** items,
                 size_t* itemnum){

    int retval = 0;
    ldb_slice_t *slice_name = ldb_slice_create(name, namelen);
    ldb_list_t *keylist, *metalist = NULL;
    retval = set_members(context, slice_name, &keylist, &metalist);
    if( retval != LDB_OK ){
        goto end;
    }
    *itemnum = keylist->length_;
    *items = create_value_item_array(*itemnum);
    ldb_list_iterator_t *keyiterator = ldb_list_iterator_create(keylist);
    ldb_list_iterator_t *metiterator = ldb_list_iterator_create(metalist);
    size_t now = 0;
    while(1){
        ldb_list_node_t* node_key = ldb_list_next(&keyiterator);
        if(node_key == NULL){
            retval = LDB_OK;
            break;
        }
        ldb_list_node_t* node_met = ldb_list_next(&metiterator);
        assert(node_met != NULL);
        size_t key_len = ldb_slice_size((ldb_slice_t*)node_key->data_);
        (*items)[now].data_len_ = key_len;
        (*items)[now].data_ = malloc_and_copy(ldb_slice_data((ldb_slice_t*)node_key->data_), key_len);
        (*items)[now].version_ = node_met->value_;
        ++now;
    }
    ldb_list_iterator_destroy(keyiterator);
    ldb_list_iterator_destroy(metiterator);

end:
    ldb_slice_destroy(slice_name);
    ldb_list_destroy(keylist);
    ldb_list_destroy(metalist);
    return retval;
}

int ldb_spop(ldb_context_t* context,
             char* name,
             size_t namelen,
             uint64_t version,
             int vercare,
             value_item_t** items,
             uint64_t nextver){
    int retval = 0;
    ldb_slice_t *slice_name = ldb_slice_create(name, namelen);
    ldb_meta_t *meta = ldb_meta_create(vercare, version, nextver);
    ldb_slice_t *key = NULL;
    *items = create_value_item_array(1);
    retval = set_pop(context, slice_name, meta, &key);
    if( retval != LDB_OK ){
        goto end; 
    }
    size_t key_len = ldb_slice_size(key);
    (*items)[0].data_len_ = key_len;
    (*items)[0].data_ = malloc_and_copy(ldb_slice_data(key), key_len);
    (*items)[0].version_ = nextver;

end:
    ldb_slice_destroy(slice_name);
    ldb_slice_destroy(key);
    ldb_meta_destroy(meta);
    return retval;
}

int ldb_srem(ldb_context_t* context,
             char* name,
             size_t namelen,
             uint64_t version,
             int vercare,
             value_item_t* keys,
             size_t keynum,
             int **results){

  int retval = 0;
  ldb_slice_t *slice_name = ldb_slice_create(name, namelen);
  *results = (int*)lmalloc(sizeof(int) * keynum);
  size_t now = 0; 
  while(now < keynum){
    ldb_meta_t *meta = ldb_meta_create(vercare, version, keys[now].version_);
    ldb_slice_t *slice_key = ldb_slice_create(keys[now].data_, keys[now].data_len_);
    (*results)[now] = set_rem(context, slice_name, slice_key, meta);
    ldb_meta_destroy(meta);
    ldb_slice_destroy(slice_key);
    ++now;
  }
  retval = LDB_OK;

  ldb_slice_destroy(slice_name);
  return retval;
}

int ldb_scard(ldb_context_t* context,
              char* name,
              size_t namelen,
              uint64_t *count){
    int retval = 0;
    ldb_slice_t *slice_name = ldb_slice_create(name, namelen);
    retval = set_card(context, slice_name, count);

    ldb_slice_destroy(slice_name);
    return retval;
}

int ldb_sismember(ldb_context_t* context,
                  char* name,
                  size_t namelen,
                  char* key,
                  size_t keylen){
    int retval = 0;
    ldb_slice_t *slice_name = ldb_slice_create(name, namelen);
    ldb_slice_t *slice_key = ldb_slice_create(key, keylen);
    retval = set_ismember(context, slice_name, slice_key);

    ldb_slice_destroy(slice_name);
    ldb_slice_destroy(slice_key);
    return retval;
}

int ldb_zscore(ldb_context_t* context,
              char* name,
              size_t namelen,
              char* key,
              size_t keylen,
              int64_t* score){
  int retval = 0;
  ldb_slice_t *slice_name = ldb_slice_create(name, namelen);
  ldb_slice_t *slice_key = ldb_slice_create(key, keylen);

  retval = zset_get(context, slice_name, slice_key, score);  
  if(retval != LDB_OK){
    goto end;
  }

end:
  ldb_slice_destroy(slice_name);
  ldb_slice_destroy(slice_key);
  return retval;
}

int ldb_zrange_by_rank(ldb_context_t* context,
                       char* name,
                       size_t namelen,
                       int rank_start,
                       int rank_end,
                       value_item_t** items,
                       size_t* itemnum,
                       int64_t** scores,
                       size_t* scorenum,
                       int reverse,
                       int withscore){
  int retval = 0;
  ldb_slice_t *slice_name = ldb_slice_create(name, namelen);
  ldb_list_t *keylist, *metlist = NULL;
  retval = zset_range(context, slice_name, rank_start, rank_end, reverse, &keylist, &metlist);
  if(retval != LDB_OK){
    goto end;
  }
  if(keylist->length_ == 0){
    goto end;
  }
  *itemnum = keylist->length_;
  *items = lmalloc( sizeof(value_item_t) * (*itemnum));
  if(withscore){
    *scorenum = keylist->length_;
    *scores = lmalloc(sizeof(int64_t) * (*scorenum));
  }
  
  ldb_list_iterator_t *keyiterator = ldb_list_iterator_create(keylist);
  ldb_list_iterator_t *metiterator = ldb_list_iterator_create(metlist);
  int now = 0;
  while(1){
    ldb_list_node_t* node_key = ldb_list_next(&keyiterator);
    if(node_key == NULL){
      retval = LDB_OK;
      break;
    }
    ldb_list_node_t* node_met = ldb_list_next(&metiterator);
    if(node_met == NULL){
      retval = LDB_OK;
      break;
    }
    if(node_key->type_ == LDB_LIST_NODE_TYPE_SLICE){
      size_t data_len = ldb_slice_size((ldb_slice_t*)(node_key->data_));
      (*items)[now].data_len_ = data_len;
      (*items)[now].data_ = malloc_and_copy(ldb_slice_data((ldb_slice_t*)(node_key->data_)), data_len);
      if(withscore){
        (*scores)[now] = node_key->value_;
      }
    }
    if(node_met->type_ == LDB_LIST_NODE_TYPE_BASE){
      (*items)[now].version_ =node_met->value_;
    }
    ++now;
  }
  ldb_list_iterator_destroy(keyiterator);
  ldb_list_iterator_destroy(metiterator);
  retval = LDB_OK;

end:
  ldb_list_destroy(keylist);
  ldb_list_destroy(metlist);
  ldb_slice_destroy(slice_name);
  return retval; 
}

int ldb_zrange_by_score(ldb_context_t* context,
                        char* name,
                        size_t namelen,
                        int64_t score_start,
                        int64_t score_end,
                        value_item_t** items,
                        size_t* itemnum,
                        int64_t** scores,
                        size_t* scorenum,
                        int reverse,
                        int withscore){

  int retval = 0;
  ldb_slice_t *slice_name = ldb_slice_create(name, namelen);
  ldb_list_t *keylist, *metlist = NULL;
  retval = zset_scan(context, slice_name, score_start, score_end, reverse, &keylist, &metlist);
  if(retval != LDB_OK){
    goto end;
  }
  if(keylist->length_ == 0){
    goto end;
  }
  *itemnum = keylist->length_;
  *items = lmalloc( sizeof(value_item_t) * (*itemnum));
  if(withscore){
    *scorenum = keylist->length_;
    *scores = lmalloc(sizeof(int64_t) * (*scorenum));
  }
  
  ldb_list_iterator_t *keyiterator = ldb_list_iterator_create(keylist);
  ldb_list_iterator_t *metiterator = ldb_list_iterator_create(metlist);
  int now = 0;
  while(1){
    ldb_list_node_t* node_key = ldb_list_next(&keyiterator);
    if(node_key == NULL){
      retval = LDB_OK;
      break;
    }
    ldb_list_node_t* node_met = ldb_list_next(&metiterator);
    if(node_met == NULL){
      retval = LDB_OK;
      break;
    }
    if(node_key->type_ == LDB_LIST_NODE_TYPE_SLICE){
      size_t data_len = ldb_slice_size((ldb_slice_t*)(node_key->data_));
      (*items)[now].data_len_ = data_len;
      (*items)[now].data_ = malloc_and_copy(ldb_slice_data((ldb_slice_t*)(node_key->data_)), data_len);
      if(withscore){
        (*scores)[now] = node_key->value_;
      }
    }
    if(node_met->type_ == LDB_LIST_NODE_TYPE_BASE){
      (*items)[now].version_ =node_met->value_;
    }
    ++now;
  }
  ldb_list_iterator_destroy(keyiterator);
  ldb_list_iterator_destroy(metiterator);
  retval = LDB_OK;

end:
  ldb_list_destroy(keylist);
  ldb_list_destroy(metlist);
  ldb_slice_destroy(slice_name);
  return retval; 
}

int ldb_zadd(ldb_context_t* context,
             char* name,
             size_t namelen,
             uint64_t version,
             int vercare,
             value_item_t* keys,
             int64_t* scores,
             size_t keynum,
             int** results){
  int retval = 0;
  ldb_slice_t *slice_name = ldb_slice_create(name, namelen);
  *results = (int*)lmalloc(sizeof(int) * keynum);
  size_t now = 0; 
  while(now < keynum){
    ldb_meta_t *meta = ldb_meta_create(vercare, version, keys[now].version_);
    ldb_slice_t *slice_key = ldb_slice_create(keys[now].data_, keys[now].data_len_);
    (*results)[now] = zset_add(context, slice_name, slice_key, meta, scores[now]);
    ldb_meta_destroy(meta);
    ldb_slice_destroy(slice_key);
    ++now;
  }
  retval = LDB_OK;

  ldb_slice_destroy(slice_name);
  return retval;
}

int ldb_zrank(ldb_context_t* context,
              char* name,
              size_t namelen,
              char* key,
              size_t keylen,
              int reverse,
              uint64_t* rank){
  int retval = 0;
  ldb_slice_t *slice_name, *slice_key = NULL;
  slice_name = ldb_slice_create(name, namelen);
  slice_key = ldb_slice_create(key, keylen);
  retval = zset_rank(context, slice_name, slice_key, reverse, rank);
  if(retval != LDB_OK){
    goto end;
  }

end:
  ldb_slice_destroy(slice_name);
  return retval;
}

int ldb_zcount(ldb_context_t* context,
               char* name,
               size_t namelen,
               int64_t score_start,
               int64_t score_end,
               uint64_t* count){
  int retval = 0;
  ldb_slice_t *slice_name = ldb_slice_create(name, namelen);
  retval = zset_count(context, slice_name, score_start, score_end, count); 
  if(retval != LDB_OK){
    goto end;
  }

end:
  ldb_slice_destroy(slice_name);
  return retval;
}

int ldb_zincrby(ldb_context_t* context,
                char* name,
                size_t namelen,
                uint64_t lastver,
                int vercare,
                value_item_t* item,
                int64_t by,
                int64_t* score){
  int retval = 0;
  ldb_slice_t* slice_name = ldb_slice_create(name, namelen);
  ldb_slice_t* slice_key = ldb_slice_create(item->data_, item->data_len_);
  ldb_meta_t* meta = ldb_meta_create(vercare, lastver, item->version_);
  retval = zset_incr(context, slice_name, slice_key, meta, by, score);  

  ldb_slice_destroy(slice_name);
  ldb_slice_destroy(slice_key);
  ldb_meta_destroy(meta);
  return retval;
}

int ldb_zrem(ldb_context_t* context,
             char* name,
             size_t namelen,
             uint64_t lastver,
             int vercare,
             value_item_t* items,
             size_t itemnum,
             int** retvals){
  int retval = 0;
  ldb_slice_t* slice_name = ldb_slice_create(name, namelen);
  *retvals = (int*)lmalloc(sizeof(int) * itemnum);
  size_t now = 0;
  for(; now< itemnum; ++now){
    ldb_slice_t* slice_key = ldb_slice_create(items[now].data_, items[now].data_len_);
    ldb_meta_t* meta = ldb_meta_create(vercare, lastver, items[now].version_);
    (*retvals)[now] = zset_del(context, slice_name, slice_key, meta);
    ldb_slice_destroy(slice_key);
    ldb_meta_destroy(meta);
  }

  ldb_slice_destroy(slice_name);
  return retval;
}

int ldb_zrem_by_rank(ldb_context_t* context,
                     char* name,
                     size_t namelen,
                     uint64_t nextver,
                     int vercare,
                     int rank_start,
                     int rank_end,
                     uint64_t* deleted){
  int retval = 0;
  ldb_slice_t* slice_name = ldb_slice_create(name, namelen);
  ldb_meta_t* meta = ldb_meta_create(vercare, 0, nextver);
  retval = zset_del_range_by_rank(context, slice_name, meta, rank_start, rank_end, deleted);

  ldb_slice_destroy(slice_name);
  ldb_meta_destroy(meta);
  return retval;
}

int ldb_zrem_by_score(ldb_context_t* context,
                      char* name,
                      size_t namelen,
                      uint64_t nextver,
                      int vercare,
                      int64_t score_start,
                      int64_t score_end,
                      uint64_t* deleted){
  int retval = 0;
  ldb_slice_t* slice_name = ldb_slice_create(name, namelen);
  ldb_meta_t* meta = ldb_meta_create(vercare, 0, nextver);
  retval = zset_del_range_by_score(context, slice_name, meta, score_start, score_end, deleted);

  ldb_slice_destroy(slice_name);
  ldb_meta_destroy(meta);
  return retval;
}

int ldb_zcard(ldb_context_t* context,
              char* name,
              size_t namelen,
              uint64_t* size){
  int retval = 0;
  ldb_slice_t *slice_name = ldb_slice_create(name, namelen);
  retval = zset_size(context, slice_name, size);
  if(retval == LDB_OK_NOT_EXIST){
    *size = 0;
    retval = LDB_OK;
  }
  
  ldb_slice_destroy(slice_name);
  return retval;
}
