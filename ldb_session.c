#include "ldb_session.h"
#include "ldb_context.h"
#include "ldb_slice.h"
#include "ldb_meta.h"
#include "ldb_define.h"
#include "trace.h"
#include "config.h"
#include "lmalloc.h"
#include "cgo_util/base.h"

#include "t_kv.h"

#include <leveldb-ldb/c.h>
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

static int decode_slice_value(const ldb_slice_t* slice_val, value_item_t* item){
  if(ldb_slice_size(slice_val) <  sizeof(uint64_t) ){
    return -1;
  }
  uint64_t version = leveldb_decode_fixed64(ldb_slice_data(slice_val));
  const char* data = ldb_slice_data(slice_val) + sizeof(uint64_t);
  size_t size = ldb_slice_size(slice_val)-sizeof(uint64_t);
  fill_value_item(item, version, data, size); 
  return 0;
}

static char* malloc_and_copy(const ldb_slice_t* slice_val){
  size_t size = ldb_slice_size(slice_val);
  char* result = lmalloc(size);
  memcpy(result, ldb_slice_data(slice_val), size); 
  return result;
}



int ldb_set(ldb_context_t* context, 
            uint32_t area, 
            char* key, 
            size_t keylen, 
            uint64_t lastver, 
            int vercare, 
            long exptime, 
            value_item_t* item, 
            int en){
  int retval = 0;
  ldb_slice_t *slice_key, *slice_val , *slice_value = NULL;
  slice_key = ldb_slice_create(key, keylen);
  slice_val = ldb_slice_create(item->data_, item->data_len_);
  ldb_meta_t *meta = ldb_meta_create(vercare, lastver, item->version_);
  if(en == IS_NOT_EXIST){
    retval = string_setnx(context, slice_key, slice_val, meta);
  } else if(en == IS_EXIST){
    retval = string_set(context, slice_key, slice_val, meta);
  } else if(en == IS_EXIST_AND_EXPIRE){
  } else if(en == IS_NOT_EXIST_AND_EXPIRE){
  }

end:
  ldb_slice_destroy(slice_key);
  ldb_slice_destroy(slice_val);
  ldb_slice_destroy(slice_value);
  ldb_meta_destroy(meta);

  return retval;
}


int ldb_mset(ldb_context_t* context, 
             uint32_t area, 
             uint64_t lastver, 
             int vercare, 
             long exptime, 
             GoByteSlice* keyvals, 
             GoUint64Slice* versions, 
             int length,
             GoUint64Slice* retvals,
             int en){
  int retval = 0;
  ldb_list_t *datalist, *metalist, *retlist = NULL;
  datalist = ldb_list_create();
  metalist = ldb_list_create();
  for( int i=0, v=0; i<length;){
    //push key
    ldb_slice_t *slice_key = ldb_slice_create(keyvals[i].data, keyvals[i].data_len);
    ldb_list_node_t *node_key = ldb_list_node_create();
    node_key->type_ = LDB_LIST_NODE_TYPE_SLICE;
    node_key->data_ = slice_key;;
    lpush_ldb_list_node(datalist, node_key);
    ++i;

    //push value
    ldb_slice_t *slice_val = ldb_slice_create(keyvals[i].data, keyvals[i].data_len);
    ldb_list_node_t *node_val = ldb_list_node_create();
    node_val->type_ = LDB_LIST_NODE_TYPE_SLICE;
    node_val->data_ = slice_val;;
    lpush_ldb_list_node(datalist, node_val);
    ++i;

    //push meta
    ldb_meta_t *meta = ldb_meta_create(vercare, lastver, versions->data[v]);
    ldb_list_node_t *node_meta = ldb_list_node_create();
    node_meta->type_ = LDB_LIST_NODE_TYPE_META;
    node_meta->data_ = meta;;
    lpush_ldb_list_node(metalist, node_meta);
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
  int now = 0;
  while(1){
    ldb_list_node_t *node = ldb_list_next(&iterator);
    if(node == NULL){
      break;
    }
    retvals->data[now] = (int)(node->value_);
    ++now;
  }
  
end:
  ldb_list_destroy(datalist);
  ldb_list_destroy(metalist);
  ldb_list_destroy(retlist);
  ldb_list_iterator_destroy(iterator);
  return retval;
}

int ldb_get(ldb_context_t* context, 
            uint32_t area, 
            char* key, 
            size_t keylen, 
            value_item_t** item){
  int retval = 0;
  ldb_slice_t *slice_key, *slice_val = NULL;
  slice_key = ldb_slice_create(key, keylen);
  retval = string_get(context, slice_key, &slice_val);
  if(retval != LDB_OK){
    goto end;
  }
  *item = create_value_item_array(1);
  if(decode_slice_value(slice_val, *item)){
    retval = LDB_ERR;
    goto end;
  }
  retval = LDB_OK; 
end:
  ldb_slice_destroy(slice_key);
  ldb_slice_destroy(slice_val);

  return retval;
}


int ldb_mget(ldb_context_t* context,
             uint32_t area,
             GoByteSlice* slice,
             int length,
             GoByteSliceSlice* items,
             GoUint64Slice* versions,
             int* number){
  int retval = 0;
  ldb_list_t *keylist, *vallist, *metalist = NULL;
  keylist = ldb_list_create();
  for( int i=0; i<length;){
    //push key
    ldb_slice_t *slice_key = ldb_slice_create(slice[i].data, slice[i].data_len);
    ldb_list_node_t *node_key = ldb_list_node_create();
    node_key->type_ = LDB_LIST_NODE_TYPE_SLICE;
    node_key->data_ = slice_key;;
    lpush_ldb_list_node(keylist, node_key);
    ++i;
  }
  retval = string_mget(context, keylist, &vallist, &metalist);
  if(retval != LDB_OK){
    goto end;
  }
  ldb_list_iterator_t *valiterator = ldb_list_iterator_create(vallist);
  ldb_list_iterator_t *metiterator = ldb_list_iterator_create(metalist);
  int now = 0;
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
      items->data[now].data_len = ldb_slice_size((ldb_slice_t*)node_val->data_);
      items->data[now].data = malloc_and_copy((ldb_slice_t*)node_val->data_);
    }
    if( node_met->type_ == LDB_LIST_NODE_TYPE_NONE ){
      versions->data[now] = 0;
    }else{
      versions->data[now] = node_met->value_;
    }
    items->data[now].cap = items->data[now].data_len;
    ++now;
  }

end:
  ldb_list_destroy(keylist);
  ldb_list_destroy(vallist);
  ldb_list_destroy(metalist);
  ldb_list_iterator_destroy(valiterator);
  ldb_list_iterator_destroy(metiterator);
  return retval;
}

int ldb_del(ldb_context_t* context, 
            uint32_t area, 
            char* key, 
            size_t keylen, 
            int vercare, 
            uint64_t version){
  int retval = 0;
  ldb_slice_t *slice_key = ldb_slice_create(key, keylen);
  ldb_meta_t *meta = ldb_meta_create(vercare, 0, version);
  retval = string_del(context, slice_key, meta);

end:
  ldb_slice_destroy(slice_key);
  ldb_meta_destroy(meta);
  return retval;
}

int ldb_incrby(ldb_context_t* context,
               uint32_t area,
               char* key,
               size_t keylen,
               uint64_t lastver,
               int vercare,
               long exptime,
               uint64_t version,
               int64_t initval,
               int64_t by,
               int64_t* result){
  int retval = 0;
  ldb_slice_t *slice_key = ldb_slice_create(key, keylen);
  ldb_meta_t *meta = ldb_meta_create(vercare, lastver, version);
  retval = string_incr(context, slice_key, meta, initval, by, result);

end:
  ldb_slice_destroy(slice_key);
  return retval;
}