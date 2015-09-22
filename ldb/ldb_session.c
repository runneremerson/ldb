#include "ldb_session.h"
#include "ldb_context.h"
#include "ldb_slice.h"
#include "ldb_meta.h"
#include "ldb_define.h"
#include "ldb_list.h"
#include "trace.h"
#include "config.h"
#include "lmalloc.h"
#include "util/cgo_util_base.h"

#include "t_kv.h"
#include "t_zset.h"

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
            char* key, 
            size_t keylen, 
            value_item_t* item){
  int retval = 0;
  ldb_slice_t *slice_key, *slice_val = NULL;
  slice_key = ldb_slice_create(key, keylen);
  retval = string_get(context, slice_key, &slice_val);
  if(retval != LDB_OK){
    goto end;
  }
  if(decode_slice_value(slice_val, item)){
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
      (*items)[now].data_ = malloc_and_copy((ldb_slice_t*)(node_key->data_));
      (*items)[now].data_len_ = ldb_slice_size((ldb_slice_t*)(node_key->data_));
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
      (*items)[now].data_ = malloc_and_copy((ldb_slice_t*)(node_key->data_));
      (*items)[now].data_len_ = ldb_slice_size((ldb_slice_t*)(node_key->data_));
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
             uint64_t lastver,
             int vercare,
             long exptime,
             value_item_t* keys,
             int64_t* scores,
             size_t keynum,
             int** retvals){
  int retval = 0;
  ldb_slice_t *slice_name = ldb_slice_create(name, namelen);
  *retvals = (int*)lmalloc(sizeof(int) * keynum);
  size_t now = 0; 
  while(now < keynum){
    ldb_meta_t *meta = ldb_meta_create(vercare, lastver, keys[now].version_);
    ldb_slice_t *slice_key = ldb_slice_create(keys[now].data_, keys[now].data_len_);
    (*retvals)[now] = zset_add(context, slice_name, slice_key, meta, scores[now]);
    ldb_meta_destroy(meta);
    ldb_slice_destroy(slice_key);
    ++now;
  }
  retval = LDB_OK;

end:
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
                long exptime,
                value_item_t* item,
                int64_t by,
                int64_t* score){
  int retval = 0;
  ldb_slice_t* slice_name = ldb_slice_create(name, namelen);
  ldb_slice_t* slice_key = ldb_slice_create(item->data_, item->data_len_);
  ldb_meta_t* meta = ldb_meta_create(vercare, lastver, item->version_);
  retval = zset_incr(context, slice_name, slice_key, meta, by, score);  

end:
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
             long exptime,
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

end:
  ldb_slice_destroy(slice_name);
  return retval;
}

int ldb_zrem_by_rank(ldb_context_t* context,
                     char* name,
                     size_t namelen,
                     uint64_t nextver,
                     int vercare,
                     long exptime,
                     int rank_start,
                     int rank_end,
                     uint64_t* deleted){
  int retval = 0;
  ldb_slice_t* slice_name = ldb_slice_create(name, namelen);
  ldb_meta_t* meta = ldb_meta_create(vercare, 0, nextver);
  retval = zset_del_range_by_rank(context, slice_name, meta, rank_start, rank_end, deleted);

end:
  ldb_slice_destroy(slice_name);
  ldb_meta_destroy(meta);
  return retval;
}

int ldb_zrem_by_score(ldb_context_t* context,
                      char* name,
                      size_t namelen,
                      uint64_t nextver,
                      int vercare,
                      long exptime,
                      int64_t score_start,
                      int64_t score_end,
                      uint64_t* deleted){
  int retval = 0;
  ldb_slice_t* slice_name = ldb_slice_create(name, namelen);
  ldb_meta_t* meta = ldb_meta_create(vercare, 0, nextver);
  retval = zset_del_range_by_score(context, slice_name, meta, score_start, score_end, deleted);

end:
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
  
end:
  ldb_slice_destroy(slice_name);
  return retval;
}
