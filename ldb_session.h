#ifndef LDB_SESSION_H
#define LDB_SESSION_H

#include "trace.h"
#include "config.h"
#include "lmalloc.h"
#include "ldb_context.h"
#include "ldb_slice.h"
#include "ldb_meta.h"
#include "ldb_define.h"

#include "t_kv.h"

#include <leveldb-ldb/c.h>
#include <string.h>
#include <stdint.h>
#include <assert.h>


typedef struct value_item_t{
  uint64_t version_;
  size_t data_len_;
  char* data_;
} value_item_t;

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

int decode_slice_value(const ldb_slice_t* slice_val, value_item_t* item){
  if(ldb_slice_size(slice_val) < sizeof(uint64_t)){
    return -1;
  }
  uint64_t version = leveldb_decode_fixed64(ldb_slice_data(slice_val));
  const char* data = ldb_slice_data(slice_val) + sizeof(uint64_t);
  size_t size = ldb_slice_size(slice_val)-sizeof(uint64_t);
  fill_value_item(item, version, data, size); 
  return 0;
}



//string
int ldb_set(ldb_context_t* context, uint32_t area, char* key, size_t keylen, uint64_t lastver, int vercare, long exptime, value_item_t* item, int en);
int ldb_get(ldb_context_t* context, uint32_t area, char* key, size_t keylen, value_item_t** item);

//hash

//zset





int ldb_set(ldb_context_t* context, uint32_t area, char* key, size_t keylen, uint64_t lastver, int vercare, long exptime, value_item_t* item, int en){
  assert(key!=NULL);
  int retval = 0;
  ldb_slice_t *slice_key, *slice_val , *slice_value = NULL;
  slice_key = ldb_slice_create(key, keylen);
  slice_val = ldb_slice_create(item->data_, item->data_len_);
  ldb_meta_t *meta = ldb_meta_create(vercare, lastver, item->version_);
  if(en == IS_NOT_EXIST){
    retval = string_setnx(context, slice_key, slice_val, meta);
  } else if(en == IS_EXIST){
    retval = string_setxx(context, slice_key, slice_val, meta);
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

int ldb_get(ldb_context_t* context, uint32_t area, char* key, size_t keylen, value_item_t** item){
  assert(key!=NULL);
  int retval = 0;
  ldb_slice_t *slice_key, *slice_val = NULL;
  slice_key = ldb_slice_create(key, keylen);
  if(string_get(context, slice_key, &slice_val)==-1){
    retval = LDB_OK_NOT_EXIST;
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
#endif //LDB_SESSION_H
