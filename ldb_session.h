#ifndef LDB_SESSION_H
#define LDB_SESSION_H

#include "trace.h"
#include "config.h"
#include "ldb_context.h"
#include "ldb_slice.h"

#include "t_kv.h"

#include <leveldb-ldb/c.h>
#include <string.h>
#include <stdint.h>
#include <assert.h>


typedef struct value_item{
    uint64_t version_;
    size_t data_len_;
    char* data_;
} value_item_t;

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
}

static void create_ldb_slice_key(ldb_slice_t** key, const char* data, size_t size){
  *key = ldb_slice_create(data, size);
}

static void create_ldb_slice_val(ldb_slice_t** val, uint32_t vercare, uint64_t lastver, uint64_t nextver, const char* data, size_t size){
  char struint32[sizeof(uint32_t)] = {0};
  char struint64[sizeof(uint64_t)] = {0};
  leveldb_encode_fixed32(struint32, vercare);
  leveldb_encode_fixed64(struint64, lastver);
  *val = ldb_slice_create(struint32, sizeof(uint32_t));
  ldb_slice_push_back(*val, struint64, sizeof(uint64_t));
  leveldb_encode_fixed64(struint64, nextver); 
  ldb_slice_push_back(*val, struint64, sizeof(uint64_t));
  ldb_slice_push_back(*val, data, size);
}

//string
int ldb_set(ldb_context_t* context, uint32_t area, char* key, size_t keylen, uint64_t lastver, int vercare, long exptime, value_item_t* item, int en);
int ldb_get(ldb_context_t* context, uint32_t area, char* key, size_t keylen, value_item_t** item, uint16_t *retver);





int ldb_set(ldb_context_t* context, uint32_t area, char* key, size_t keylen, uint64_t lastver, int vercare, long exptime, value_item_t* item, int en){
  assert(key!=NULL);
  int retval = 0;
  ldb_slice_t *slice_key, *slice_val , *slice_value = NULL;
  create_ldb_slice_key(&slice_key, key, keylen);
  create_ldb_slice_val(&slice_val, vercare, lastver, item->version_, item->data_, item->data_len_);
  if(en == IS_NOT_EXIST){
    if(string_get(context, slice_key, &slice_value)){
      string_set(context, slice_key, slice_val);
    }
    goto end;
  } else if(en == IS_EXIST){
    if(!string_get(context, slice_key, &slice_value)){
      string_set(context, slice_key, slice_val);  
    }
    goto end;
  } else if(en == IS_EXIST_AND_EXPIRE){
    goto end;
  } else if(en == IS_NOT_EXIST_AND_EXPIRE){
    goto end;
  }
end:
  ldb_slice_destroy(slice_key);
  ldb_slice_destroy(slice_val);
  ldb_slice_destroy(slice_value);

  return retval;
}

int ldb_get(ldb_context_t* context, uint32_t area, char* key, size_t keylen, value_item_t** item, uint16_t *retver){
  assert(key!=NULL);
  int retval = 0;
  ldb_slice_t *slice_key, *slice_val = NULL;
  create_ldb_slice_key(&slice_key, key, keylen);
  if(!string_get(context, slice_key, &slice_val)){
    goto end;
  }
end:
  ldb_slice_destroy(slice_key);
  ldb_slice_destroy(slice_val);
  return retval;
}
#endif //LDB_SESSION_H
