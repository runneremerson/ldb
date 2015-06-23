#include "ldb_meta.h"
#include "lmalloc.h"

#include <leveldb-ldb/c.h>
#include <stdint.h>


struct ldb_meta_t{
  uint32_t vercare_;
  uint64_t lastver_;
  uint64_t nextver_; 
};

ldb_meta_t* ldb_meta_create(uint32_t vercare, uint64_t lastver, uint64_t nextver){
  ldb_meta_t* meta = (ldb_meta_t*)lmalloc(sizeof(ldb_meta_t)); 
  meta->vercare_ = vercare;
  meta->lastver_ = lastver;
  meta->nextver_ = nextver;
  return meta;
}

void ldb_meta_destroy(ldb_meta_t* meta){
  lfree(meta);
}


uint32_t ldb_meta_vercare(const ldb_meta_t* meta){
  return meta->vercare_;
}

uint64_t ldb_meta_lastver(const ldb_meta_t* meta){
  return meta->lastver_;
}

uint64_t ldb_meta_nextver(const ldb_meta_t* meta){
  return meta->nextver_;
}

ldb_slice_t* ldb_meta_slice_create(const ldb_meta_t* meta){
  char strvercare[sizeof(uint32_t)] = {0};
  char strlastver[sizeof(uint64_t)] = {0};
  char strnextver[sizeof(uint64_t)] = {0};
  if(meta != NULL){
    leveldb_encode_fixed32(strvercare, meta->vercare_);
    leveldb_encode_fixed64(strlastver, meta->lastver_);
    leveldb_encode_fixed64(strnextver, meta->nextver_); 
  }else{
    leveldb_encode_fixed32(strvercare, 0);
    leveldb_encode_fixed64(strlastver, 0);
    leveldb_encode_fixed64(strnextver, 0); 
  }
  ldb_slice_t *slice = ldb_slice_create(strvercare, sizeof(uint32_t));
  ldb_slice_push_back(slice, strlastver, sizeof(uint64_t));
  ldb_slice_push_back(slice, strnextver, sizeof(uint64_t));
  return slice;
}

void ldb_meta_encode(char* buf, uint32_t vercare, uint64_t lastver, uint64_t nextver){
  leveldb_encode_fixed32(buf, vercare);
  buf += sizeof(uint32_t);
  leveldb_encode_fixed64(buf, lastver);
  buf += sizeof(uint64_t);
  leveldb_encode_fixed64(buf, nextver);
}
