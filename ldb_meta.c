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
  char struint32[sizeof(uint32_t)] = {0};
  char struint64[sizeof(uint64_t)] = {0};
  leveldb_encode_fixed32(struint32, meta->vercare_);
  leveldb_encode_fixed64(struint64, meta->lastver_);
  ldb_slice_t *slice = ldb_slice_create(struint32, sizeof(uint32_t));
  ldb_slice_push_back(slice, struint64, sizeof(uint64_t));
  leveldb_encode_fixed64(struint64, meta->nextver_); 
  ldb_slice_push_back(slice, struint64, sizeof(uint64_t));
  return slice;
}
