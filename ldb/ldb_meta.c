#include "ldb_meta.h"
#include "lmalloc.h"

#include <leveldb/c.h>
#include <stdint.h>


struct ldb_meta_t{
  uint32_t vercare_;
  uint64_t lastver_;
  uint64_t nextver_; 
  uint64_t exptime_;
};

ldb_meta_t* ldb_meta_create(uint32_t vercare, uint64_t lastver, uint64_t nextver){
  ldb_meta_t* meta = (ldb_meta_t*)lmalloc(sizeof(ldb_meta_t)); 
  meta->vercare_ = vercare;
  meta->lastver_ = lastver;
  meta->nextver_ = nextver;
  meta->exptime_ = 0;
  return meta;
}


ldb_meta_t* ldb_meta_create_with_exp(uint32_t vercare, uint64_t lastver, uint64_t nextver, uint64_t exptime){
  ldb_meta_t* meta = (ldb_meta_t*)lmalloc(sizeof(ldb_meta_t)); 
  meta->vercare_ = vercare;
  meta->lastver_ = lastver;
  meta->nextver_ = nextver;
  meta->exptime_ = exptime;
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

uint64_t ldb_meta_exptime(const ldb_meta_t* meta){
  return meta->exptime_;
}

ldb_slice_t* ldb_meta_slice_create(const ldb_meta_t* meta){
  char strmeta[sizeof(uint32_t) + sizeof(uint64_t) + sizeof(uint64_t) + sizeof(uint64_t)] = {0};
  char *buf = strmeta;
  if(meta != NULL){
    leveldb_encode_fixed32(buf, meta->vercare_);
    buf += sizeof(uint32_t);
    leveldb_encode_fixed64(buf, meta->lastver_);
    buf += sizeof(uint64_t);
    leveldb_encode_fixed64(buf, meta->nextver_); 
    buf += sizeof(uint64_t);
    leveldb_encode_fixed64(buf, meta->exptime_);
  }else{
    leveldb_encode_fixed32(buf, 0);
    buf += sizeof(uint32_t);
    leveldb_encode_fixed64(buf, 0);
    buf += sizeof(uint64_t);
    leveldb_encode_fixed64(buf, 0); 
    buf += sizeof(uint64_t);
    leveldb_encode_fixed64(buf, 0); 
  }
  ldb_slice_t *slice = ldb_slice_create(strmeta, sizeof(uint32_t) + sizeof(uint64_t) + sizeof(uint64_t) + sizeof(uint64_t));
  return slice;
}

void ldb_meta_encode(char* buf, uint32_t vercare, uint64_t lastver, uint64_t nextver){
  leveldb_encode_fixed32(buf, vercare);
  buf += sizeof(uint32_t);
  leveldb_encode_fixed64(buf, lastver);
  buf += sizeof(uint64_t);
  leveldb_encode_fixed64(buf, nextver);
}


void ldb_meta_encode_with_exp(char* buf, uint32_t vercare, uint64_t lastver, uint64_t nextver, uint64_t exptime){
  leveldb_encode_fixed32(buf, vercare);
  buf += sizeof(uint32_t);
  leveldb_encode_fixed64(buf, lastver);
  buf += sizeof(uint64_t);
  leveldb_encode_fixed64(buf, nextver);
  buf += sizeof(uint64_t);
  leveldb_encode_fixed64(buf, exptime);
}
