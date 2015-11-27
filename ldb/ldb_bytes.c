#include "ldb_bytes.h"
#include "ldb_slice.h"
#include "lmalloc.h"

#include <leveldb/c.h>
#include <string.h>

struct ldb_bytes_t{
  const char* data_;
  size_t size_;
};

ldb_bytes_t* ldb_bytes_create(const char* data, size_t size){
  ldb_bytes_t *bytes = (ldb_bytes_t*)lmalloc(sizeof(ldb_bytes_t));
  bytes->data_ = data;
  bytes->size_ = size;
  return bytes;
}

void ldb_bytes_destroy(ldb_bytes_t* bytes){
  lfree(bytes);
}

int ldb_bytes_skip(ldb_bytes_t* bytes, size_t n){
  if(bytes->size_ < n ){
    return -1;
  }
  bytes->size_ -= n;
  bytes->data_ += n;
  return n;
}

int ldb_bytes_read_int64(ldb_bytes_t* bytes, int64_t* val){
  if(bytes->size_ < sizeof(int64_t)){
    return -1;
  }
  if(val != NULL){
    *val = leveldb_decode_fixed64(bytes->data_);
  }
  bytes->data_ += sizeof(int64_t);
  bytes->size_ -= sizeof(int64_t);
  return sizeof(int64_t);
}


int ldb_bytes_read_uint64(ldb_bytes_t* bytes, uint64_t* val){
  if(bytes->size_ < sizeof(uint64_t)){
    return -1;
  }
  if(val != NULL){
    *val = leveldb_decode_fixed64(bytes->data_);
  }
  bytes->data_ += sizeof(uint64_t);
  bytes->size_ -= sizeof(uint64_t);
  return sizeof(uint64_t);
}

int ldb_bytes_read_slice_size_left(ldb_bytes_t* bytes, ldb_slice_t** pslice){
  int n = bytes->size_;
  if(pslice != NULL){
    *pslice = ldb_slice_create(bytes->data_, bytes->size_);
  }
  bytes->size_ = 0;
  bytes->data_ += n;
  return n; 
}

int ldb_bytes_read_slice_size_uint8(ldb_bytes_t* bytes, ldb_slice_t** pslice){
  if(bytes->size_ < sizeof(uint8_t)){
    return -1;
  }
  uint8_t size = leveldb_decode_fixed8(bytes->data_);
  bytes->size_ -= sizeof(uint8_t);
  bytes->data_ += sizeof(uint8_t);
  if(bytes->size_ < size){
    return -1;
  }
  if(pslice != NULL){
    *pslice = ldb_slice_create(bytes->data_, size);
  }
  bytes->size_ -= size;
  bytes->data_ += size;
  return size+sizeof(uint8_t);
}

int ldb_bytes_read_slice_size_uint16(ldb_bytes_t* bytes, ldb_slice_t** pslice){
  if(bytes->size_ < sizeof(uint16_t)){
    return -1;
  }
  uint16_t size = leveldb_decode_fixed16(bytes->data_);
  bytes->size_ -= sizeof(uint16_t);
  bytes->data_ += sizeof(uint16_t);
  if(bytes->size_ < size){
    return -1;
  }
  if(pslice != NULL){
    *pslice = ldb_slice_create(bytes->data_, size);
  }
  bytes->size_ -= size;
  bytes->data_ += size;
  return size+sizeof(uint16_t);
}

