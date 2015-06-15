#include "ldb_slice.h"
#include "lmalloc.h"

#include <string.h>



struct ldb_slice_t {
    size_t size_;
    size_t capacity_;
    char *data_;
};




ldb_slice_t* ldb_slice_create(const char* data, size_t size){
  ldb_slice_t *slice = (ldb_slice_t*)lmalloc(sizeof(ldb_slice_t));
  memcpy(slice->data_, data, size);
  slice->size_ = size;
  slice->capacity_ = size;
  return slice;
}

void ldb_slice_destroy(ldb_slice_t* slice){
  if(slice!=NULL){
    lfree(slice->data_);
  }
  lfree(slice);
}

static size_t ensure_capacity(size_t size){
  size_t capacity = size;
  if(size >16){
    capacity += (size >>1);
  }else{
    capacity += size;
  }
  return capacity;
}

void ldb_slice_push_back(ldb_slice_t* slice, const char* data, size_t size){
  if(slice->capacity_ < (slice->size_ + size)){
    slice->capacity_ = ensure_capacity(slice->size_ + size);
    slice->data_ = lrealloc(slice->data_, slice->capacity_);
  }
  memcpy(slice->data_ + slice->size_, data, size);
  slice->size_ += size;
}


void ldb_slice_push_front(ldb_slice_t* slice, const char* data, size_t size){
  if(slice->capacity_ < (slice->size_ + size)){
    slice->capacity_ = ensure_capacity(slice->size_ + size);
    slice->data_ = lrealloc(slice->data_, slice->capacity_);
  }
  memmove(slice->data_ + size, slice->data_, slice->size_);
  memcpy(slice->data_, data, size);
  slice->size_ += size;
}

const char* ldb_slice_data(const ldb_slice_t* slice){
  return slice->data_;
}

size_t ldb_slice_size(const ldb_slice_t* slice){
  return slice->size_;
}
