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
  if(size==0 || data == NULL){
    slice->data_ = NULL;
    slice->size_ = 0;
  }else{
    slice->data_ = lmalloc(size + 1);
    memcpy(slice->data_, data, size);
    slice->data_[size] = '\0';
    slice->size_ = size;
  }
  slice->capacity_ = slice->size_ + 1;
  return slice;
}

void ldb_slice_destroy(ldb_slice_t* slice){
  if(slice!=NULL){
    lfree(slice->data_);
    lfree(slice);
  }
}

static size_t ensure_capacity(size_t size){
  size_t capacity = size;
  if(capacity>16){
    capacity += (size>>1);
  }else{
    capacity += size;
  }
  return capacity;
}

void ldb_slice_push_back(ldb_slice_t* slice, const char* data, size_t size){
  if(size > 0){
    if(slice->capacity_ < (slice->size_ + size + 1)){
      slice->capacity_ = ensure_capacity(slice->size_ + size + 1);
      slice->data_ = lrealloc(slice->data_, slice->capacity_);
    }
    memcpy(slice->data_ + slice->size_, data, size);
    slice->size_ += size;
    slice->data_[slice->size_] = '\0';
  }
}


void ldb_slice_push_front(ldb_slice_t* slice, const char* data, size_t size){
  if(size > 0){
    if(slice->capacity_ < (slice->size_ + size + 1)){
      slice->capacity_ = ensure_capacity(slice->size_ + size + 1);
      slice->data_ = lrealloc(slice->data_, slice->capacity_);
    }
    memmove(slice->data_ + size, slice->data_, slice->size_);
    memcpy(slice->data_, data, size);
    slice->size_ += size;
    slice->data_[slice->size_] = '\0';
  }
}

const char* ldb_slice_data(const ldb_slice_t* slice){
  if(slice != NULL){
    return slice->data_;     
  }
  return NULL;
}

size_t ldb_slice_size(const ldb_slice_t* slice){
  if(slice != NULL){
    return slice->size_;     
  }
  return 0;
}
