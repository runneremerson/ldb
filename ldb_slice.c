#include "ldb_slice.h"
#include "lmalloc.h"

#include <string.h>


ldb_slice_t* ldb_slice_create(const char* data, size_t size){
    ldb_slice_t *slice = (ldb_slice_t*)lmalloc(size);
    slice->data_ = (char*)data;
    slice->size_ = size;
    return slice;
}

void ldb_slice_destroy(ldb_slice_t* slice){
    if(slice!=NULL){
        lfree(slice->data_);
    }
    lfree(slice);
}

void ldb_slice_push_back(ldb_slice_t* slice, const char* data, size_t size){
    slice->data_ = lrealloc(slice->data_, slice->size_ + size);
    memcpy(((void*)slice->data_)+slice->size_, data, size);
    slice->size_ += size;
}


void ldb_slice_push_front(ldb_slice_t* slice, const char* data, size_t size){
    slice->data_ = lrealloc(slice->data_, slice->size_ + size);
    memmove(slice->data_ + size, slice->data_, size);
    memcpy(slice->data_, data, size);
}
