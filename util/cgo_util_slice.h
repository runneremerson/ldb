#ifndef UTIL_CGO_UTIL_SLICE_H
#define UTIL_CGO_UTIL_SLICE_H

#include "cgo_util_slice.h"

void freeByteSliceMembers(GoByteSliceSlice* slice) {
    int i=0;
    for(i=0; i<slice->len; i+=1) {
        GoByteSlice item = slice->data[i];
        if(item.data != NULL) {
            free(item.data);
            item.data = NULL;
        }
    }
}

#endif // UTIL_CGO_UTIL_SLICE_H

