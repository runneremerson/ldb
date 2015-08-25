#ifndef UTIL_CGO_UTIL_BASE_H
#define UTIL_CGO_UTIL_BASE_H

#include <stdio.h>
#include <inttypes.h>
#include <stdlib.h>

typedef struct GoByteSlice
{
    char *data;
    uint64_t data_len;
    uint64_t cap;
} GoByteSlice;

typedef struct GoUint64Slice
{
    uint64_t *data;
    uint64_t len;
    uint64_t cap;
} GoUint64Slice;

typedef struct GoByteSliceSlice
{
    GoByteSlice *data;
    uint64_t len;
    uint64_t cap;
} GoByteSliceSlice;

#endif // UTIL_CGO_UTIL_BASE_H
