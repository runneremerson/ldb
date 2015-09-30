#ifndef LDB_TTL_H
#define LDB_TTL_H

#include "ldb_slice.h"
#include "ldb_meta.h"
#include "ldb_context.h"

#include <stdint.h>



int ttl_get(ldb_context_t* context, const ldb_slice_t* key, int64_t *pttl);
int ttl_del(ldb_context_t* context, const ldb_slice_t* key, const ldb_meta_t* meta);
int ttl_set(ldb_context_t* context, const ldb_slice_t* key, const ldb_meta_t* meta, int64_t ttl);

#endif //LDB_TTL_H


