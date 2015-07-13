#ifndef LDB_T_ZSET_H
#define LDB_T_ZSET_H


#include "ldb_slice.h"
#include "ldb_meta.h"
#include "ldb_context.h"
#include "ldb_iterator.h"

#include <stdint.h>

int zset_add(ldb_context_t* context, const ldb_slice_t* name, const ldb_slice_t* key, const ldb_meta_t* meta, int64_t score); 

int zset_del(ldb_context_t* context, const ldb_slice_t* name, const ldb_slice_t* key, const ldb_meta_t* meta);

int zset_incr(ldb_context_t* context, const ldb_slice_t* name, const ldb_slice_t* key, const ldb_meta_t* meta,  int64_t by, int64_t* val);

int zset_size(ldb_context_t* context, const ldb_slice_t* name, uint64_t* size);

int zset_get(ldb_context_t* context, const ldb_slice_t* name, const ldb_slice_t* key, int64_t* score);

int zset_rank(ldb_context_t* context, const ldb_slice_t* name, const ldb_slice_t* key, int reverse, int64_t* rank);

int zset_scan(ldb_context_t* context, const ldb_slice_t* name, const ldb_slice_t* key, int64_t start, int64_t end, uint64_t limit, int withscores, int reverse); 



#endif //LDB_T_ZSET_H

