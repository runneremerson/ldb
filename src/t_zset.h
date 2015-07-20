#ifndef LDB_T_ZSET_H
#define LDB_T_ZSET_H


#include "ldb_slice.h"
#include "ldb_meta.h"
#include "ldb_context.h"
#include "ldb_list.h"

#include <stdint.h>

int zset_add(ldb_context_t* context, const ldb_slice_t* name, 
        const ldb_slice_t* key, const ldb_meta_t* meta, int64_t score); 

int zset_del(ldb_context_t* context, const ldb_slice_t* name, 
        const ldb_slice_t* key, const ldb_meta_t* meta);

int zset_del_range_by_rank(ldb_context_t* context, const ldb_slice_t* name,
                           const ldb_meta_t* meta, uint64_t offset, uint64_t limit, int64_t *deleted);

int zset_del_range_by_score(ldb_context_t* context, const ldb_slice_t* name,
                            const ldb_meta_t* meta, int64_t start, int64_t end, int *deleted);

int zset_incr(ldb_context_t* context, const ldb_slice_t* name, 
        const ldb_slice_t* key, const ldb_meta_t* meta,  int64_t by, int64_t* val);

int zset_size(ldb_context_t* context, const ldb_slice_t* name, 
        uint64_t* size);

int zset_get(ldb_context_t* context, const ldb_slice_t* name, 
        const ldb_slice_t* key, int64_t* score);

int zset_rank(ldb_context_t* context, const ldb_slice_t* name, 
        const ldb_slice_t* key, int reverse, int64_t* rank);

int zset_range(ldb_context_t* context, const ldb_slice_t* name, 
               uint64_t offset, uint64_t limit, int reverse, ldb_list_t **plist);

int zset_scan(ldb_context_t* context, const ldb_slice_t* name,
              int64_t start, int64_t end, int reverse, ldb_list_t **plist);


#endif //LDB_T_ZSET_H

