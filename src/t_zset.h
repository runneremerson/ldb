#ifndef LDB_T_ZSET_H
#define LDB_T_ZSET_H


#include "ldb_slice.h"
#include "ldb_meta.h"
#include "ldb_context.h"
#include "ldb_list.h"

#include <stdint.h>


void encode_zsize_key(const char* name, size_t namelen, const ldb_meta_t* meta, ldb_slice_t** pslice);
int decode_zsize_key(const char* ldbkey, size_t ldbkeylen, ldb_slice_t** pslice);

void encode_zset_key(const char* name, size_t namelen, const char* key, size_t keylen, const ldb_meta_t* meta, ldb_slice_t** pslice);
int decode_zset_key(const char* ldbkey, size_t ldbkeylen, ldb_slice_t** pslice_name, ldb_slice_t** pslice_key);

void encode_zscore_key(const char* name, size_t namelen, const char* key, size_t keylen, const ldb_meta_t* meta, int64_t score, ldb_slice_t** pslice);
int decode_zscore_key(const char* ldbkey, size_t ldbkeylen, ldb_slice_t** pslice_name, ldb_slice_t** pslice_key,  int64_t *pscore);


int zset_add(ldb_context_t* context, const ldb_slice_t* name, 
        const ldb_slice_t* key, const ldb_meta_t* meta, int64_t score); 

int zset_del(ldb_context_t* context, const ldb_slice_t* name, 
        const ldb_slice_t* key, const ldb_meta_t* meta);

int zset_del_range_by_rank(ldb_context_t* context, const ldb_slice_t* name,
        const ldb_meta_t* meta, int rank_start, int rank_end, uint64_t *deleted);

int zset_del_range_by_score(ldb_context_t* context, const ldb_slice_t* name,
        const ldb_meta_t* meta, int64_t score_start, int64_t score_end, uint64_t *deleted);

int zset_incr(ldb_context_t* context, const ldb_slice_t* name, 
        const ldb_slice_t* key, const ldb_meta_t* meta,  int64_t by, int64_t* val);

int zset_size(ldb_context_t* context, const ldb_slice_t* name, 
        uint64_t* size);

int zset_get(ldb_context_t* context, const ldb_slice_t* name, 
        const ldb_slice_t* key, int64_t* score);

int zset_rank(ldb_context_t* context, const ldb_slice_t* name, 
        const ldb_slice_t* key, int reverse, int64_t* rank);

int zset_count(ldb_context_t* context, const ldb_slice_t* name,
        int64_t score_start, int64_t score_end, uint64_t *count);

int zset_range(ldb_context_t* context, const ldb_slice_t* name, 
        int rank_start, int rank_end, int reverse, ldb_list_t **pkeylist, ldb_list_t **pmetalist);

int zset_scan(ldb_context_t* context, const ldb_slice_t* name,
        int64_t score_start, int64_t score_end, int reverse, ldb_list_t **pkeylist, ldb_list_t **pmetalist);

#endif //LDB_T_ZSET_H

