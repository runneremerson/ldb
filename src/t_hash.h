#ifndef LDB_T_HASH_H
#define LDB_T_HASH_H

#include "ldb_slice.h"
#include "ldb_meta.h"
#include "ldb_context.h"
#include "ldb_list.h"

#include <stdint.h>



int hash_get(ldb_context_t* context, const ldb_slice_t* name, const ldb_slice_t* key, ldb_slice_t** pslice);

int hash_mget(ldb_context_t* context, const ldb_slice_t* name, const ldb_list_t* keylist, ldb_list_t** pvallist, ldb_list_t** pmetalist);

int hash_getall(ldb_context_t* context, const ldb_slice_t* name, ldb_list_t** pkeylist, ldb_list_t** pvallist, ldb_list_t** pmetalist);

int hash_keys(ldb_context_t* context, const ldb_slice_t* name, ldb_list_t **plist);

int hash_vals(ldb_context_t* context, const ldb_slice_t* name, ldb_list_t** pvallist, ldb_list_t** pmetalist);

int hash_set(ldb_context_t* context, const ldb_slice_t* name, const ldb_slice_t* key, const ldb_meta_t* meta);

int hash_setnx(ldb_context_t* context, const ldb_slice_t* name, const ldb_slice_t* key, const ldb_meta_t* meta);

int hash_mset(ldb_context_t* context, const ldb_slice_t* name, const ldb_list_t* datalist, const ldb_list_t* metalist, ldb_list_t** plist);

int hash_exists(ldb_context_t* context, const ldb_slice_t* name, const ldb_slice_t* key);

int hash_length(ldb_context_t* context, const ldb_slice_t* name, uint64_t* length);

int hash_incr(ldb_context_t* context, const ldb_slice_t* name, const ldb_slice_t* key, const ldb_meta_t* meta,  int64_t by, int64_t* val);



#endif //LDB_T_HASH_H
