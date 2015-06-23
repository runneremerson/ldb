#ifndef LDB_T_KV_H
#define LDB_T_KV_H


#include "ldb_context.h"
#include "ldb_slice.h"
#include "ldb_meta.h"

int string_set(ldb_context_t* context, const ldb_slice_t* key, const ldb_slice_t* value, const ldb_meta_t* meta); 

int string_setnx(ldb_context_t* context, const ldb_slice_t* key, const ldb_slice_t* value, const ldb_meta_t* meta);

int string_setxx(ldb_context_t* context, const ldb_slice_t* key, const ldb_slice_t* value, const ldb_meta_t* meta);

int string_get(ldb_context_t* context, const ldb_slice_t* key, ldb_slice_t** pvalue);

int string_getset(ldb_context_t* context, const ldb_slice_t* key, const ldb_slice_t* value, const ldb_meta_t* meta, ldb_slice_t** pvalue);

//int string_mset
//
//int string_mget
//
//int string_del
//
//int string_incr





#endif //LDB_T_KV_H

