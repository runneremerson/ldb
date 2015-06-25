#ifndef LDB_T_KV_H
#define LDB_T_KV_H


#include "ldb_context.h"
#include "ldb_slice.h"
#include "ldb_meta.h"
#include "ldb_list.h"

int string_set(ldb_context_t* context, const ldb_slice_t* key, const ldb_slice_t* value, const ldb_meta_t* meta); 

int string_setnx(ldb_context_t* context, const ldb_slice_t* key, const ldb_slice_t* value, const ldb_meta_t* meta);

int string_setxx(ldb_context_t* context, const ldb_slice_t* key, const ldb_slice_t* value, const ldb_meta_t* meta);


int string_mset(ldb_context_t* context, const ldb_list_t* datalist, const ldb_list_t* metalist, ldb_list_t** plist);

int string_msetnx(ldb_context_t* context, const ldb_list_t* datalist, const ldb_list_t* metalist, ldb_list_t** plist); 

int string_get(ldb_context_t* context, const ldb_slice_t* key, ldb_slice_t** pvalue);


int string_del(ldb_context_t* context, const ldb_slice_t* key, const ldb_meta_t* meta);

//
//int string_mget
//
//int string_incr





#endif //LDB_T_KV_H

