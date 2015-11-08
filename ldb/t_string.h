#ifndef LDB_T_STRING_H
#define LDB_T_STRING_H


#include "ldb_context.h"
#include "ldb_slice.h"
#include "ldb_meta.h"
#include "ldb_list.h"


void encode_kv_key(const char* key, size_t keylen, const ldb_meta_t* meta, ldb_slice_t** pslice);

int decode_kv_key(const char* ldbkey, size_t ldbkeylen, ldb_slice_t** pslice);


int string_set(ldb_context_t* context, const ldb_slice_t* key, const ldb_slice_t* value, const ldb_meta_t* meta); 

int string_setnx(ldb_context_t* context, const ldb_slice_t* key, const ldb_slice_t* value, const ldb_meta_t* meta);

int string_setxx(ldb_context_t* context, const ldb_slice_t* key, const ldb_slice_t* value, const ldb_meta_t* meta);


int string_mset(ldb_context_t* context, const ldb_list_t* datalist, const ldb_list_t* metalist, ldb_list_t** plist);

int string_msetnx(ldb_context_t* context, const ldb_list_t* datalist, const ldb_list_t* metalist, ldb_list_t** plist); 

int string_get(ldb_context_t* context, const ldb_slice_t* key, ldb_slice_t** pvalue, ldb_meta_t** meta);

int string_mget(ldb_context_t* context, const ldb_list_t* keylist, ldb_list_t** pvallist, ldb_list_t** pmetalist);

int string_del(ldb_context_t* context, const ldb_slice_t* key, const ldb_meta_t* meta);

int string_incr(ldb_context_t* context, const ldb_slice_t* key, const ldb_meta_t* meta, int64_t init, int64_t by, int64_t* val);






#endif //LDB_T_STRING_H

