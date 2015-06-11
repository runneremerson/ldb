#ifndef LDB_T_KV_H
#define LDB_T_KV_H


#include "ldb_context.h"
#include "ldb_slice.h"

int string_set(ldb_context_t* context, const ldb_slice_t* key, const ldb_slice_t* value); 

int string_get(ldb_context_t* context, const ldb_slice_t* key, ldb_slice_t** pvalue);



#endif //LDB_T_KV_H

