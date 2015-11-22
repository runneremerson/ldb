#ifndef LDB_T_SET_H
#define LDB_T_SET_H


#include "ldb_slice.h"
#include "ldb_meta.h"
#include "ldb_context.h"
#include "ldb_list.h"

#include <stdint.h>


void encode_ssize_key(const char* name, size_t namelen, const ldb_meta_t* meta, ldb_slice_t** pslice);
int decode_ssize_key(const char* ldbkey, size_t ldbkeylen, ldb_slice_t** pslice);

void encode_set_key(const char* name, size_t namelen, const char* key, size_t keylen, const ldb_meta_t* meta, ldb_slice_t** pslice);
int decode_set_key(const char* ldbkey, size_t ldbkeylen, ldb_slice_t** pslice_name, ldb_slice_t** pslice_key);

int set_card(ldb_context_t* context, const ldb_slice_t* name, uint64_t *length);

int set_members(ldb_context_t* context, const ldb_slice_t* name, ldb_list_t** pkeylist, ldb_list_t** pmetalist);

int set_add(ldb_context_t* context, const ldb_slice_t* name, const ldb_slice_t* key, const ldb_meta_t* meta);

int set_pop(ldb_context_t* context, const ldb_slice_t* name, const ldb_meta_t* meta, ldb_slice_t** pslice);

int set_rem(ldb_context_t* context, const ldb_slice_t* name, const ldb_slice_t* key, const ldb_meta_t* meta);

int set_ismember(ldb_context_t* context, const ldb_slice_t* name, const ldb_slice_t* key);

#endif //LDB_T_SET_H
