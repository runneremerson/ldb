#include "ttl.h"
#include "t_zset.h"



#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <stdint.h>

#define EXPIRATION_LIST_KV "\xff\xff\xff\xff\xff|EXPIRATION_LIST|KV"



int ttl_get(ldb_context_t* context, const ldb_slice_t* key, int64_t *pttl){
    ldb_slice_t *name = ldb_slice_create(EXPIRATION_LIST_KV, strlen(EXPIRATION_LIST_KV));
    int retval = zset_get(context, name, key, pttl); 
    ldb_slice_destroy(name);
    return retval;
}

int ttl_del(ldb_context_t* context, const ldb_slice_t* key, const ldb_meta_t* meta){
    ldb_slice_t *name = ldb_slice_create(EXPIRATION_LIST_KV, strlen(EXPIRATION_LIST_KV));
    int retval = zset_del(context, name, key, meta);
    ldb_slice_destroy(name);
    return retval; 
}

int ttl_set(ldb_context_t* context, const ldb_slice_t* key, const ldb_meta_t* meta, int64_t ttl){
    ldb_slice_t *name = ldb_slice_create(EXPIRATION_LIST_KV, strlen(EXPIRATION_LIST_KV));
    int retval = zset_add(context, name, key, meta, ttl);
    ldb_slice_destroy(name);
    return retval; 
}
