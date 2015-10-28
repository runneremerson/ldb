#include "ldb_expiration.h"
#include "ldb_slice.h"
#include "ldb_iterator.h"
#include "ldb_define.h"

#include "lmalloc.h"


#include <leveldb-ldb/c.h>
#include <stdlib.h>
#include <string.h>

struct ldb_expiration_t {
    ldb_kv_iterator_t *iter_;
};

ldb_expiration_t* ldb_expiration_create( ldb_context_t* context ){
    ldb_expiration_t *expiration = (ldb_expiration_t*)lmalloc(sizeof(ldb_expiration_t));
    ldb_slice_t *start = ldb_slice_create(LDB_DATA_TYPE_KV, strlen(LDB_DATA_TYPE_KV));
    ldb_slice_t *end = ldb_slice_create(LDB_DATA_TYPE_KV, strlen(LDB_DATA_TYPE_KV));
    ldb_slice_push_back(end, "\xff", strlen("\xff"));
    uint64_t limit = 1000*1000*1000;
    expiration->iter_ = ldb_kv_iterator_create(context, start, end, limit, FORWARD);

end:
    ldb_slice_destroy(start);
    ldb_slice_destroy(end);
    return expiration;
}

void ldb_expiration_destroy( ldb_expiration_t* expiration ){
    if(expiration!=NULL){
        ldb_kv_iterator_destroy(expiration->iter_);
    }
    lfree(expiration); 
}

int ldb_expiration_next( ldb_expiration_t* expiration ){
    int retval = ldb_kv_iterator_next(expiration->iter_);
    if(retval<0){
        return -1;
    }
    return 0;
}

int ldb_expiration_exp(ldb_expiration_t* expiration, ldb_slice_t **pslice, uint64_t* expire){
    int retval = 0;
    size_t vlen = 0;
    const char* val = ldb_kv_iterator_val_raw( expiration->iter_, &vlen); 

    return retval; 
}
