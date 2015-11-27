#include "ldb_recovery.h"
#include "ldb_slice.h"
#include "ldb_iterator.h"
#include "ldb_define.h"
#include "ldb_list.h"
#include "ldb_meta.h"

#include "lmalloc.h"
#include "util.h"


#include <leveldb/c.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <stdio.h>

struct ldb_recovery_t {
    ldb_recov_iterator_t* iter_;
};

ldb_recovery_t* ldb_recovery_create( ldb_context_t* context ){
    ldb_recovery_t *recovery = (ldb_recovery_t*)lmalloc(sizeof(ldb_recovery_t)); 
    recovery->iter_ = ldb_recov_iterator_create(context);
    return recovery;
}


void ldb_recovery_destroy( ldb_recovery_t* recovery ){
    if(recovery!=NULL){
        ldb_recov_iterator_destroy(recovery->iter_);
    }
    lfree(recovery);
}

static int check_recovery_valid(ldb_recovery_t* recovery){
    return ldb_recov_iterator_valid(recovery->iter_);
}

static int ldb_recovery_next(ldb_recovery_t* recovery){
    int retval = ldb_recov_iterator_next(recovery->iter_);
    if(retval<0){
        return -1;
    }
    return 0;
}

int ldb_recovery_rec(ldb_context_t* context, ldb_recovery_t* recovery){
    int retval = 0;
    ldb_slice_t *slice_key = NULL;

    size_t vlen = 0;
    const char *val = ldb_recov_iterator_val_raw(recovery->iter_, &vlen);
    if(vlen < LDB_VAL_META_SIZE){
        fprintf(stderr, "%s iterator encountered invalid value length %lu.\n", __func__, vlen);
        retval = 0; 
        goto end;
    }
    uint64_t version = leveldb_decode_fixed64(val + LDB_VAL_TYPE_SIZE);
    if(version == 0){
       retval = 0; 
       goto end;
    }
    size_t klen = 0;
    const char *key = ldb_recov_iterator_key_raw(recovery->iter_, &klen);
    ldb_meta_t *meta = ldb_meta_create(0 , 0, version);
    slice_key = ldb_meta_slice_create(meta);
    ldb_meta_destroy(meta);
    ldb_slice_push_back(slice_key, key, klen);
    char *errptr = NULL;
    leveldb_put_meta(context->database_, ldb_slice_data(slice_key), ldb_slice_size(slice_key), &errptr);
    if(errptr!=NULL){
        fprintf(stderr, "%s leveldb_put_meta failed %s.\n", __func__, errptr);
        leveldb_free(errptr);
        retval = -1;
        goto end;
    }
    retval = 0;

end:
    ldb_slice_destroy(slice_key);
    return retval;
}

int ldb_recovery_rec_batch(ldb_context_t* context, ldb_recovery_t* recovery, size_t limit){
    int retval = 0;
    if(!check_recovery_valid(recovery)){
        retval = -1;
        goto end;
    }
    while(limit>0){
        if(ldb_recovery_rec(context, recovery) < 0){
            retval = -1;
            goto end; 
        } 
        --limit;
        if(ldb_recovery_next(recovery) < 0){
            fprintf(stderr, "%s iterator came to the end.\n", __func__);
            retval = -1;
            goto end;
        }
    }
end:
    return retval;
    
}
