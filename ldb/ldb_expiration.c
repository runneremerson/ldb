#include "ldb_expiration.h"
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

struct ldb_expiration_t {
    ldb_string_iterator_t *iter_;
};

ldb_expiration_t* ldb_expiration_create( ldb_context_t* context ){
    ldb_expiration_t *expiration = (ldb_expiration_t*)lmalloc(sizeof(ldb_expiration_t));
    ldb_meta_t *meta = ldb_meta_create(0, 0, 0);
    ldb_slice_t *start = ldb_meta_slice_create(meta);
    ldb_slice_t *end = ldb_meta_slice_create(meta);
    ldb_slice_push_back(start, LDB_DATA_TYPE_STRING, strlen(LDB_DATA_TYPE_STRING));
    ldb_slice_push_back(end, LDB_DATA_TYPE_STRING, strlen(LDB_DATA_TYPE_STRING));
    ldb_slice_push_back(end, "\xff", strlen("\xff"));
    uint64_t limit = 1000*1000*1000;
    expiration->iter_ = ldb_string_iterator_create(context, start, end, limit, FORWARD);

    ldb_slice_destroy(start);
    ldb_slice_destroy(end);
    ldb_meta_destroy(meta);
    return expiration;
}

void ldb_expiration_destroy( ldb_expiration_t* expiration ){
    if(expiration!=NULL){
        ldb_string_iterator_destroy(expiration->iter_);
    }
    lfree(expiration); 
}

static int check_expiration_valid( ldb_expiration_t* expiration){
    if(!ldb_string_iterator_valid(expiration->iter_)){
        return 0;
    }
    size_t klen = 0;
    const char* key = ldb_string_iterator_key_raw(expiration->iter_, &klen);
    if(compare_with_length(key, strlen(LDB_DATA_TYPE_STRING), LDB_DATA_TYPE_STRING, strlen(LDB_DATA_TYPE_STRING))==0){
        return 1;
    }
    return 0;
}

static int ldb_expiration_next( ldb_expiration_t* expiration ){
    int retval = ldb_string_iterator_next(expiration->iter_);
    if(retval<0){
        return -1;
    }
    return 0;
}

int ldb_expiration_exp(ldb_expiration_t* expiration, ldb_slice_t **pslice, uint64_t* expire){
    int retval = 0;
    size_t vlen = 0;
    const char* val = ldb_string_iterator_val_raw( expiration->iter_, &vlen); 
    if(vlen < LDB_VAL_META_SIZE){
        retval = -1; 
        goto end;
    }
    uint8_t type = leveldb_decode_fixed8(val);
    uint64_t now = time_ms();
    if(type & LDB_VALUE_TYPE_VAL){
        if(type & LDB_VALUE_TYPE_EXP){
            *expire = leveldb_decode_fixed64(val + LDB_VAL_META_SIZE);
            if(*expire > now){
                //log expire not timeout
                retval = -1;
                goto end;
            }
            size_t klen = 0;
            const char* key = ldb_string_iterator_key_raw(expiration->iter_, &klen);
            *pslice = ldb_slice_create(key, klen);
            retval = 0;
            goto end;
        }
    }
    retval = -1;

end:
    return retval; 
}

int ldb_expiration_exp_batch(ldb_expiration_t* expiration, ldb_list_t** plist, size_t limit){
    int retval = 0;


    if(!check_expiration_valid(expiration)){
        //log expiration
        retval = -1; 
        goto end;
    }
    *plist = ldb_list_create();

    while(limit >0){
        ldb_list_node_t* node = ldb_list_node_create();
        ldb_slice_t* key = NULL;
        uint64_t expire = 0;
        if( ldb_expiration_exp(expiration, &key, &expire) == 0){
            node->data_     = key; 
            node->value_    = expire;
            node->type_     = LDB_LIST_NODE_TYPE_SLICE;
            rpush_ldb_list_node(*plist, node);
            --limit;
        }
        if(ldb_expiration_next(expiration)<0){
            fprintf(stderr, "%s iterator came to the end.\n", __func__);
            //log iterator
            retval = -1;
            goto end;
        }
    } 

end:
    return retval;
}
