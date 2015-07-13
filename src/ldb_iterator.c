#include "ldb_iterator.h"
#include "ldb_define.h"
#include "lmalloc.h"
#include "util.h"

#include <leveldb-ldb/c.h>
#include <string.h>

struct ldb_zset_iterator_t {
    ldb_slice_t *end_;
    int direction_;
    uint64_t limit_;
    leveldb_iterator_t *iterator_;
};

ldb_zset_iterator_t* ldb_zset_iterator_create(ldb_context_t *context, const ldb_slice_t *start, const ldb_slice_t *end, uint64_t limit, int direction){
    ldb_zset_iterator_t *ziterator = (ldb_zset_iterator_t*)lmalloc(sizeof(ldb_zset_iterator_t));
    ziterator->end_ = ldb_slice_create(ldb_slice_data(end), ldb_slice_size(end));
    ziterator->direction_ = direction;
    ziterator->limit_ = limit;
    leveldb_readoptions_t *readoptions = leveldb_readoptions_create();
    leveldb_readoptions_set_fill_cache(readoptions, 0);
    ziterator->iterator_ = leveldb_create_iterator(context->database_, readoptions);
    leveldb_iter_seek(ziterator->iterator_, ldb_slice_data(start), ldb_slice_size(start));
    if(ziterator->direction_ == FORWARD){
        if(leveldb_iter_valid(ziterator->iterator_)){
            size_t klen = 0;
            const char* key = leveldb_iter_key(ziterator->iterator_, &klen);
            if(compare_with_length(ldb_slice_data(start), ldb_slice_size(start), key, klen)==0){
                leveldb_iter_next(ziterator->iterator_);
            }
        }
    }else{
        if(leveldb_iter_valid(ziterator->iterator_)){
            leveldb_iter_prev(ziterator->iterator_);
        }else{
            leveldb_iter_seek_to_last(ziterator->iterator_);
        }
    }
    leveldb_readoptions_destroy(readoptions);
}

void ldb_zset_iterator_destroy(ldb_zset_iterator_t* ziterator){
    if(ziterator!=NULL){
        ldb_slice_destroy(ziterator->end_);
        leveldb_iter_destroy(ziterator->iterator_);
    }
    lfree(ziterator);
}

int ldb_zset_iterator_next(ldb_zset_iterator_t *ziterator){
    int retval = 0;
    if(ziterator->limit_ == 0){
        retval = -1;
        goto end;
    }

    if(ziterator->direction_ == FORWARD){
        leveldb_iter_next(ziterator->iterator_);
    }else{
        leveldb_iter_prev(ziterator->iterator_);
    }

    if(!leveldb_iter_valid(ziterator->iterator_)){
        ziterator->limit_ = 0;
        retval = -1;
        goto end;
    }

    size_t klen = 0;
    const char *key = leveldb_iter_key(ziterator->iterator_, &klen);
    if(ziterator->direction_ == FORWARD){
        if(ldb_slice_size(ziterator->end_) > 0){
            if(compare_with_length(key, klen, ldb_slice_data(ziterator->end_), ldb_slice_size(ziterator->end_)) > 0){
                ziterator->limit_ = 0;
                retval = -1;
                goto end;
            }
        }
    }else{
        if(ldb_slice_size(ziterator->end_) >0){
            if(compare_with_length(key, klen, ldb_slice_data(ziterator->end_), ldb_slice_size(ziterator->end_)) < 0){
                ziterator->limit_ = 0;
                retval = -1;
                goto end;
            }
        }
    }

    (ziterator->limit_)--;

    if(compare_with_length(key, strlen(LDB_DATA_TYPE_ZSCORE), LDB_DATA_TYPE_ZSCORE, strlen(LDB_DATA_TYPE_ZSCORE))!=0){
        retval = -1;
        goto end;
    }
    retval = 0;
end:
    return retval;
}

int ldb_zset_iterator_skip(ldb_zset_iterator_t *ziterator, uint64_t offset){
    int retval = 0;
    while(offset>0){
        if(ldb_zset_iterator_next(ziterator) < 0){
            retval = -1;
            goto end;
        }
        --offset;
    }
    retval = 0;

end:
    return retval;
}

void ldb_zset_iterator_val(const ldb_zset_iterator_t *ziterator, ldb_slice_t **pslice){
    size_t vlen = 0;
    const char *val = leveldb_iter_value(ziterator->iterator_, &vlen);
    *pslice = ldb_slice_create(val, vlen);
}

void ldb_zset_iterator_key(const ldb_zset_iterator_t *ziterator, ldb_slice_t **pslice){
    size_t klen = 0;
    const char *key = leveldb_iter_key(ziterator->iterator_, &klen);
    *pslice = ldb_slice_create(key, klen);
}
