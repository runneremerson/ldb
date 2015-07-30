#include "ldb_iterator.h"
#include "ldb_define.h"
#include "lmalloc.h"
#include "util.h"

#include <leveldb-ldb/c.h>
#include <string.h>

struct ldb_data_iterator_t {
    ldb_slice_t *end_;
    ldb_slice_t *type_;
    int direction_;
    uint64_t limit_;
    leveldb_iterator_t *iterator_;
};

ldb_data_iterator_t* ldb_data_iterator_create(ldb_context_t *context, 
                                              ldb_slice_t *type, ldb_slice_t *start, ldb_slice_t *end, uint64_t limit, int direction){
    ldb_data_iterator_t *iterator = (ldb_data_iterator_t*)lmalloc(sizeof(ldb_data_iterator_t));
    iterator->end_ = ldb_slice_create(ldb_slice_data(end) + LDB_KEY_META_SIZE, ldb_slice_size(end) - LDB_KEY_META_SIZE);
    iterator->type_ = type;
    iterator->direction_ = direction;
    iterator->limit_ = limit;
    leveldb_readoptions_t *readoptions = leveldb_readoptions_create();
    leveldb_readoptions_set_fill_cache(readoptions, 0);
    iterator->iterator_ = leveldb_create_iterator(context->database_, readoptions);
    leveldb_iter_seek(iterator->iterator_, ldb_slice_data(start) + LDB_KEY_META_SIZE, ldb_slice_size(start) - LDB_KEY_META_SIZE);
    if(iterator->direction_ == FORWARD){
        if(leveldb_iter_valid(iterator->iterator_)){
            size_t klen = 0;
            const char* key = leveldb_iter_key(iterator->iterator_, &klen);
            if(compare_with_length(ldb_slice_data(start), ldb_slice_size(start), key, klen)==0){
                leveldb_iter_next(iterator->iterator_);
            }
        }
    }else{
        if(leveldb_iter_valid(iterator->iterator_)){
            leveldb_iter_prev(iterator->iterator_);
        }else{
            leveldb_iter_seek_to_last(iterator->iterator_);
        }
    }
    leveldb_readoptions_destroy(readoptions);
    ldb_slice_destroy(start);
    ldb_slice_destroy(end);
    return iterator;
}

void ldb_data_iterator_destroy(ldb_data_iterator_t* iterator){
    if(iterator!=NULL){
        ldb_slice_destroy(iterator->end_);
        ldb_slice_destroy(iterator->type_);
        leveldb_iter_destroy(iterator->iterator_);
    }
    lfree(iterator);
}

int ldb_data_iterator_next(ldb_data_iterator_t *iterator){
    int retval = 0;
    if(iterator->limit_ == 0){
        retval = -1;
        goto end;
    }

    if(iterator->direction_ == FORWARD){
        leveldb_iter_next(iterator->iterator_);
    }else{
        leveldb_iter_prev(iterator->iterator_);
    }

    if(!leveldb_iter_valid(iterator->iterator_)){
        iterator->limit_ = 0;
        retval = -1;
        goto end;
    }

    size_t klen = 0;
    const char *key = leveldb_iter_key(iterator->iterator_, &klen);
    if(iterator->direction_ == FORWARD){
        if(ldb_slice_size(iterator->end_) > 0){
            if(compare_with_length(key, klen, ldb_slice_data(iterator->end_), ldb_slice_size(iterator->end_)) > 0){
                iterator->limit_ = 0;
                retval = -1;
                goto end;
            }
        }
    }else{
        if(ldb_slice_size(iterator->end_) >0){
            if(compare_with_length(key, klen, ldb_slice_data(iterator->end_), ldb_slice_size(iterator->end_)) < 0){
                iterator->limit_ = 0;
                retval = -1;
                goto end;
            }
        }
    }

    (iterator->limit_)--;

    if(compare_with_length(key, ldb_slice_size(iterator->type_), ldb_slice_data(iterator->type_), ldb_slice_size(iterator->type_))!=0){
        retval = -1;
        goto end;
    }
    retval = 0;
end:
    return retval;
}

int ldb_data_iterator_skip(ldb_data_iterator_t *iterator, uint64_t offset){
    int retval = 0;
    while(offset>0){
        if(ldb_data_iterator_next(iterator) < 0){
            retval = -1;
            goto end;
        }
        --offset;
    }
    retval = 0;

end:
    return retval;
}

void ldb_data_iterator_val(const ldb_data_iterator_t *iterator, ldb_slice_t **pslice){
    size_t vlen = 0;
    const char *val = leveldb_iter_value(iterator->iterator_, &vlen);
    *pslice = ldb_slice_create(val, vlen);
}

void ldb_data_iterator_key(const ldb_data_iterator_t *iterator, ldb_slice_t **pslice){
    size_t klen = 0;
    const char *key = leveldb_iter_key(iterator->iterator_, &klen);
    *pslice = ldb_slice_create(key, klen);
}

