#include "ldb_iterator.h"
#include "ldb_define.h"
#include "lmalloc.h"
#include "util.h"

#include "t_zset.h"
#include "t_hash.h"
#include "t_string.h"

#include <leveldb/c.h>
#include <string.h>
#include <assert.h>

struct ldb_data_iterator_t {
    ldb_slice_t *name_;
    ldb_slice_t *end_;
    int direction_;
    uint64_t limit_;
    leveldb_iterator_t *iterator_;
};

ldb_zset_iterator_t* ldb_zset_iterator_create(ldb_context_t *context, const ldb_slice_t *name, 
                                              ldb_slice_t *start, ldb_slice_t *end, uint64_t limit, int direction){
    ldb_zset_iterator_t *iterator = (ldb_zset_iterator_t*)lmalloc(sizeof(ldb_zset_iterator_t));
    iterator->name_ = ldb_slice_create(ldb_slice_data(name), ldb_slice_size(name));
    iterator->end_ = ldb_slice_create(ldb_slice_data(end) + LDB_KEY_META_SIZE, ldb_slice_size(end) - LDB_KEY_META_SIZE);
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
            if(compare_with_length(ldb_slice_data(start) + LDB_KEY_META_SIZE, ldb_slice_size(start) - LDB_KEY_META_SIZE, key, klen)==0){
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
    return iterator;
}

void ldb_zset_iterator_destroy(ldb_zset_iterator_t* iterator){
    if(iterator!=NULL){
        ldb_slice_destroy(iterator->name_);
        ldb_slice_destroy(iterator->end_);
        leveldb_iter_destroy(iterator->iterator_);
    }
    lfree(iterator);
}

int ldb_zset_iterator_next(ldb_zset_iterator_t *iterator){
    int retval = 0;

    while(1){
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

        size_t vlen = 0;
        const char *val = leveldb_iter_value(iterator->iterator_, &vlen);
        assert(vlen > LDB_VAL_META_SIZE);
        uint8_t type = leveldb_decode_fixed8(val);
        if(type & LDB_VALUE_TYPE_LAT){
            continue;
        }

        size_t klen = 0;
        const char *key = leveldb_iter_key(iterator->iterator_, &klen);
        if(iterator->direction_ == FORWARD){
            if(ldb_slice_size(iterator->end_) > 0){
                if(compare_with_length(key, klen, ldb_slice_data(iterator->end_), ldb_slice_size(iterator->end_)) >= 0){
                    iterator->limit_ = 0;
                    retval = -1;
                    goto end;
                }
            }
        }else{
            if(ldb_slice_size(iterator->end_) >0){
                if(compare_with_length(key, klen, ldb_slice_data(iterator->end_), ldb_slice_size(iterator->end_)) <= 0){
                    iterator->limit_ = 0;
                    retval = -1;
                    goto end;
                }
            }
        }

        (iterator->limit_)--;

        if(compare_with_length(key, strlen(LDB_DATA_TYPE_ZSCORE), LDB_DATA_TYPE_ZSCORE, strlen(LDB_DATA_TYPE_ZSCORE))!=0){
            retval = -1;
            goto end;
        }

        ldb_slice_t *slice_name, *slice_key = NULL;
        int64_t score = 0;

        int repeat = 0;
        if(decode_zscore_key(key, klen, &slice_name, &slice_key, &score) < 0){
            retval = -1;
            repeat = 1;
        }else{
            repeat = 0;
            retval = 0;
        }

        ldb_slice_destroy(slice_name);
        ldb_slice_destroy(slice_key);
        if(repeat ==0){
            goto end;
        }
    }
  
end:
    return retval;
}

int ldb_zset_iterator_skip(ldb_zset_iterator_t *iterator, uint64_t offset){
    int retval = 0;
    while(offset>0){
        if(ldb_zset_iterator_next(iterator) < 0){
            retval = -1;
            goto end;
        }
        --offset;
    }
    retval = 0;

end:
    return retval;
}

void ldb_zset_iterator_val(const ldb_zset_iterator_t *iterator, ldb_slice_t **pslice){
    size_t vlen = 0;
    const char *val = leveldb_iter_value(iterator->iterator_, &vlen);
    *pslice = ldb_slice_create(val, vlen);
}

const char* ldb_zset_iterator_key_raw(const ldb_zset_iterator_t *iterator, size_t* klen){
    return leveldb_iter_key(iterator->iterator_, klen);
}

void ldb_zset_iterator_key(const ldb_zset_iterator_t *iterator, ldb_slice_t **pslice){
    size_t klen = 0;
    const char *key = leveldb_iter_key(iterator->iterator_, &klen);
    *pslice = ldb_slice_create(key, klen);
}



ldb_hash_iterator_t* ldb_hash_iterator_create(ldb_context_t *context, const ldb_slice_t *name, 
                                              ldb_slice_t *start, ldb_slice_t *end, uint64_t limit, int direction){
    ldb_hash_iterator_t *iterator = (ldb_zset_iterator_t*)lmalloc(sizeof(ldb_hash_iterator_t));
    iterator->name_ = ldb_slice_create(ldb_slice_data(name), ldb_slice_size(name));
    iterator->end_ = ldb_slice_create(ldb_slice_data(end) + LDB_KEY_META_SIZE, ldb_slice_size(end) - LDB_KEY_META_SIZE);
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
            if(compare_with_length(ldb_slice_data(start) + LDB_KEY_META_SIZE, ldb_slice_size(start) - LDB_KEY_META_SIZE, key, klen)==0){
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
    return iterator;
}

void ldb_hash_iterator_destroy(ldb_hash_iterator_t* iterator){
    if(iterator!=NULL){
        ldb_slice_destroy(iterator->name_);
        ldb_slice_destroy(iterator->end_);
        leveldb_iter_destroy(iterator->iterator_);
    }
    lfree(iterator);
}

int ldb_hash_iterator_next(ldb_hash_iterator_t *iterator){
    int retval = 0;

    while(1){
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

        size_t vlen = 0;
        const char *val = leveldb_iter_value(iterator->iterator_, &vlen);
        assert(vlen > LDB_VAL_META_SIZE);
        uint8_t type = leveldb_decode_fixed8(val);
        if(type & LDB_VALUE_TYPE_LAT){
            continue;
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

        if(compare_with_length(key, strlen(LDB_DATA_TYPE_HASH), LDB_DATA_TYPE_HASH, strlen(LDB_DATA_TYPE_HASH))!=0){
            retval = -1;
            goto end;
        }
        ldb_slice_t *slice_name, *slice_key = NULL;
        int repeat = 0;
        if(decode_hash_key(key, klen, &slice_name, &slice_key) < 0){
          retval = -1;
          repeat = 1;
        }else{
            if(compare_with_length(ldb_slice_data(slice_name), ldb_slice_size(slice_name), ldb_slice_data(iterator->name_), ldb_slice_size(iterator->name_)) != 0){
                retval = -1;
                goto end;
            }
        }
        ldb_slice_destroy(slice_name);
        ldb_slice_destroy(slice_key);
        if(repeat == 0){
            goto end;    
        }
    }

end:
    return retval;
}

void ldb_hash_iterator_val(const ldb_hash_iterator_t *iterator, ldb_slice_t **pslice){
    size_t vlen = 0;
    const char *val = leveldb_iter_value(iterator->iterator_, &vlen);
    *pslice = ldb_slice_create(val, vlen);
}

void ldb_hash_iterator_key(const ldb_hash_iterator_t *iterator, ldb_slice_t **pslice){
    size_t klen = 0;
    const char *key = leveldb_iter_key(iterator->iterator_, &klen);
    *pslice = ldb_slice_create(key, klen);
}

ldb_string_iterator_t* ldb_string_iterator_create(ldb_context_t *context, ldb_slice_t *start, ldb_slice_t *end, uint64_t limit, int direction){ 
    ldb_string_iterator_t *iterator = (ldb_string_iterator_t*)lmalloc(sizeof(ldb_string_iterator_t));
    iterator->name_ = NULL;
    iterator->end_ = ldb_slice_create(ldb_slice_data(end) + LDB_KEY_META_SIZE, ldb_slice_size(end) - LDB_KEY_META_SIZE);
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
            if(compare_with_length(ldb_slice_data(start) + LDB_KEY_META_SIZE, ldb_slice_size(start) - LDB_KEY_META_SIZE, key, klen)==0){
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
    return iterator;
}


void ldb_string_iterator_destroy(ldb_string_iterator_t* iterator){
    if(iterator!=NULL){
        ldb_slice_destroy(iterator->name_);
        ldb_slice_destroy(iterator->end_);
        leveldb_iter_destroy(iterator->iterator_);
    }
    lfree(iterator); 
}


int ldb_string_iterator_next(ldb_string_iterator_t *iterator){
    int retval = 0;

    while(1){
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

        size_t vlen = 0;
        const char *val = leveldb_iter_value(iterator->iterator_, &vlen);
        assert(vlen > LDB_VAL_META_SIZE);
        uint8_t type = leveldb_decode_fixed8(val);
        if(type & LDB_VALUE_TYPE_LAT){
            continue;
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

        if(compare_with_length(key, strlen(LDB_DATA_TYPE_STRING), LDB_DATA_TYPE_STRING, strlen(LDB_DATA_TYPE_STRING))!=0){
            retval = -1;
            goto end;
        }
        ldb_slice_t *slice_key = NULL;
        int repeat = 0;
        if(decode_kv_key(key, klen, &slice_key) < 0){
          retval = -1;
          repeat = 1;
        }
        ldb_slice_destroy(slice_key);
        if(repeat == 0){
            goto end;    
        }
    }

end:
    return retval; 
}


void ldb_string_iterator_val(const ldb_string_iterator_t *iterator, ldb_slice_t **pslice){
    size_t vlen = 0;
    const char *val = leveldb_iter_value(iterator->iterator_, &vlen);
    *pslice = ldb_slice_create(val, vlen); 
}

const char* ldb_string_iterator_val_raw(const ldb_string_iterator_t *iterator, size_t* vlen){
   return leveldb_iter_value(iterator->iterator_, vlen); 
}

void ldb_string_iterator_key(const ldb_string_iterator_t *iterator, ldb_slice_t **pslice){
    size_t klen = 0;
    const char *key = leveldb_iter_key(iterator->iterator_, &klen);
    *pslice = ldb_slice_create(key, klen); 
}

const char* ldb_string_iterator_key_raw(const ldb_string_iterator_t *iterator, size_t* klen){
    return leveldb_iter_key(iterator->iterator_, klen);
}

int ldb_string_iterator_valid(const ldb_string_iterator_t *iterator){
    return leveldb_iter_valid(iterator->iterator_); 
}
