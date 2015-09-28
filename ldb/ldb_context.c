#include "ldb_context.h"
#include "ldb_slice.h"
#include "lmalloc.h"

#include <leveldb-ldb/c.h>
#include <stdlib.h>
#include <unistd.h>
#include <assert.h>
#include <string.h>


#define EXPIRATION_LIST_KV "\xff\xff\xff\xff\xff|EXPIRATION_LIST|KV"



ldb_context_t* ldb_context_create(const char* name, size_t cache_size, size_t write_buffer_size){
    ldb_context_t* context = (ldb_context_t*)(lmalloc(sizeof(ldb_context_t)));
    memset(context, 0, sizeof(ldb_context_t));
    context->options_ = leveldb_options_create();
    leveldb_options_set_create_if_missing(context->options_, 1);
    leveldb_options_set_max_open_files(context->options_, 10000);
    context->filtter_policy_ = leveldb_filterpolicy_create_bloom(10);
    leveldb_options_set_filter_policy(context->options_, context->filtter_policy_);
    context->block_cache_ = leveldb_cache_create_lru(cache_size*1024*1024);
    leveldb_options_set_cache(context->options_, context->block_cache_);
    context->batch_ = leveldb_writebatch_create();
    context->mutex_ = leveldb_mutex_create();
    leveldb_options_set_block_size(context->options_, 32*1024);
    leveldb_options_set_write_buffer_size(context->options_, write_buffer_size*1024*1024);
    leveldb_options_set_compression(context->options_, leveldb_snappy_compression);
    leveldb_options_set_compaction_speed(context->options_, 1000);
    ldb_slice_t *expiring_name = ldb_slice_create(EXPIRATION_LIST_KV, strlen(EXPIRATION_LIST_KV));
    context->expiring_name_ = expiring_name;
    char* leveldb_error = NULL;
    context->database_ = leveldb_open(context->options_, name, &leveldb_error); 
    if(leveldb_error!=NULL){
        goto err;
    }
    return context;
err:
    if(context->database_!=NULL){
        leveldb_close(context->database_);
    }
    if(context->options_!=NULL){
        leveldb_options_destroy(context->options_);
    }
    if(context->filtter_policy_!=NULL){
        leveldb_filterpolicy_destroy(context->filtter_policy_);
    }
    if(context->block_cache_!=NULL){
        leveldb_cache_destroy(context->block_cache_);
    }
    if(context->batch_!=NULL){
        leveldb_writebatch_destroy(context->batch_);
    }
    if(context->mutex_!=NULL){
        leveldb_mutex_destroy(context->mutex_);
    }
    if(context->expiring_name_ !=NULL){
        ldb_slice_destroy((ldb_slice_t*)(context->expiring_name_));
    }
    lfree(context);
    return NULL;
}

void ldb_context_destroy( ldb_context_t* context){
    if(context!=NULL){
        leveldb_close(context->database_);
        leveldb_options_destroy(context->options_);
        leveldb_filterpolicy_destroy(context->filtter_policy_);
        leveldb_cache_destroy(context->block_cache_);
        leveldb_writebatch_destroy(context->batch_);
        leveldb_mutex_destroy(context->mutex_);
        ldb_slice_destroy((ldb_slice_t*)(context->expiring_name_));
    }
    lfree(context);
}

void ldb_context_writebatch_commit(ldb_context_t* context, char** errptr){
    leveldb_writeoptions_t *writeoptions = leveldb_writeoptions_create();

    leveldb_mutex_lock(context->mutex_);
    leveldb_write(context->database_, writeoptions, context->batch_, errptr);
    leveldb_writebatch_clear(context->batch_);
    leveldb_mutex_unlock(context->mutex_);

    leveldb_writeoptions_destroy(writeoptions);
}

void ldb_context_writebatch_put(ldb_context_t* context, const char* key, size_t klen, const char* val, size_t vlen){
    leveldb_mutex_lock(context->mutex_);
    leveldb_writebatch_put(context->batch_, key, klen, val, vlen);
    leveldb_mutex_unlock(context->mutex_);
}

void ldb_context_writebatch_delete(ldb_context_t* context, const char* key, size_t klen){
    leveldb_mutex_lock(context->mutex_);
    leveldb_writebatch_delete(context->batch_, key, klen);
    leveldb_mutex_unlock(context->mutex_);
}
