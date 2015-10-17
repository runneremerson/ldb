#ifndef LDB_CONTEXT_H
#define LDB_CONTEXT_H


#include <leveldb-ldb/c.h>
#include <stdlib.h>
#include <unistd.h>


struct ldb_context_t{
    leveldb_t*                  database_;
    leveldb_options_t*          options_;
    leveldb_filterpolicy_t*     filter_policy_;
    leveldb_cache_t*            block_cache_;
    leveldb_writebatch_t*       batch_;
    leveldb_mutex_t*            mutex_; //protect batch_
    void*                       expiring_name_;
};

typedef struct ldb_context_t    ldb_context_t;


ldb_context_t* ldb_context_create(const char* name, size_t cache_size, size_t write_buffer_size);

void ldb_context_destroy( ldb_context_t* context);

void ldb_context_writebatch_commit(ldb_context_t* context, char** errptr);

void ldb_context_writebatch_put(ldb_context_t* context, const char* key, size_t klen, const char* val, size_t vlen);

void ldb_context_writebatch_delete(ldb_context_t* context, const char* key, size_t klen);

#endif //LDB_CONTEXT_H
