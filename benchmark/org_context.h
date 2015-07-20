#ifndef ORG_CONTEXT_H
#define ORG_CONTEXT_H

#include <leveldb/c.h>
#include <stdlib.h>
#include <unistd.h>


struct org_context_t{
    leveldb_t*                  database_;
    leveldb_options_t*          options_;
    leveldb_filterpolicy_t*     filtter_policy_;
    leveldb_cache_t*            block_cache_;
    leveldb_writebatch_t*       batch_;
};

typedef struct org_context_t    org_context_t;


org_context_t* org_context_create(const char* name, size_t cache_size, size_t write_buffer_size);

void org_context_destroy( org_context_t* context);

void org_context_writebatch_commit(org_context_t* context, char** errptr);

void org_context_writebatch_put(org_context_t* context, const char* key, size_t klen, const char* val, size_t vlen);

void org_context_writebatch_delete(org_context_t* context, const char* key, size_t klen);

#endif //ORG_CONTEXT_H
