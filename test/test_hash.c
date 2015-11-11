#include "ldb/t_hash.h"
#include "ldb/ldb_define.h"
#include "ldb/util.h"

#include <assert.h>
#include <string.h>
#include <stdio.h>
#include <stdint.h>

static void test_hash(ldb_context_t* context){
    char *hash_name1 = "hash_name";
    char *hash_key1 = "hash_key1";
    ldb_slice_t *slice_name1 = ldb_slice_create(hash_name1, strlen(hash_name1));
    ldb_slice_t *slice_key1 = ldb_slice_create(hash_key1, strlen(hash_key1));
    ldb_slice_t *slice_val1 = NULL;
    ldb_meta_t *meta1 = NULL;
    assert(hash_get(context, slice_name1, slice_key1, &slice_val1, &meta1) == LDB_OK_NOT_EXIST);

    uint64_t nextver1 = time_ms();
    meta1 = ldb_meta_create(0, 0, nextver1);
    char *hash_val1 = "hash_val1";
    slice_val1 = ldb_slice_create(hash_val1, strlen(hash_val1));
    assert(hash_set(context, slice_name1, slice_key1, slice_val1, meta1) == LDB_OK);

    ldb_slice_destroy(slice_val1);
    ldb_meta_destroy(meta1);

    assert(hash_get(context, slice_name1, slice_key1, &slice_val1, &meta1) == LDB_OK);

    assert(ldb_meta_nextver(meta1)==nextver1);

    ldb_slice_destroy(slice_name1);
    ldb_slice_destroy(slice_key1);
    ldb_slice_destroy(slice_val1);
    ldb_meta_destroy(meta1);

    
}



int main(int argc, char* argv[]){
    ldb_context_t *context = ldb_context_create("/tmp/testhash", 128, 64);
    assert(context != NULL);

    test_hash(context);



    ldb_context_destroy(context);  
    return 0;
}
