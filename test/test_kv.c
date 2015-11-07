#include "ldb/t_kv.h"
#include "ldb/ldb_define.h"
#include "ldb/util.h"

#include <assert.h>
#include <string.h>
#include <stdio.h>

static void test_string(ldb_context_t* context){
    char *ckey = "key1";
    char *cval = "val1";
    ldb_slice_t *key1 = ldb_slice_create(ckey, strlen(ckey));
    ldb_slice_t *val1 = ldb_slice_create(cval, strlen(cval));
    uint64_t nextver1 = time_ms();
    ldb_meta_t *meta1 = ldb_meta_create(0, 0, nextver1); 

    assert(string_set(context, key1, val1, meta1) == LDB_OK);

    ldb_slice_destroy(val1);
    ldb_meta_destroy(meta1); 

    ldb_meta_t *meta2 = NULL;

    assert(string_get(context, key1, &val1, &meta2)==LDB_OK); 

    assert(compare_with_length(ldb_slice_data(val1), ldb_slice_size(val1), cval, strlen(cval))==0);
    assert(nextver1 == ldb_meta_nextver(meta2));

    ldb_slice_destroy(val1);
    ldb_meta_destroy(meta2);

    uint64_t nextver2 = nextver1 + 100000;
    ldb_meta_t *meta3 =  ldb_meta_create(0, 0, nextver2); 
    assert(string_del(context, key1, meta3) == LDB_OK);

    ldb_meta_t *meta4 = NULL;
    assert(string_get(context, key1, &val1, &meta4) == LDB_OK_NOT_EXIST);

    ldb_meta_destroy(meta3);
    ldb_slice_destroy(key1);
}

static void test_expire(ldb_context_t* context){
    char *ckey = "key2";
    char *cval = "val2";
    ldb_slice_t *key1 = ldb_slice_create(ckey, strlen(ckey));
    ldb_slice_t *val1 = ldb_slice_create(cval, strlen(cval));
    uint64_t now = time_ms();
    uint64_t exp = 5000;
    ldb_meta_t *meta1 = ldb_meta_create_with_exp(0, 0, now, exp+now); 

    assert(string_set(context, key1, val1, meta1) == LDB_OK);

    ldb_slice_destroy(val1);
    ldb_meta_destroy(meta1); 

    ldb_meta_t *meta2 = NULL;

    assert(string_get(context, key1, &val1, &meta2)==LDB_OK); 
    assert(now = ldb_meta_nextver(meta2));
    printf("expire time %lu \n", ldb_meta_exptime(meta2));

    ldb_slice_destroy(val1);
    ldb_slice_destroy(key1);
    ldb_meta_destroy(meta2); 
}


int main(int argc, char* argv[]){
    ldb_context_t *context = ldb_context_create("/tmp/testdb", 128, 64);
    assert(context != NULL);

    test_string(context);
    test_expire(context);



    ldb_context_destroy(context);  
    return 0;
}
