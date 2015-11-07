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

    int64_t init = 100;
    int64_t by = 7;
    int64_t result = 0;
    uint64_t nextver3 = nextver2 + 100000;
    meta4 = ldb_meta_create(0, 0, nextver3);
    assert(string_incr(context, key1, meta4, init, by, &result) == LDB_OK);
    assert(result == 107);
    ldb_meta_destroy(meta4);

    printf("string_incr result %ld\n", result);


    uint64_t nextver4 = nextver3 + 100000;
    init = 0;
    by = 7;
    result = 0;
    ldb_meta_t *meta5 = ldb_meta_create(0, 0, nextver4);
    assert(string_incr(context, key1, meta5, init, by, &result) == LDB_OK);
    assert(result == 114);
    ldb_meta_destroy(meta5);

    printf("string_incr result %ld\n", result);


    
    char *cval2 = "val2";
    ldb_slice_t *val2 = ldb_slice_create(cval2, strlen(cval2));
    uint64_t nextver5 = nextver4 + 100000;
    ldb_meta_t *meta6 = ldb_meta_create(0, 0, nextver5);
    assert(string_setnx(context, key1, val2, meta6) == LDB_OK_BUT_ALREADY_EXIST);


    assert(string_setxx(context, key1, val2, meta6) == LDB_OK);
    char *ckey2 = "key2";
    ldb_slice_t *key2 = ldb_slice_create(ckey2, strlen(ckey2));
    assert(string_setxx(context, key2, val2, meta6) == LDB_OK_NOT_EXIST);

 
    ldb_meta_destroy(meta6);
    ldb_slice_destroy(val2);
    ldb_slice_destroy(key2);
    ldb_slice_destroy(key1);

    //mset mget
}

static void test_expire(ldb_context_t* context){
    char *ckey = "expkey2";
    char *cval = "expval2";
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
    ldb_context_t *context = ldb_context_create("/tmp/teststring", 128, 64);
    assert(context != NULL);

    test_string(context);
    test_expire(context);



    ldb_context_destroy(context);  
    return 0;
}

