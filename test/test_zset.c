#include "ldb/t_zset.h"
#include "ldb/ldb_define.h"
#include "ldb/util.h"

#include <leveldb/c.h>
#include <assert.h>
#include <string.h>
#include <stdio.h>

static void test_zset(ldb_context_t* context){
    const char *zset_name1 = "zset_namekk";
    ldb_slice_t *slice_name1 = ldb_slice_create(zset_name1, strlen(zset_name1));
    const char *zset_key1 = "zset_key1";
    ldb_slice_t *slice_key1 = ldb_slice_create(zset_key1, strlen(zset_key1));
    uint64_t nextver1 = time_ms();
    ldb_meta_t *meta1 = ldb_meta_create(0, 0, nextver1);
    int64_t score1 = 50;

    assert(zset_add(context, slice_name1, slice_key1, meta1, score1) == LDB_OK);

    score1 = 0;
    assert(zset_get(context, slice_name1, slice_key1, &score1) == LDB_OK);


    uint64_t size = 0;

    const char *zset_key2 = "zset_key2";
    ldb_slice_t *slice_key2 = ldb_slice_create(zset_key2, strlen(zset_key2));
    uint64_t nextver2 = nextver1 + 100000;
    ldb_meta_t *meta2 = ldb_meta_create(0, 0, nextver2);
    int64_t score2 = 51;
    assert(zset_add(context, slice_name1, slice_key2, meta2, score2)== LDB_OK);


    const char *zset_key3 = "zset_key3";
    ldb_slice_t *slice_key3 = ldb_slice_create(zset_key3, strlen(zset_key3));
    uint64_t nextver3 = nextver2 + 100000;
    ldb_meta_t *meta3 = ldb_meta_create(0, 0, nextver3);
    int64_t score3 = 57;
    assert(zset_add(context, slice_name1, slice_key3, meta3, score3)== LDB_OK);

    size = 0;
    assert(zset_size(context, slice_name1, &size) == LDB_OK);

    int64_t rank = 0;
    assert(zset_rank(context, slice_name1, slice_key2, 0 , &rank) == LDB_OK);
    printf("zset_rank result=%ld\n", rank);

    const char *zset_key4 = "zset_key4";
    ldb_slice_t *slice_key4 = ldb_slice_create(zset_key4, strlen(zset_key4));
    uint64_t nextver4 = nextver3 + 100000;
    ldb_meta_t *meta4 = ldb_meta_create(0, 0, nextver4);
    int64_t score4= -70;
    assert(zset_add(context, slice_name1, slice_key4, meta4, score4) == LDB_OK);

    score4 = 0;
    assert(zset_get(context, slice_name1, slice_key4, &score4) == LDB_OK);
    assert(score4 == -70);

    const char *zset_key5= "zset_key5";
    ldb_slice_t *slice_key5 = ldb_slice_create(zset_key5, strlen(zset_key5));
    uint64_t nextver5 = nextver4 + 100000;
    ldb_meta_t *meta5= ldb_meta_create(0, 0, nextver5);
    int64_t val = 0;
    assert(zset_incr(context, slice_name1, slice_key5, meta5, -10, &val) == LDB_OK);

    val = 0;
    assert(zset_get(context, slice_name1, slice_key5,  &val) == LDB_OK);
    printf("zset_incr result val=%ld\n", val);

    

    size = 0;
    assert(zset_size(context, slice_name1, &size) == LDB_OK);
    assert(size == 5);

}



int main(int argc, char* argv[]){
    ldb_context_t *context = ldb_context_create("/tmp/testzset", 128, 64);
    assert(context != NULL);

    test_zset(context);


    ldb_context_destroy(context);  
    return 0;
}
