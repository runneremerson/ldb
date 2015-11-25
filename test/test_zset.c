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
    int64_t score1 = -500;

    assert(zset_add(context, slice_name1, slice_key1, meta1, score1) == LDB_OK);

    score1 = 0;
    assert(zset_get(context, slice_name1, slice_key1, &score1) == LDB_OK);
    printf("zset_get %s, result=%ld\n\n", zset_key1, score1);


    uint64_t size = 0;

    const char *zset_key2 = "zset_key2";
    ldb_slice_t *slice_key2 = ldb_slice_create(zset_key2, strlen(zset_key2));
    uint64_t nextver2 = nextver1 + 100000;
    ldb_meta_t *meta2 = ldb_meta_create(0, 0, nextver2);
    int64_t score2 = 15100;
    assert(zset_add(context, slice_name1, slice_key2, meta2, score2)== LDB_OK);

    score2= 0;
    assert(zset_get(context, slice_name1, slice_key2, &score2) == LDB_OK);
    printf("zset_get %s, result=%ld\n\n", zset_key2, score2);


    const char *zset_key3 = "zset_key3";
    ldb_slice_t *slice_key3 = ldb_slice_create(zset_key3, strlen(zset_key3));
    uint64_t nextver3 = nextver2 + 100000;
    ldb_meta_t *meta3 = ldb_meta_create(0, 0, nextver3);
    int64_t score3 = 5700;
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
    assert(zset_incr(context, slice_name1, slice_key5, meta5, 100, &val) == LDB_OK);

    val = 0;
    assert(zset_get(context, slice_name1, slice_key5,  &val) == LDB_OK);
    printf("zset_incr %s, result val=%ld\n", zset_key5, val);

    

    const char *zset_key6 = "zset_key6";
    ldb_slice_t *slice_key6 = ldb_slice_create(zset_key6, strlen(zset_key6));
    uint64_t nextver6 = nextver5 + 100000;
    ldb_meta_t *meta6 = ldb_meta_create(0, 0, nextver6);
    int64_t score6= -170;
    assert(zset_add(context, slice_name1, slice_key6, meta6, score6) == LDB_OK);



    const char *zset_key7 = "zset_key7";
    ldb_slice_t *slice_key7 = ldb_slice_create(zset_key7, strlen(zset_key7));
    uint64_t nextver7 = nextver6 + 100000;
    ldb_meta_t *meta7 = ldb_meta_create(0, 0, nextver7);
    int64_t score7= 7170;
    assert(zset_add(context, slice_name1, slice_key7, meta7, score7) == LDB_OK);

    //size = 0;
    //assert(zset_size(context, slice_name1, &size) == LDB_OK);
    //assert(size == 7);

    printf("\n");

    uint64_t count = 0;
    int64_t sstart = -7000000;
    int64_t send = 0x000000000000FFFF;
    assert(zset_count(context, slice_name1, sstart, send, &count) == LDB_OK);
    printf("zset_count %ld  members between [%ld and %ld)\n\n", count, sstart, send);


    uint64_t nextver8 = nextver7 + 100000;
    uint32_t vercare8 = 0;
    ldb_meta_t *meta8 = ldb_meta_create(vercare8, 0, nextver8);
    assert(zset_del(context, slice_name1, slice_key5, meta8) == LDB_OK);
    printf("after deleting %s\n", zset_key5);


    rank = 0;
    assert(zset_rank(context, slice_name1, slice_key2, 0 , &rank) == LDB_OK);
    printf("zset_rank result=%ld\n", rank);

    assert(zset_count(context, slice_name1, sstart, send, &count) == LDB_OK);
    printf("zset_count %ld  members between [%ld and %ld)\n\n", count, sstart, send);

    uint64_t nextver9 = nextver8 + 100000;
    uint32_t vercare9 = 0;
    ldb_meta_t *meta9 = ldb_meta_create(vercare9, 0, nextver9);
    uint64_t deleted = 0;
    int rank_start = 0, rank_end = 1;
    assert(zset_del_range_by_rank(context, slice_name1, meta9, rank_start, rank_end, &deleted)== LDB_OK); 
    printf("after del_range_by_rank[%d,%d)  deleted = %lu\n",rank_start, rank_end, deleted);

    rank = 0;
    assert(zset_rank(context, slice_name1, slice_key2, 0 , &rank) == LDB_OK);
    printf("zset_rank result=%ld\n", rank);


    

    count = 0;
    assert(zset_count(context, slice_name1, sstart, send, &count) == LDB_OK);
    printf("zset_count %ld  members between [%ld and %ld)\n\n", count, sstart, send);

    uint64_t nextver10 = nextver9 + 100000;
    uint32_t vercare10 = 0;
    ldb_meta_t *meta10 = ldb_meta_create(vercare10, 0, nextver9);
    deleted = 0;
    int64_t score_start = -170, score_end = -69;
    assert(zset_del_range_by_score(context, slice_name1, meta10, score_start, score_end, &deleted)== LDB_OK); 
    printf("after del_range_by_score[%ld,%ld)  deleted = %lu\n",score_start, score_end,  deleted);


    rank = 0;
    assert(zset_rank(context, slice_name1, slice_key2, 0 , &rank) == LDB_OK);
    printf("zset_rank result=%ld\n", rank);


    count = 0;
    assert(zset_count(context, slice_name1, sstart, send, &count) == LDB_OK);
    printf("zset_count %ld  members between [%ld and %ld)\n\n", count, sstart, send);


    //range and scan
    ldb_list_t *keylist11 = NULL, *metalist11 = NULL;
    assert(zset_range(context, slice_name1, 0, -1, 0, &keylist11, &metalist11) == LDB_OK); 
    ldb_list_iterator_t *iterator11 = ldb_list_iterator_create(keylist11);
    while(1){
        ldb_list_node_t *node_key = ldb_list_next(&iterator11);
        if(node_key == NULL){
            break;
        }
        printf("zset_range result key=%s, score=%ld\n", ldb_slice_data(node_key->data_), node_key->value_);  
    }

    ldb_list_t *keylist12 = NULL, *metalist12 = NULL;
    assert(zset_scan(context, slice_name1, 5699, 20000, 0, &keylist12, &metalist12) == LDB_OK); 
    ldb_list_iterator_t *iterator12 = ldb_list_iterator_create(keylist12);
    while(1){
        ldb_list_node_t *node_key = ldb_list_next(&iterator12);
        if(node_key == NULL){
            break;
        }
        printf("zset_scan result key=%s, score=%ld\n", ldb_slice_data(node_key->data_), node_key->value_);  
    }


}



int main(int argc, char* argv[]){
    ldb_context_t *context = ldb_context_create("/tmp/testzset", 128, 64);
    assert(context != NULL);
    ldb_context_release_recovering_snapshot(context);
    

    test_zset(context);


    ldb_context_destroy(context);  
    return 0;
}
