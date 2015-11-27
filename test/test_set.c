#include "ldb/t_set.h"
#include "ldb/ldb_define.h"
#include "ldb/util.h"

#include <leveldb/c.h>
#include <assert.h>
#include <string.h>
#include <stdio.h>

static void test_set(ldb_context_t* context){

    const char* set_name1 = "set_name1";
    ldb_slice_t *slice_name1 = ldb_slice_create(set_name1, strlen(set_name1));
    const char* set_key1 = "set_key1";
    ldb_slice_t *slice_key1 = ldb_slice_create(set_key1, strlen(set_key1));
    uint64_t nextver1 =time_ms();
    ldb_meta_t *meta1 = ldb_meta_create(0, 0, nextver1);

    assert(set_add(context, slice_name1, slice_key1, meta1) == LDB_OK);

    const char* set_key2 = "set_key2";
    ldb_slice_t *slice_key2 = ldb_slice_create(set_key2, strlen(set_key2));
    uint64_t nextver2 =nextver1 + 100000;
    ldb_meta_t *meta2 = ldb_meta_create(0, 0, nextver2);

    assert(set_add(context, slice_name1, slice_key2, meta2) == LDB_OK);

    const char* set_key3 = "set_key3";
    ldb_slice_t *slice_key3 = ldb_slice_create(set_key3, strlen(set_key3));
    uint64_t nextver3 =nextver2 + 100000;
    ldb_meta_t *meta3 = ldb_meta_create(0, 0, nextver3);

    assert(set_add(context, slice_name1, slice_key3, meta3) == LDB_OK);

    uint64_t length = 0;
    assert(set_card(context, slice_name1, &length) == LDB_OK);

    assert(length == 3);

    uint64_t nextver4 = nextver3 + 100000;
    ldb_meta_t *meta4 = ldb_meta_create(0, 0, nextver4);
    assert(set_rem(context, slice_name1, slice_key3, meta4) == LDB_OK);
    assert(set_ismember(context, slice_name1, slice_key3) == LDB_OK_NOT_EXIST);


    length = 0;
    assert(set_card(context, slice_name1, &length) == LDB_OK);
    assert(length == 2);



    ldb_list_t *keylist5 = NULL, *metalist5 = NULL;
    assert(set_members(context, slice_name1, &keylist5, &metalist5) == LDB_OK);
    ldb_list_iterator_t *iterator5 = ldb_list_iterator_create(keylist5);
    while(1){
        ldb_list_node_t *node_key = ldb_list_next(&iterator5);
        if(node_key == NULL){
            break;
        }
        printf("set_members result key=%s\n", ldb_slice_data(node_key->data_));  
    }

    ldb_slice_t *slice_key6 = NULL;
    uint64_t nextver6 = nextver4 + 100000;
    ldb_meta_t *meta6 = ldb_meta_create(0, 0, nextver6);
    assert(set_pop(context, slice_name1, meta6, &slice_key6) == LDB_OK);
    printf("set_pop result key=%s\n", ldb_slice_data(slice_key6));


    ldb_list_t *keylist7 = NULL, *metalist7 = NULL;
    assert(set_members(context, slice_name1, &keylist7, &metalist7) == LDB_OK);
    ldb_list_iterator_t *iterator7 = ldb_list_iterator_create(keylist7);
    while(1){
        ldb_list_node_t *node_key = ldb_list_next(&iterator7);
        if(node_key == NULL){
            break;
        }
        printf("set_members result key=%s\n", ldb_slice_data(node_key->data_));  
        assert(set_ismember(context, slice_name1, node_key->data_)==LDB_OK);
    }
}



int main(int argc, char* argv[]){
    ldb_context_t *context = ldb_context_create("/tmp/testset", 128, 64, 1);
    ldb_context_release_recovering_snapshot(context);
    assert(context != NULL);
    

    test_set(context);


    ldb_context_destroy(context);  
    return 0;
}
