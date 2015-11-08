#include "ldb/t_hash.h"
#include "ldb/ldb_define.h"
#include "ldb/util.h"

#include <assert.h>
#include <string.h>
#include <stdio.h>
#include <stdint.h>

static void test_hash(ldb_context_t* context){
    const char *hash_name1 = "hash_name";
    const char *hash_key1 = "hash_key1";
    ldb_slice_t *slice_name1 = ldb_slice_create(hash_name1, strlen(hash_name1));
    ldb_slice_t *slice_key1 = ldb_slice_create(hash_key1, strlen(hash_key1));
    ldb_slice_t *slice_val1 = NULL;
    ldb_meta_t *meta1 = NULL;

    uint64_t nextver1 = time_ms();
    meta1 = ldb_meta_create(0, 0, nextver1);
    const char *hash_val1 = "hash_val1";
    slice_val1 = ldb_slice_create(hash_val1, strlen(hash_val1));
    assert(hash_set(context, slice_name1, slice_key1, slice_val1, meta1) == LDB_OK);

    ldb_slice_destroy(slice_val1);
    ldb_meta_destroy(meta1);

    assert(hash_get(context, slice_name1, slice_key1, &slice_val1, &meta1) == LDB_OK);
    assert(compare_with_length(ldb_slice_data(slice_val1), ldb_slice_size(slice_val1), hash_val1, strlen(hash_val1))==0);

    assert(ldb_meta_nextver(meta1)==nextver1);

    const char *hash_key2 = "hash_key2";
    ldb_slice_t *slice_key2 = ldb_slice_create(hash_key2, strlen(hash_key2));
    assert(hash_exists(context, slice_name1, slice_key2) == LDB_OK_NOT_EXIST);

    const char *hash_val2 = "hash_val2";
    ldb_slice_t *slice_val2 = ldb_slice_create(hash_val2, strlen(hash_val2));
    uint64_t nextver2 = nextver1 + 100000;
    ldb_meta_t *meta2 = ldb_meta_create(0, 0, nextver2);

    assert(hash_set(context, slice_name1, slice_key2, slice_val2, meta2) == LDB_OK);

    uint64_t length = 0;
    assert(hash_length(context, slice_name1, &length) == LDB_OK);
    assert(length == 2);

    uint64_t nextver3 = nextver2 + 100000;
    ldb_meta_t *meta3 = ldb_meta_create(0, 0, nextver3);
    assert(hash_del(context, slice_name1, slice_key2, meta3) == LDB_OK);
    
    assert(hash_length(context, slice_name1, &length) == LDB_OK);
    assert(length == 1);

    assert(hash_exists(context, slice_name1, slice_key2) == LDB_OK_NOT_EXIST);


    const char *hash_val4 = "hash_val4";
    ldb_slice_t *slice_val4 = ldb_slice_create(hash_val4, strlen(hash_val4));
    uint64_t nextver4 = nextver3 + 100000;
    ldb_meta_t *meta4 = ldb_meta_create(0, 0, nextver4);
    assert(hash_setnx(context, slice_name1, slice_key1, slice_val4, meta4) == LDB_OK_BUT_ALREADY_EXIST);

    const char *hash_name2 = "hash_name2";
    ldb_slice_t *slice_name2 = ldb_slice_create(hash_name2, strlen(hash_name2));


    int64_t val = 0;
    const char *hash_key5 = "hash_key5";
    ldb_slice_t *slice_key5 = ldb_slice_create(hash_key5, strlen(hash_key5));
    uint64_t nextver5 = nextver4 + 100000;
    ldb_meta_t *meta5 = ldb_meta_create(0, 0, nextver5);

    assert(hash_incr(context, slice_name2, slice_key5, meta5, 100, &val)== LDB_OK);
    printf("val=%ld\n", val);



    uint64_t nextver6 = nextver5 + 100000;
    ldb_meta_t *meta6 = ldb_meta_create(0, 0, nextver6);
    assert(hash_incr(context, slice_name2, slice_key5, meta6, -150, &val) == LDB_OK);
    printf("val=%ld\n", val);

    assert(hash_exists(context, slice_name2, slice_key5) == LDB_OK);
    assert(hash_length(context, slice_name2, &length) == LDB_OK);
    assert(length ==  1);

    ldb_slice_destroy(slice_name1);
    ldb_slice_destroy(slice_name2);
    ldb_slice_destroy(slice_key1);
    ldb_slice_destroy(slice_val1);
    ldb_slice_destroy(slice_key2);
    ldb_slice_destroy(slice_val2);
    ldb_meta_destroy(meta1); 


    ldb_list_t *datalist1, *metalist1, *retlist1 = NULL;
    datalist1 = ldb_list_create();
    metalist1 = ldb_list_create();

    const char *hash_name3 = "hash_name3";
    ldb_slice_t *slice_name3 = ldb_slice_create(hash_name3, strlen(hash_name3));
    const char *hash_key6 = "hash_key6";
    ldb_slice_t *slice_key6 = ldb_slice_create(hash_key6, strlen(hash_key6));
    const char *hash_val6 = "hash_val6";
    ldb_slice_t *slice_val6 = ldb_slice_create(hash_val6, strlen(hash_val6));
    uint64_t mnextver1 = nextver6 + 100000;
    ldb_meta_t *meta7 = ldb_meta_create(0, 0, mnextver1);

    ldb_list_node_t *node_key = ldb_list_node_create();
    node_key->type_ = LDB_LIST_NODE_TYPE_SLICE;
    node_key->data_ = slice_key6;;
    rpush_ldb_list_node(datalist1, node_key);

    ldb_list_node_t *node_val = ldb_list_node_create();
    node_val->type_ = LDB_LIST_NODE_TYPE_SLICE;
    node_val->data_ = slice_val6;
    rpush_ldb_list_node(datalist1, node_val);

    ldb_list_node_t *node_meta = ldb_list_node_create();
    node_meta->type_ = LDB_LIST_NODE_TYPE_META;
    node_meta->data_ = meta7;
    rpush_ldb_list_node(metalist1, node_meta);

    const char *hash_key7 = "hash_key7";
    ldb_slice_t *slice_key7 = ldb_slice_create(hash_key7, strlen(hash_key7));
    const char *hash_val7 = "hash_val7";
    ldb_slice_t *slice_val7 = ldb_slice_create(hash_val7, strlen(hash_val7));
    uint64_t mnextver2 = mnextver1 + 100000;
    ldb_meta_t *meta8 = ldb_meta_create(0, 0, mnextver2);


    node_key = ldb_list_node_create();
    node_key->type_ = LDB_LIST_NODE_TYPE_SLICE;
    node_key->data_ = slice_key7;;
    rpush_ldb_list_node(datalist1, node_key);

    node_val = ldb_list_node_create();
    node_val->type_ = LDB_LIST_NODE_TYPE_SLICE;
    node_val->data_ = slice_val7;
    rpush_ldb_list_node(datalist1, node_val);

    node_meta = ldb_list_node_create();
    node_meta->type_ = LDB_LIST_NODE_TYPE_META;
    node_meta->data_ = meta8;
    rpush_ldb_list_node(metalist1, node_meta);

    assert(hash_mset(context, slice_name3, datalist1, metalist1, &retlist1) == LDB_OK);

    ldb_list_iterator_t *iterator1 = ldb_list_iterator_create(retlist1);
    while(1){
        ldb_list_node_t *node = ldb_list_next(&iterator1);
        if(node == NULL){
            break;
        }
        printf("hmset reault %d\n", (int)(node->value_));
    }
    ldb_list_iterator_destroy(iterator1);
    ldb_list_destroy(datalist1);
    ldb_list_destroy(metalist1);
    ldb_list_destroy(retlist1);

    uint64_t mlength = 0;
    assert(hash_length(context, slice_name3, &mlength) == LDB_OK);
    printf("slice_name3 length=%lu\n", mlength);
    assert(mlength == 2);


    ldb_list_t *vallist2, *metalist2, *keylist2 = NULL;
    keylist2 = ldb_list_create();

    const char *hash_key8 = "hash_key6";
    ldb_slice_t *slice_key8 = ldb_slice_create(hash_key8, strlen(hash_key8));
    node_key = ldb_list_node_create();
    node_key->type_ = LDB_LIST_NODE_TYPE_SLICE;
    node_key->data_ = slice_key8;
    rpush_ldb_list_node(keylist2, node_key);

    const char *hash_key9 = "hash_key7";
    ldb_slice_t *slice_key9 = ldb_slice_create(hash_key9, strlen(hash_key9));
    node_key = ldb_list_node_create();
    node_key->type_ = LDB_LIST_NODE_TYPE_SLICE;
    node_key->data_ = slice_key9;
    rpush_ldb_list_node(keylist2, node_key);

    assert(hash_mget(context, slice_name3, keylist2, &vallist2, &metalist2) == LDB_OK);

    ldb_list_destroy(vallist2);
    ldb_list_destroy(keylist2);
    ldb_list_destroy(metalist2);
}





int main(int argc, char* argv[]){
    ldb_context_t *context = ldb_context_create("/tmp/testhash", 128, 64);
    ldb_context_create_recovering_snapshot(context);
    ldb_context_release_recovering_snapshot(context);
    assert(context != NULL);

    test_hash(context);



    ldb_context_destroy(context);  
    return 0;
}
