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
    ldb_list_t *datalist, *metalist, *retlist = NULL;
    datalist = ldb_list_create();
    metalist = ldb_list_create();


    char *cmkey1 = "mkey1";
    char *cmval1 = "mval1";
    uint64_t mnextver1 = nextver1 + 100000;
    
    ldb_slice_t *slice_key = ldb_slice_create(cmkey1, strlen(cmkey1));
    ldb_list_node_t *node_key = ldb_list_node_create();
    node_key->type_ = LDB_LIST_NODE_TYPE_SLICE;
    node_key->data_ = slice_key;;
    rpush_ldb_list_node(datalist, node_key);

    ldb_slice_t *slice_val = ldb_slice_create(cmval1, strlen(cmval1));
    ldb_list_node_t *node_val = ldb_list_node_create();
    node_val->type_ = LDB_LIST_NODE_TYPE_SLICE;
    node_val->data_ = slice_val;
    rpush_ldb_list_node(datalist, node_val);

    ldb_list_node_t *node_meta = ldb_list_node_create();
    node_meta->type_ = LDB_LIST_NODE_TYPE_META;
    node_meta->data_ = ldb_meta_create(0, 0, mnextver1);
    rpush_ldb_list_node(metalist, node_meta);


    char *cmkey2 = "mkey2";
    char *cmval2 = "mval2";
    uint64_t mnextver2 = mnextver1 + 100000;
 
    slice_key = ldb_slice_create(cmkey2, strlen(cmkey2));
    node_key = ldb_list_node_create();
    node_key->type_ = LDB_LIST_NODE_TYPE_SLICE;
    node_key->data_ = slice_key;;
    rpush_ldb_list_node(datalist, node_key);

    slice_val = ldb_slice_create(cmval2, strlen(cmval2));
    node_val = ldb_list_node_create();
    node_val->type_ = LDB_LIST_NODE_TYPE_SLICE;
    node_val->data_ = slice_val;
    rpush_ldb_list_node(datalist, node_val);

    node_meta = ldb_list_node_create();
    node_meta->type_ = LDB_LIST_NODE_TYPE_META;
    node_meta->data_ = ldb_meta_create(0, 0, mnextver2);
    rpush_ldb_list_node(metalist, node_meta);

    assert(string_mset(context, datalist, metalist, &retlist) == LDB_OK);
    ldb_list_iterator_t *iterator = ldb_list_iterator_create(retlist);
    while(1){
        ldb_list_node_t *node = ldb_list_next(&iterator);
        if(node == NULL){
            break;
        }
        printf("mset result %d\n",(int)(node->value_));
    }
    ldb_list_iterator_destroy(iterator);
    ldb_list_destroy(metalist);
    ldb_list_destroy(retlist);

    //test msetnx
    uint64_t mnextver3 = mnextver2 + 100000;
    uint64_t mnextver4 = mnextver3 + 100000;
    ldb_list_t *retlistnx = ldb_list_create();
    ldb_list_t *metalistnx = ldb_list_create();

    node_meta = ldb_list_node_create();
    node_meta->type_ = LDB_LIST_NODE_TYPE_META;
    node_meta->data_ = ldb_meta_create(0, 0, mnextver3);
    rpush_ldb_list_node(metalistnx, node_meta);

    node_meta = ldb_list_node_create();
    node_meta->type_ = LDB_LIST_NODE_TYPE_META;
    node_meta->data_ = ldb_meta_create(0, 0, mnextver4);
    rpush_ldb_list_node(metalistnx, node_meta);

    
    assert(string_msetnx(context, datalist, metalistnx, &retlistnx) == LDB_OK);
    ldb_list_iterator_t *iteratornx = ldb_list_iterator_create(retlistnx);
    while(1){
        ldb_list_node_t *node = ldb_list_next(&iteratornx);
        if(node == NULL){
            break;
        }
        printf("msetnx result %d\n",(int)(node->value_));
    }
    ldb_list_iterator_destroy(iteratornx);
    ldb_list_destroy(metalistnx);
    ldb_list_destroy(retlistnx);
    ldb_list_destroy(datalist);

    //mget
    ldb_list_t *keylist1, *vallist1, *metalist1 = NULL;
    keylist1 = ldb_list_create();

    slice_key = ldb_slice_create(cmkey1, strlen(cmkey1));
    node_key = ldb_list_node_create();
    node_key->type_ = LDB_LIST_NODE_TYPE_SLICE;
    node_key->data_ = slice_key;;
    rpush_ldb_list_node(keylist1, node_key);

    slice_key = ldb_slice_create(cmkey2, strlen(cmkey2));
    node_key = ldb_list_node_create();
    node_key->type_ = LDB_LIST_NODE_TYPE_SLICE;
    node_key->data_ = slice_key;
    rpush_ldb_list_node(keylist1, node_key);

    assert(string_mget(context, keylist1, &vallist1, &metalist1) == LDB_OK);

    ldb_list_iterator_t *valiterator1 = ldb_list_iterator_create(vallist1);
    ldb_list_iterator_t *metiterator1 = ldb_list_iterator_create(metalist1);
    while(1){
        ldb_list_node_t* node_val = ldb_list_next(&valiterator1);
        if(node_val == NULL){
            break;
        }
        ldb_list_node_t* node_met = ldb_list_next(&metiterator1);
        if(node_met == NULL){
            break;
        }
        assert(node_val->type_ == LDB_LIST_NODE_TYPE_SLICE);
        assert(node_met->type_ == LDB_LIST_NODE_TYPE_META);
        printf("mget val=%s nextver=%lu\n",
               ldb_slice_data((ldb_slice_t*)(node_val->data_)), 
               ldb_meta_nextver((ldb_meta_t*)(node_met->data_))
              );
    }
    ldb_list_iterator_destroy(valiterator1);
    ldb_list_iterator_destroy(metiterator1);

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

