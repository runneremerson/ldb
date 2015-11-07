#include "ldb/t_hash.h"
#include "ldb/ldb_define.h"
#include "ldb/util.h"

#include <assert.h>
#include <string.h>
#include <stdio.h>

static void test_hash(ldb_context_t* context){
}



int main(int argc, char* argv[]){
    ldb_context_t *context = ldb_context_create("/tmp/testhash", 128, 64);
    assert(context != NULL);

    test_hash(context);



    ldb_context_destroy(context);  
    return 0;
}
