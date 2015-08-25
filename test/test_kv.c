#include "storage/t_kv.h"
#include "storage/ldb_define.h"

#include <assert.h>
#include <time.h>
#include <string.h>


int main(int argc, char* argv[]){
  ldb_context_t *context = ldb_context_create("/tmp/testdb", 128, 64);
  assert(context != NULL);
  char *ckey = "key1";
  char *cval = "val1";
  ldb_slice_t *key1 = ldb_slice_create(ckey, strlen(ckey));
  ldb_slice_t *val1 = ldb_slice_create(cval, strlen(cval));
  ldb_meta_t *meta1 = ldb_meta_create(0, 0, time(NULL)); 

  assert(string_set(context, key1, val1, meta1) == LDB_OK);

  ldb_slice_destroy(val1);
  ldb_meta_destroy(meta1); 

  assert(string_get(context, key1, &val1)==LDB_OK); 

  ldb_slice_destroy(val1);
  ldb_slice_destroy(key1);

  ldb_context_destroy(context);  
  return 0;
}
