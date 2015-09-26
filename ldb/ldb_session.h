#ifndef LDB_SESSION_H
#define LDB_SESSION_H

#include "ldb_context.h"

#include "util/cgo_util_base.h"


#include <stdint.h>


typedef struct value_item_t{
  uint64_t version_;
  size_t data_len_;
  char* data_;
} value_item_t;

void fill_value_item(value_item_t* item, uint64_t version, const char* data, size_t size);

void free_value_item(value_item_t* item);

void destroy_value_item_array( value_item_t* array, size_t size);

value_item_t* create_value_item_array( size_t size);



int set_ldb_signal_handler(const char* name);



//string
int ldb_set(ldb_context_t* context,
            char* key, 
            size_t keylen, 
            uint64_t lastver, 
            int vercare, 
            long exptime, 
            value_item_t* item, 
            int en);
int ldb_mset(ldb_context_t* context, 
             uint64_t lastver, 
             int vercare, 
             long exptime, 
             GoByteSlice* keyvals, 
             GoUint64Slice* versions, 
             int length,
             GoUint64Slice* results,
             int en);
int ldb_expire(ldb_context_t* context,
              char* key,
              size_t keylen,
              long exptime,
              uint64_t expversion);
int ldb_pexpire(ldb_context_t* context,
               char* key,
               size_t keylen,
               long exptime,
               uint64_t expversion); 
int ldb_ttl(ldb_context_t* context,
           char* key,
           size_t keylen,
           long* remain);

int ldb_pttl(ldb_context_t* context,
            char* key,
            size_t keylen,
            long* remain);

int ldb_persist(ldb_context_t* context,
               char* key,
               size_t keylen,
               uint64_t version);

int ldb_exists(ldb_context_t* context,
              char* key,
              size_t keylen);

int ldb_get(ldb_context_t* context, 
            char* key, 
            size_t keylen, 
            value_item_t** item);

int ldb_mget(ldb_context_t* context,
             GoByteSlice* slice,
             int length,
             GoByteSliceSlice* items,
             GoUint64Slice* versions,
             int* itemnum);

int ldb_del(ldb_context_t* context, 
            char* key, 
            size_t keylen, 
            int vercare, 
            uint64_t version);

int ldb_incrby(ldb_context_t* context,
               char* key,
               size_t keylen,
               uint64_t lastver,
               int vercare,
               long exptime,
               uint64_t version,
               int64_t initval,
               int64_t by,
               int64_t* reault);

//hash
int ldb_hgetall(ldb_context_t* context,
                char* name,
                size_t namelen,
                value_item_t** items,
                size_t* itemnum);

int ldb_hkeys(ldb_context_t* context,
              char* key,
              size_t keylen,
              value_item_t** items,
              size_t* itemnum);

int ldb_hvals(ldb_context_t* context,
              char* key,
              size_t keylen,
              value_item_t** items,
              size_t* itemnum);

int ldb_hget(ldb_context_t* context,
             GoByteSlice* key,
             GoByteSlice* field,
             value_item_t** item);

int ldb_hmget(ldb_context_t* context,
              char* key,
              size_t keylen,
              value_item_t* fields,
              size_t fieldnum,
              value_item_t** items,
              size_t* itemnum);

int ldb_hincrby(ldb_context_t* context,
                char* key,
                size_t keylen,
                uint64_t lastver,
                int vercare,
                long exptime,
                value_item_t* item,
                long long by,
                long long* result);

int ldb_hset(ldb_context_t* context,
             char* key,
             size_t keylen,
             uint64_t lastver,
             int vercare,
             long exptime,
             char* field,
             size_t fieldlen,
             value_item_t* item);

int ldb_hmset(ldb_context_t* context,
              char* key,
              size_t keylen,
              uint64_t lastver,
              int vercare,
              long exptime,
              value_item_t* items,
              size_t itemnum,
              int** results);

int ldb_hdel(ldb_context_t* context,
             char* key,
             size_t keylen,
             uint64_t lastver,
             int vercare,
             long exptime,
             value_item_t* items,
             size_t itemnum,
             int** results);

int ldb_hlen(ldb_context_t* context,
             char* key,
             size_t keylen,
             long long* length);

int ldb_hexists(ldb_context_t* context,
                char* key,
                size_t keylen,
                char* field,
                size_t fieldlen);





//zset
int ldb_zscore(ldb_context_t* context,
              char* name,
              size_t namelen,
              char* key,
              size_t keylen,
              int64_t* score);

int ldb_zrange_by_rank(ldb_context_t* context,
                       char* name,
                       size_t namelen,
                       int rank_start,
                       int rank_end,
                       value_item_t** items,
                       size_t* itemnum,
                       int64_t** scores,
                       size_t* scorenum,
                       int withscore,
                       int reverse);


int ldb_zrange_by_score(ldb_context_t* context,
                        char* name,
                        size_t namelen,
                        int64_t score_start,
                        int64_t score_end,
                        value_item_t** items,
                        size_t* itemnum,
                        int64_t** scores,
                        size_t* scorenum,
                        int reverse,
                        int withscore);

int ldb_zadd(ldb_context_t* context,
             char* name,
             size_t namelen,
             uint64_t version,
             int vercare,
             long exptime,
             value_item_t* keys,
             int64_t* scores,
             size_t keynum,
             int** retvals);

int ldb_zrank(ldb_context_t* context,
              char* name,
              size_t namelen,
              char* key,
              size_t keylen,
              int reverse,
              uint64_t* rank);

int ldb_zcount(ldb_context_t* context,
               char* name,
               size_t namelen,
               int64_t score_start,
               int64_t score_end,
               uint64_t* count);

int ldb_zincrby(ldb_context_t* context,
                char* name,
                size_t namelen,
                uint64_t lastver,
                int vercare,
                long exptime,
                value_item_t* item,
                int64_t by,
                int64_t* score);

int ldb_zrem(ldb_context_t* context,
             char* name,
             size_t namelen,
             uint64_t lastver,
             int vercare,
             long exptime,
             value_item_t* items,
             size_t itemnum,
             int**retvals);

int ldb_zrem_by_rank(ldb_context_t* context,
                     char* name,
                     size_t namelen,
                     uint64_t nextver,
                     int vercare,
                     long exptime,
                     int rank_start,
                     int rank_end,
                     uint64_t* deleted);

int ldb_zrem_by_score(ldb_context_t* context,
                      char* name,
                      size_t namelen,
                      uint64_t nextver,
                      int vercare,
                      long exptime,
                      int64_t score_start,
                      int64_t score_end,
                      uint64_t* deleted); 

int ldb_zcard(ldb_context_t* context,
              char* name,
              size_t namelen,
              uint64_t* size);





#endif //LDB_SESSION_H
