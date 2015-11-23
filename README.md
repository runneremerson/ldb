# ldb
This is a storage engine of eventual consistency based on leveldb-1.18.

It supports many idempotent commands such as: 

set,get,exists.....
hget, hset, hgetall......
sadd, srem, smembers.....
zadd, zrem, zrank, zscore....

and their associated data structs like: string, set, hash, zset.

The implementation of eventual consistency inspires by vector clock which is represented by a uint64_t.


