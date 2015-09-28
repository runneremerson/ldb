#ifndef LDB_LEVELDB_DEFINE_H
#define LDB_LEVELDB_DEFINE_H

#define LDB_VALUE_TYPE_DEL                   0x0
#define LDB_VALUE_TYPE_VAL                   0x1
#define LDB_VALUE_TYPE_EXP                   0x2

#define LDB_KEY_META_SIZE                    28
#define LDB_VAL_META_SIZE                    9
#define LDB_VAL_TYPE_SIZE                    1

#define LDB_DATA_TYPE_KEY_LEN_MAX            255
#define LDB_DATA_TYPE_ZSET_SCORE_WIDTH       9

#define LDB_DATA_TYPE_KV                     "k"
#define LDB_DATA_TYPE_HASH                   "h" 
#define LDB_DATA_TYPE_HSIZE                  "H"
#define LDB_DATA_TYPE_ZSET                   "s"
#define LDB_DATA_TYPE_ZSCORE                 "z"
#define LDB_DATA_TYPE_ZSIZE                  "Z"


#define LDB_LIST_NODE_TYPE_NONE              0
#define LDB_LIST_NODE_TYPE_BASE              1
#define LDB_LIST_NODE_TYPE_SLICE             2
#define LDB_LIST_NODE_TYPE_META              3



#define	LDB_OK_HYPERLOGLOG_NOT_EXIST         8
#define lDB_OK_HYPERLOGLOG_EXIST             7
#define LDB_OK_SUB_NOT_EXIST                 6
#define LDB_OK_BUT_ALREADY_EXIST		     5
#define LDB_ERR_EXPIRE_TIME_OUT              4
#define LDB_OK_NOT_EXIST                     3
#define LDB_OK_BUT_CONE                      2
#define LDB_OK_BUT_CZERO                     1
#define LDB_OK                               0
#define LDB_ERR                             -1
#define LDB_ERR_LENGTHZERO                  -2
#define LDB_ERR_REACH_MAXMEMORY             -3
#define LDB_ERR_UNKNOWN_COMMAND             -4
#define LDB_ERR_WRONG_NUMBER_ARGUMENTS      -5
#define LDB_ERR_OPERATION_NOT_PERMITTED     -6
#define LDB_ERR_QUEUED                      -7
#define LDB_ERR_LOADINGERR                  -8
#define LDB_ERR_FORBIDDEN_ABOUT_PUBSUB      -9
#define LDB_ERR_FORBIDDEN_INFO_SLAVEOF      -10
#define LDB_ERR_VERSION_ERROR               -11
#define LDB_OK_RANGE_HAVE_NONE			    -12
#define LDB_ERR_WRONG_TYPE_ERROR            -13
#define LDB_ERR_CNEGO_ERROR                 -14
#define LDB_ERR_IS_NOT_NUMBER               -15
#define LDB_ERR_INCDECR_OVERFLOW            -16
#define LDB_ERR_IS_NOT_INTEGER              -17
#define LDB_ERR_MEMORY_ALLOCATE_ERROR       -18
#define LDB_ERR_OUT_OF_RANGE                -19
#define LDB_ERR_IS_NOT_DOUBLE               -20
#define LDB_ERR_SYNTAX_ERROR                -21
#define LDB_ERR_NAMESPACE_ERROR             -22
#define LDB_ERR_DATA_LEN_LIMITED            -23
#define LDB_SAME_OBJECT_ERR                 -24
#define LDB_ERR_BUCKETID                    -25
#define LDB_ERR_NOT_SET_EXPIRE              -26

#endif //LDB_LEVELDB_DEFINE_H

