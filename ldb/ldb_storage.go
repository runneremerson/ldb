package ldb

import "unsafe"

const (
	STORAGE_OK_SUB_NOT_EXIST            = 6
	STORAGE_OK_BUT_ALREADY_EXIST        = 5
	STORAGE_ERR_EXPIRE_TIME_OUT         = 4
	STORAGE_OK_NOT_EXIST                = 3
	STORAGE_OK_BUT_CONE                 = 2
	STORAGE_OK_BUT_CZERO                = 1
	STORAGE_OK                          = 0
	STORAGE_ERR                         = -1
	STORAGE_ERR_LENGTHXERO              = -2
	STORAGE_ERR_REACH_MAXMEMORY         = -3
	STORAGE_ERR_UNKNOWN_COMMAND         = -4
	STORAGE_ERR_WRONG_NUMBER_ARGUMENTS  = -5
	STORAGE_ERR_OPERATION_NOT_PERMITTED = -6
	STORAGE_ERR_QUEUED                  = -7
	STORAGE_ERR_LOADINGERR              = -8
	STORAGE_ERR_FORBIDDEN_ABOUT_PUBSUB  = -9
	STORAGE_ERR_FORBIDDEN_INFO_SLAVEOF  = -10
	STORAGE_ERR_VERSION_ERROR           = -11
	STORAGE_OK_RANGE_HAVE_NONE          = -12
	STORAGE_ERR_WRONG_TYPE_ERROR        = -13
	STORAGE_ERR_CNEGO_ERROR             = -14
	STORAGE_ERR_IS_NOT_NUMBER           = -15
	STORAGE_ERR_INCDECR_OVERFLOW        = -16
	STORAGE_ERR_IS_NOT_INTEGER          = -17
	STORAGE_ERR_MEMORY_ALLOCATE_ERROR   = -18
	STORAGE_ERR_OUT_OF_RANGE            = -19
	STORAGE_ERR_IS_NOT_DOUBLE           = -20
	STORAGE_ERR_SYNTAX_ERROR            = -21
	STORAGE_ERR_NAMESPACE_ERROR         = -22
	STORAGE_ERR_DATA_LEN_LIMITED        = -23
	STORAGE_SAME_OBJECT_ERR             = -24
	STORAGE_ERR_BUCKETID                = -25
	STORAGE_ERR_NOT_SET_EXPIRE          = -26
)

type SetOpt int

const (
	IS_NOT_EXIST            SetOpt = 0 //nx
	IS_EXIST                       = 1
	IS_NOT_EXIST_AND_EXPIRE        = 2
	IS_EXIST_AND_EXPIRE            = 3
)

var CNULL = unsafe.Pointer(uintptr(0))

type StorageVersionType uint64

type StorageMetaData struct {
	Lastversion StorageVersionType
	Versioncare int
	Expiretime  uint32
}

var STORAGE_VERSION_CARE_EQUAL = 0x00000001
var STORAGE_VERSION_CARE_CLEAR = 0x00000002

func DefaultMetaData() StorageMetaData {
	return StorageMetaData{
		0,
		0,
		0,
	}
}

type StorageValueData struct {
	Value   string
	Version StorageVersionType
}

// define empty StorageValueData:
// value: ""
// version: 0
func EmptyValueData() StorageValueData {
	return StorageValueData{
		"",
		0,
	}
}

func (value *StorageValueData) IsNil() bool {
	if value.Version == StorageVersionType(0) {
		return true
	}
	return false
}

type StorageByteValueData struct {
	Value   []byte
	Version StorageVersionType
}

func EmptyByteValueData() StorageByteValueData {
	return StorageByteValueData{
		nil,
		0,
	}
}

func (value *StorageByteValueData) IsNil() bool {
	if value.Value == nil {
		return true
	}
	return false
}

type StorageScoreValueData struct {
	Value   string
	Version StorageVersionType
	Score   int64
}

type StorageStatusItem struct {
	Name  string
	Value string
}

type IStorageManager interface {

	/// common

	SetExceptionTrace(programName string)

	Expire(key string, seconds uint32, version StorageVersionType) int

	PExpire(key string, seconds uint32, version StorageVersionType) int

	Persist(key string, version StorageVersionType) int

	Exists(key string) int

	Ttl(key string, time_remain *uint32) int

	PTtl(key string, time_remain *uint32) int

	Type(key string) (string, int)

	Rename(src_key, des_key string, version StorageVersionType, nx int) int

	/// string

	Set(key string, value StorageValueData, meta StorageMetaData, en SetOpt) int

	SetEx(key string, value StorageValueData, expiretime uint32) int

	SetWithSecond(key string, value StorageValueData, meta StorageMetaData, seconds []string) int

	Get(key string) (int, StorageByteValueData)

	GetWithByte(key []byte) (int, StorageByteValueData)

	GetSet(key string, value *StorageValueData, meta StorageMetaData) int

	Del(keys []string, versions []StorageVersionType, meta StorageMetaData) (int, []int)

	Incr(key string, meta StorageMetaData, version StorageVersionType, ret_value *int64) int

	Incrby(key string, meta StorageMetaData, version StorageVersionType, incr_value int64, ret_value *int64) int

	Decr(key string, meta StorageMetaData, version StorageVersionType, ret_value *int64) int

	Decrby(key string, meta StorageMetaData, version StorageVersionType, decr_value int64, ret_value *int64) int

	IncrDecr(key string, meta StorageMetaData, version StorageVersionType, add_value int64, ret_value *int64) int

	MSet(keyValues map[string]StorageValueData, meta StorageMetaData, en SetOpt) (int, map[string]int, []uint64)

	MSetWithByte(keyVals [][]byte, versions []StorageVersionType, meta StorageMetaData, en SetOpt, retArr []uint64) int

	MGet(keys []string) (int, []StorageByteValueData)

	MGetWithByte2(keys [][]byte, results *[][]byte, versions []uint64) int

	/// hash

	HSet(key, field string, value StorageValueData, meta StorageMetaData) int

	HGet(key, field string) (int, StorageByteValueData)

	HGetWithByte(key, field []byte) (int, StorageByteValueData)

	HmSet(key string, values map[string]StorageValueData, meta StorageMetaData) (int, map[string]int)

	HmGet(key string, fields []string) (int, []StorageByteValueData)

	HDel(key string, fields map[string]StorageValueData, meta StorageMetaData) (int, map[string]int)

	HLen(key string) (retcode int, length uint64)

	HKeys(key string) (retcode int, values [][]byte)

	HVals(key string) (retcode int, values [][]byte)

	HIncrby(key, field string, version StorageVersionType, addValue int64, retValue *int64, meta StorageMetaData) int

	HExists(key, field string) int

	HGetAll(key string) (retcode int, values map[string]StorageByteValueData)

	/// set

	SAdd(key string, values []StorageValueData, meta StorageMetaData) (int, []int)

	SMembers(key string) (int, []StorageByteValueData)

	SPop(key string, meta StorageMetaData) (int, StorageByteValueData)

	SRem(key string, values []StorageValueData, meta StorageMetaData) (int, []int)

	SCard(key string) (int, uint64)

	SIsMember(key string, value string) int

	/// zset

	ZAdd(key string, scoreValues []StorageScoreValueData, meta StorageMetaData) (int, []int)

	ZRem(key string, values []StorageValueData, meta StorageMetaData) (int, []int)

	ZCount(key string, start, end string) (int, uint64)

	ZCard(key string) (int, uint64)

	ZScore(key string, value StorageValueData) (int, int64)

	ZRange(key string, start, end, withscore int) (int, []StorageByteValueData, []int64)

	ZRevrange(key string, start, end, withscore int) (int, []StorageByteValueData, []int64)

	ZRangeByScore(key string, min, max string, withscore int, reverse int) (int, []StorageByteValueData, []int64)

	ZRemRangeByScore(key string, min, max string, version StorageVersionType, meta StorageMetaData) (int, uint64)

	ZRank(key string, value StorageValueData) (int, uint64)

	ZRevRank(key string, value StorageValueData) (int, uint64)

	HScan(key string, cursor int, matchFmt string, count int) (int, []StorageByteValueData)

	SScan(key string, cursor int, matchFmt string, count int) (int, []StorageByteValueData)

	ZScan(key string, cursor int, matchFmt string, count int) (int, []StorageByteValueData)
}
