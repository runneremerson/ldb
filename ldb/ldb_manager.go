package ldb

/*
#cgo linux CFLAGS: -std=gnu99 -W -I/usr/local/include -I../ -DUSE_TCMALLOC=1 -DUSE_INT=1
#cgo  LDFLAGS:	-L/usr/local/lib  -lleveldb-ldb -ltcmalloc
#include "ldb_session.h"
#include "ldb_context.h"
#include "ldb_expiration.h"
*/
import "C"

import (
	"hash/crc32"
	"reflect"
	"strconv"
	"sync"
	"time"
	"unsafe"

	log "github.com/golang/glog"
)

var KEY_LOCK_NUM int = 8

const (
	VALUE_ITEM_SIZE = unsafe.Sizeof(C.value_item_t{})
	DOUBLE_SIZE     = unsafe.Sizeof(C.double(0.0))
	INT_SIZE        = unsafe.Sizeof(C.int(0))
	INT64_SIZE      = unsafe.Sizeof(C.int64_t(0))
)

// StringPointer returns &s[0], which is not allowed in go
func StringPointer(s string) unsafe.Pointer {
	pstring := (*reflect.StringHeader)(unsafe.Pointer(&s))
	return unsafe.Pointer(pstring.Data)
}

func ConvertCValueItemPointer2GoByte(valueItems *C.value_item_t, i int, value *StorageByteValueData) {
	var item *C.value_item_t
	item = (*C.value_item_t)(unsafe.Pointer(uintptr(unsafe.Pointer(valueItems)) + uintptr(i)*VALUE_ITEM_SIZE))
	if unsafe.Pointer(item.data_) != CNULL {
		value.Value = C.GoBytes(unsafe.Pointer(item.data_), C.int(item.data_len_))
		value.Version = StorageVersionType(item.version_)
	} else {
		// empty StorageValueData
		value.Value = nil
		value.Version = StorageVersionType(0)
	}
}

func ConvertCIntPointer2Go(intItems *C.int, i int, value *int) {
	if unsafe.Pointer(intItems) != CNULL {
		*value = int(*(*C.int)(unsafe.Pointer(uintptr(unsafe.Pointer(intItems)) + uintptr(i)*INT_SIZE)))
	}
}

func ConvertCInt64Pointer2Go(int64Items *C.int64_t, i int, value *int64) {
	if unsafe.Pointer(int64Items) != CNULL {
		*value = int64(*(*C.int64_t)(unsafe.Pointer(uintptr(unsafe.Pointer(int64Items)) + uintptr(i)*INT64_SIZE)))
	}
}

func ConvertCValueItemPointer2ByteSlice(valueItems *C.value_item_t, i int, buf *[]byte) {
	var item *C.value_item_t
	item = (*C.value_item_t)(unsafe.Pointer(uintptr(unsafe.Pointer(valueItems)) + uintptr(i)*VALUE_ITEM_SIZE))
	if unsafe.Pointer(item.data_) != CNULL {
		*buf = C.GoBytes(unsafe.Pointer(item.data_), C.int(item.data_len_))
	} else {
		*buf = nil
	}
}

func FreeValueItems(valueItems *C.value_item_t, size C.size_t) {
	if unsafe.Pointer(valueItems) != CNULL {
		C.destroy_value_item_array(valueItems, size)
	}
}

func GetRevisedExpireTime(ttl uint32, unit int) uint64 {
	// revise expiretime
	// ms
	nowTime := uint64(time.Now().UnixNano()/1000) / 1000
	// covert to ms
	t := uint64(ttl)
	if unit == 0 {
		//0: ex, s
		//1: px, ms
		t *= 1000
	}

	return uint64(nowTime + t)
}

type LdbManager struct {
	inited     bool
	readCnt    uint64
	writeCnt   uint64
	context    *C.ldb_context_t
	ldbLock    sync.RWMutex
	ldbKeyLock []sync.RWMutex
}

func (manager *LdbManager) doLdbLock() {
	manager.ldbLock.Lock()
}

func (manager *LdbManager) doLdbUnlock() {
	manager.ldbLock.Unlock()
}

func (manager *LdbManager) doLdbRLock() {
	manager.ldbLock.RLock()
}

func (manager *LdbManager) doLdbRUnlock() {
	manager.ldbLock.RUnlock()
}

func getLockID(key string) uint32 {
	v := crc32.ChecksumIEEE([]byte(key))
	return uint32(v % uint32(KEY_LOCK_NUM))
}

func getMLockID(keys []string) []uint32 {
	h := crc32.NewIEEE()
	keymap := make(map[uint32]uint32)
	for i := 0; i < len(keys); i += 1 {
		h.Write([]byte(keys[i]))
		v := h.Sum32()
		keymap[uint32(v%uint32(KEY_LOCK_NUM))] = 1
		h.Reset()
	}
	keyslice := make([]uint32, len(keymap))
	i := 0
	for f, _ := range keymap {
		keyslice[i] = f
		i += 1
	}
	return keyslice
}

func (manager *LdbManager) doLdbMKeyLock(ids []uint32) {
	manager.doLdbLock()
	for i := 0; i < len(ids); i += 1 {
		manager.ldbKeyLock[ids[i]].Lock()
	}
}

func (manager *LdbManager) doLdbMKeyUnlock(ids []uint32) {
	manager.doLdbUnlock()
	for i := 0; i < len(ids); i += 1 {
		manager.ldbKeyLock[ids[i]].Unlock()
	}
}

func (manager *LdbManager) doLdbKeyLock(id uint32) {
	manager.doLdbRLock()
	manager.ldbKeyLock[id].Lock()
}

func (manager *LdbManager) doLdbKeyUnlock(id uint32) {
	defer manager.doLdbRUnlock()
	manager.ldbKeyLock[id].Unlock()
}

func NewLdbManager() (*LdbManager, error) {
	ldbManager := &LdbManager{
		inited:   false,
		readCnt:  0,
		writeCnt: 0,
		context:  (*C.ldb_context_t)(CNULL),
	}
	return ldbManager, nil
}

func (manager *LdbManager) InitDB(file_path string, cache_size int, write_buffer_size int) int {
	manager.doLdbLock()
	defer manager.doLdbUnlock()

	log.Infof("LdbManager cache_size InitDB %v write_buffer_size %v\n", cache_size, write_buffer_size)
	if manager.inited {
		return 0
	}

	manager.context = (*C.ldb_context_t)(C.ldb_context_create(C.CString(file_path), C.size_t(cache_size), C.size_t(write_buffer_size)))
	if unsafe.Pointer(manager.context) == CNULL {
		log.Errorf("leveldb_context_create error")
		return -1
	}
	KEY_LOCK_NUM = 64
	manager.ldbKeyLock = make([]sync.RWMutex, KEY_LOCK_NUM)
	manager.inited = true

	go manager.CleanExpiredData()

	return 0
}

func (manager *LdbManager) FinaliseDB() {
	manager.doLdbLock()
	defer manager.doLdbUnlock()
	if manager.inited {
		C.ldb_context_destroy(manager.context)
		manager.inited = false
	}
}

func (manager *LdbManager) SetExceptionTrace(programName string) {
	cName := C.CString(programName)
	defer C.free(unsafe.Pointer(cName))

	C.set_ldb_signal_handler(cName)
}

func (manager *LdbManager) CleanExpiredData() {
	for {
		expiration := (*C.ldb_expiration_t)(CNULL)
		for {
			valueItems := (*C.value_item_t)(CNULL)
			cSize := C.size_t(0)
			ret := C.ldb_fetch_expire(manager.context, &expiration, &valueItems, &cSize)
			if int(ret) < 0 || int(cSize) == 0 {
				break
			}
			keys := make([]string, int(cSize))
			versions := make([]StorageVersionType, int(cSize))
			version := uint64((time.Now().UnixNano() / 1000) << 8)
			for i := 0; i < int(cSize); i++ {
				var value StorageByteValueData
				ConvertCValueItemPointer2GoByte(valueItems, i, &value)
				keys[i] = string(value.Value)
				versions[i] = StorageVersionType(version)
			}
			meta := DefaultMetaData()
			manager.Del(keys, versions, meta)

			time.Sleep(time.Duration(50) * time.Millisecond)
		}
	}
}

func (manager *LdbManager) Expire(key string, seconds uint32, version StorageVersionType) int {
	id := getLockID(key)
	manager.doLdbKeyLock(id)
	defer manager.doLdbKeyUnlock(id)

	csKey := C.CString(key)
	defer C.free(unsafe.Pointer(csKey))

	expiretime := GetRevisedExpireTime(seconds, 0)

	log.Infof("Expire key %v seconds %v", key, seconds)

	cExpireTime := C.uint64_t(expiretime)
	cVersion := C.uint64_t(version)

	ret := C.ldb_expire(manager.context, csKey, C.size_t(len(key)), cExpireTime, cVersion)

	iRet := int(ret)
	return iRet
}

func (manager *LdbManager) PExpire(key string, seconds uint32, version StorageVersionType) int {
	id := getLockID(key)
	manager.doLdbKeyLock(id)
	defer manager.doLdbKeyUnlock(id)

	csKey := C.CString(key)
	defer C.free(unsafe.Pointer(csKey))

	expiretime := GetRevisedExpireTime(seconds, 1)

	cExpireTime := C.uint64_t(expiretime)
	cVersion := C.uint64_t(version)

	ret := C.ldb_pexpire(manager.context, csKey, C.size_t(len(key)), cExpireTime, cVersion)

	iRet := int(ret)
	return iRet
}

func (manager *LdbManager) Persist(key string, version StorageVersionType) int {
	id := getLockID(key)
	manager.doLdbKeyLock(id)
	defer manager.doLdbKeyUnlock(id)

	csKey := C.CString(key)
	defer C.free(unsafe.Pointer(csKey))

	cVersion := C.uint64_t(version)

	ret := C.ldb_persist(manager.context, csKey, C.size_t(len(key)), cVersion)

	iRet := int(ret)
	return iRet
}

func (manager *LdbManager) Exists(key string) int {
	id := getLockID(key)
	manager.doLdbKeyLock(id)
	defer manager.doLdbKeyUnlock(id)

	csKey := C.CString(key)
	defer C.free(unsafe.Pointer(csKey))

	ret := C.ldb_exists(manager.context, csKey, C.size_t(len(key)))

	iRet := int(ret)
	return iRet
}

func (manager *LdbManager) Ttl(key string, remain *uint32) int {
	id := getLockID(key)
	manager.doLdbKeyLock(id)
	defer manager.doLdbKeyUnlock(id)

	csKey := C.CString(key)
	defer C.free(unsafe.Pointer(csKey))

	cRemain := C.uint64_t(0)

	ret := C.ldb_ttl(manager.context, csKey, C.size_t(len(key)), &cRemain)
	iRet := int(ret)
	if iRet == 0 {
		*remain = uint32(cRemain)
	}
	return iRet
}

func (manager *LdbManager) PTtl(key string, remain *uint32) int {
	id := getLockID(key)
	manager.doLdbKeyLock(id)
	defer manager.doLdbKeyUnlock(id)

	csKey := C.CString(key)
	defer C.free(unsafe.Pointer(csKey))

	cRemain := C.uint64_t(0)

	ret := C.ldb_pttl(manager.context, csKey, C.size_t(len(key)), &cRemain)
	iRet := int(ret)
	if iRet == 0 {
		*remain = uint32(cRemain)
	}
	return iRet
}

func (manager *LdbManager) Set(key string, value StorageValueData, meta StorageMetaData, en SetOpt) int {
	id := getLockID(key)
	manager.doLdbKeyLock(id)
	defer manager.doLdbKeyUnlock(id)

	csKey := (*C.char)(StringPointer(key))
	csVal := (*C.char)(StringPointer(value.Value))

	if value.Version == 0 {
		log.Errorf("Set value %s version %u is 0! return -1", value.Value, value.Version)
		return -1
	}

	valueItem := new(C.value_item_t)
	valueItem.data_len_ = C.size_t(len(value.Value))
	valueItem.data_ = csVal
	valueItem.version_ = C.uint64_t(value.Version)
	cEn := C.int(en)

	ret := C.ldb_set(manager.context,
		csKey,
		C.size_t(len(key)),
		C.uint64_t(meta.Lastversion),
		C.int(meta.Versioncare),
		C.uint64_t(meta.Expiretime),
		valueItem,
		cEn)
	return int(ret)
}

func (manager *LdbManager) SetEx(key string, value StorageValueData, ttl uint32) int {
	id := getLockID(key)
	manager.doLdbKeyLock(id)
	defer manager.doLdbKeyUnlock(id)

	csKey := (*C.char)(StringPointer(key))         //C.CString(key)
	csVal := (*C.char)(StringPointer(value.Value)) //C.CString(value.Value)

	if value.Version == 0 {
		log.Errorf("Set value %s version %u is zero! return -1", value.Value, value.Version)
		return -1
	}

	valueItem := new(C.value_item_t)
	valueItem.data_len_ = C.size_t(len(value.Value))
	valueItem.data_ = csVal
	valueItem.version_ = C.uint64_t(value.Version)
	cEn := C.int(IS_EXIST_AND_EXPIRE)

	//revise expiretime
	expiretime := GetRevisedExpireTime(ttl, 0)

	log.Infof("SetEx key %v val %v ex %v", key, value, expiretime)

	ret := C.ldb_set(manager.context,
		csKey,
		C.size_t(len(key)),
		C.uint64_t(0),
		C.int(0),
		C.uint64_t(expiretime),
		valueItem,
		cEn)
	return int(ret)
}

func (manager *LdbManager) SetWithSecond(key string, value StorageValueData, meta StorageMetaData, args []string) int {
	id := getLockID(key)
	manager.doLdbKeyLock(id)
	defer manager.doLdbKeyUnlock(id)

	var en SetOpt
	en = IS_EXIST_AND_EXPIRE
	var expiretime uint64
	seconds := 0
	unit := 0 // 0:seconds, 1:milliseconds
	var err error

	if args != nil && len(args) > 0 {
		argSize := len(args)
		if args[argSize-1] == "NX" {
			en = IS_NOT_EXIST_AND_EXPIRE
		} else if args[argSize-1] == "XX" {
			// do nothing
		}

		if argSize > 1 {
			if args[0] == "PX" {
				unit = 1
			} else if args[0] != "EX" {
				return STORAGE_ERR_WRONG_NUMBER_ARGUMENTS
			}

			seconds, err = strconv.Atoi(args[1])
			if err != nil {
				return STORAGE_ERR_WRONG_NUMBER_ARGUMENTS
			}
		}

		expiretime = uint64(GetRevisedExpireTime(uint32(seconds), unit))
	}
	csKey := (*C.char)(StringPointer(key))
	csVal := (*C.char)(StringPointer(value.Value))

	if value.Version == 0 {
		log.Errorf("Set value %s version %u is 0! return -1", value.Value, value.Version)
		return -1
	}

	valueItem := new(C.value_item_t)
	valueItem.data_len_ = C.size_t(len(value.Value))
	valueItem.data_ = csVal
	valueItem.version_ = C.uint64_t(value.Version)
	cEn := C.int(en)

	ret := C.ldb_set(manager.context,
		csKey,
		C.size_t(len(key)),
		C.uint64_t(meta.Lastversion),
		C.int(unit),
		C.uint64_t(expiretime),
		valueItem,
		cEn)
	return int(ret)
}

func (manager *LdbManager) Get(key string) (int, StorageByteValueData) {
	id := getLockID(key)
	manager.doLdbKeyLock(id)
	defer manager.doLdbKeyUnlock(id)

	csKey := (*C.char)(StringPointer(key))
	var valueItem *C.value_item_t
	valueItem = (*C.value_item_t)(CNULL)

	ret := C.ldb_get(manager.context,
		csKey,
		C.size_t(len(key)),
		&valueItem)
	iRet := int(ret)
	value := StorageByteValueData{}
	if iRet == 0 {
		ConvertCValueItemPointer2GoByte(valueItem, 0, &value)
	}
	FreeValueItems(valueItem, 1)
	return iRet, value
}

func (manager *LdbManager) GetWithByte(key []byte) (int, StorageByteValueData) {
	id := getLockID(string(key))
	manager.doLdbKeyLock(id)
	defer manager.doLdbKeyUnlock(id)

	csKey := (*C.char)(unsafe.Pointer(&key[0]))
	var valueItem *C.value_item_t
	valueItem = (*C.value_item_t)(CNULL)

	ret := C.ldb_get(manager.context,
		csKey,
		C.size_t(len(key)),
		&valueItem)
	iRet := int(ret)
	value := StorageByteValueData{}
	if iRet == 0 {
		ConvertCValueItemPointer2GoByte(valueItem, 0, &value)
	}
	FreeValueItems(valueItem, 1)
	return iRet, value
}

func (manager *LdbManager) Del(keys []string, versions []StorageVersionType, meta StorageMetaData) (int, []int) {
	manager.doLdbLock()
	defer manager.doLdbUnlock()

	if len(keys) != len(versions) {
		log.Errorf("Del count dont match keys %d versions %d", len(keys), len(versions))
		return -1, nil
	}

	rets := make([]int, len(keys))
	cnt := 0
	for i := 0; i < len(keys); i += 1 {
		csKey := C.CString(keys[i])
		defer C.free(unsafe.Pointer(csKey))

		ret := C.ldb_del(manager.context, csKey, C.size_t(len(keys[i])), C.int(meta.Versioncare), C.uint64_t(versions[i]))
		rets[i] = int(ret)
	}
	return cnt, rets
}

func (manager *LdbManager) Incr(key string, meta StorageMetaData, version StorageVersionType, value *int64) int {
	return manager.IncrDecr(key, meta, version, 1, value)
}

func (manager *LdbManager) Incrby(key string, meta StorageMetaData, version StorageVersionType, by int64, value *int64) int {
	return manager.IncrDecr(key, meta, version, by, value)
}

func (manager *LdbManager) Decr(key string, meta StorageMetaData, version StorageVersionType, value *int64) int {
	return manager.IncrDecr(key, meta, version, -1, value)
}

func (manager *LdbManager) Decrby(key string, meta StorageMetaData, version StorageVersionType, by int64, value *int64) int {
	return manager.IncrDecr(key, meta, version, -1*by, value)
}

func (manager *LdbManager) IncrDecr(key string, meta StorageMetaData, version StorageVersionType, by int64, value *int64) int {
	id := getLockID(string(key))
	manager.doLdbKeyLock(id)
	defer manager.doLdbKeyUnlock(id)

	csKey := C.CString(key)

	defer C.free(unsafe.Pointer(csKey))

	cInit := C.int64_t(0)
	cBy := C.int64_t(by)
	cValue := C.int64_t(0)

	ret := C.ldb_incrby(manager.context,
		csKey,
		C.size_t(len(key)),
		C.uint64_t(meta.Lastversion),
		C.int(meta.Versioncare),
		C.uint64_t(meta.Expiretime),
		C.uint64_t(version),
		cInit,
		cBy,
		&cValue)

	*value = int64(cValue)
	return int(ret)
}

func (manager *LdbManager) MSet(keyVals [][]byte, versions []StorageVersionType, meta StorageMetaData, en SetOpt, results []uint64) int {
	manager.doLdbLock()
	defer manager.doLdbUnlock()

	cSize := C.size_t(len(keyVals))
	cEn := C.int(en)

	ret := C.ldb_mset(manager.context,
		C.uint64_t(meta.Lastversion),
		C.int(meta.Versioncare),
		C.uint64_t(meta.Expiretime),
		(*C.GoByteSlice)(unsafe.Pointer(&keyVals[0])),
		(*C.GoUint64Slice)(unsafe.Pointer(&versions)),
		cSize,
		(*C.GoUint64Slice)(unsafe.Pointer(&results)),
		cEn)

	return int(ret)
}

func (manager *LdbManager) MGet(keys [][]byte, results *[][]byte, versions []uint64) int {
	manager.doLdbLock()
	defer manager.doLdbUnlock()

	if len(keys) == 0 {
		return STORAGE_ERR
	}

	cSize := C.size_t(0)

	ret := C.ldb_mget(manager.context,
		(*C.GoByteSlice)(unsafe.Pointer(&keys[0])),
		C.size_t(len(keys)),
		(*C.GoByteSliceSlice)(unsafe.Pointer(results)),
		(*C.GoUint64Slice)(unsafe.Pointer(&versions)),
		&cSize)
	iRet := int(ret)

	if iRet == 0 {
		if log.V(2) {
			log.V(2).Infof("versions %v len %v", versions, len(versions))
			log.V(2).Infof("results %v len %v", *results, len(*results))
			for i, _ := range *results {
				log.V(2).Infof("result[%d] %v", i, (*results)[i])
			}
		}
	}

	return iRet
}

func (manager *LdbManager) HSet(key, field string, value StorageValueData, meta StorageMetaData) int {
	id := getLockID(string(key))
	manager.doLdbKeyLock(id)
	defer manager.doLdbKeyUnlock(id)

	csKey := C.CString(key)
	csField := C.CString(field)
	csValue := C.CString(value.Value)

	defer C.free(unsafe.Pointer(csKey))
	defer C.free(unsafe.Pointer(csField))
	defer C.free(unsafe.Pointer(csValue))

	valueItem := new(C.value_item_t)
	valueItem.data_len_ = C.size_t(len(value.Value))
	valueItem.data_ = csValue
	valueItem.version_ = C.uint64_t(value.Version)

	ret := C.ldb_hset(manager.context,
		csKey,
		C.size_t(len(key)),
		C.uint64_t(meta.Lastversion),
		C.int(meta.Versioncare),
		csField,
		C.size_t(len(field)),
		valueItem)

	return int(ret)
}

func (manager *LdbManager) HGet(key, field []byte) (int, StorageByteValueData) {
	id := getLockID(string(key))
	manager.doLdbKeyLock(id)
	defer manager.doLdbKeyUnlock(id)

	var valueItem *C.value_item_t
	valueItem = (*C.value_item_t)(CNULL)

	ret := C.ldb_hget(manager.context,
		(*C.GoByteSlice)(unsafe.Pointer(&key)),
		(*C.GoByteSlice)(unsafe.Pointer(&field)),
		&valueItem)
	iRet := int(ret)

	value := StorageByteValueData{}

	if iRet == 0 {
		ConvertCValueItemPointer2GoByte(valueItem, 0, &value)
	}

	FreeValueItems(valueItem, 1)

	return iRet, value
}

func (manager *LdbManager) HExists(key, field string) int {
	id := getLockID(string(key))
	manager.doLdbKeyLock(id)
	defer manager.doLdbKeyUnlock(id)

	csKey := C.CString(key)
	csField := C.CString(field)

	defer C.free(unsafe.Pointer(csKey))
	defer C.free(unsafe.Pointer(csField))

	ret := C.ldb_hexists(manager.context,
		csKey,
		C.size_t(len(key)),
		csField,
		C.size_t(len(field)))

	return int(ret)
}

func (manager *LdbManager) HIncrby(key, field string, version StorageVersionType, by int64, result *int64, meta StorageMetaData) int {
	id := getLockID(string(key))
	manager.doLdbKeyLock(id)
	defer manager.doLdbKeyUnlock(id)

	csKey := C.CString(key)
	csField := C.CString(field)

	defer C.free(unsafe.Pointer(csKey))
	defer C.free(unsafe.Pointer(csField))

	fieldItem := new(C.value_item_t)
	fieldItem.data_len_ = C.size_t(len(field))
	fieldItem.data_ = csField
	fieldItem.version_ = C.uint64_t(version)
	cResult := C.int64_t(0)

	// set key-field-value
	ret := C.ldb_hincrby(manager.context,
		csKey,
		C.size_t(len(key)),
		C.uint64_t(meta.Lastversion),
		C.int(meta.Versioncare),
		fieldItem,
		C.int64_t(by), &cResult)

	*result = int64(cResult)

	return int(ret)
}

func (manager *LdbManager) HmSet(key string, values map[string]StorageValueData, meta StorageMetaData) (int, map[string]int) {
	id := getLockID(string(key))
	manager.doLdbKeyLock(id)
	defer manager.doLdbKeyUnlock(id)

	csKey := C.CString(key)
	defer C.free(unsafe.Pointer(csKey))

	valueItems := make([]C.value_item_t, len(values)*2)
	index := 0
	for f, v := range values {
		// field
		csField := C.CString(f)
		defer C.free(unsafe.Pointer(csField))
		valueItems[index].data_len_ = C.size_t(len(f))
		valueItems[index].data_ = csField
		valueItems[index].version_ = C.uint64_t(v.Version)

		index += 1

		// value
		csValue := C.CString(v.Value)
		defer C.free(unsafe.Pointer(csValue))
		valueItems[index].data_len_ = C.size_t(len(v.Value))
		valueItems[index].data_ = csValue
		valueItems[index].version_ = C.uint64_t(v.Version)

		if v.Version == 0 {
			log.Errorf("HMSet value %s version %u is zero! return -1", v.Value, v.Version)
			return -1, nil
		}

		index += 1
	}

	var results *C.int
	results = (*C.int)(CNULL)

	// set key-fields-values
	ret := C.ldb_hmset(manager.context,
		csKey,
		C.size_t(len(key)),
		C.uint64_t(meta.Lastversion),
		C.int(meta.Versioncare),
		(*C.value_item_t)(unsafe.Pointer(&valueItems[0])),
		C.size_t(index),
		&results)

	resultMap := map[string]int{}
	if int(ret) == 0 {
		k := 0
		for i := 0; i < len(valueItems); i += 2 {
			key := C.GoStringN(valueItems[i].data_, C.int(valueItems[i].data_len_))
			tmpInt := int(0)
			ConvertCIntPointer2Go(results, k, &tmpInt)
			resultMap[key] = tmpInt
			k += 1
		}
	}

	if unsafe.Pointer(results) != CNULL {
		C.free(unsafe.Pointer(results))
	}

	return int(ret), resultMap
}

func (manager *LdbManager) HmGet(key string, fields []string) (int, []StorageByteValueData) {
	id := getLockID(string(key))
	manager.doLdbKeyLock(id)
	defer manager.doLdbKeyUnlock(id)

	csKey := C.CString(key)
	defer C.free(unsafe.Pointer(csKey))

	cFields := make([]C.value_item_t, len(fields))

	var valueItems *C.value_item_t
	valueItems = (*C.value_item_t)(CNULL)

	index := 0
	for _, f := range fields {
		// field
		csField := C.CString(f)
		cFields[index].data_len_ = C.size_t(len(f))
		cFields[index].data_ = csField
		index += 1
	}

	for _, f := range cFields {
		defer C.free(unsafe.Pointer(f.data_))
	}

	cSize := C.size_t(0)

	// get key-fields-values
	ret := C.ldb_hmget(manager.context,
		csKey,
		C.size_t(len(key)),
		(*C.value_item_t)(unsafe.Pointer(&cFields[0])),
		C.size_t(len(cFields)),
		&valueItems,
		&cSize)
	iRet := int(ret)

	var values []StorageByteValueData

	if iRet == 0 {
		values = make([]StorageByteValueData, int(cSize))
		for i := 0; i < int(cSize); i += 1 {
			ConvertCValueItemPointer2GoByte(valueItems, i, &values[i])
		}
	}

	FreeValueItems(valueItems, cSize)

	return iRet, values
}

func (manager *LdbManager) HDel(key string, fields map[string]StorageValueData, meta StorageMetaData) (int, map[string]int) {
	id := getLockID(string(key))
	manager.doLdbKeyLock(id)
	defer manager.doLdbKeyUnlock(id)

	csKey := C.CString(key)

	defer C.free(unsafe.Pointer(csKey))

	valueItems := make([]C.value_item_t, len(fields)*2)
	index := 0
	for f, v := range fields {
		// field
		csField := C.CString(f)
		defer C.free(unsafe.Pointer(csField))
		valueItems[index].data_len_ = C.size_t(len(f))
		valueItems[index].data_ = csField
		valueItems[index].version_ = C.uint64_t(v.Version)

		index += 1

		// value
		csValue := C.CString(v.Value)
		defer C.free(unsafe.Pointer(csValue))
		valueItems[index].data_len_ = C.size_t(len(v.Value))
		valueItems[index].data_ = csValue
		valueItems[index].version_ = C.uint64_t(v.Version)

		index += 1
	}

	var results *C.int
	results = (*C.int)(CNULL)

	// del fields
	ret := C.ldb_hdel(manager.context,
		csKey,
		C.size_t(len(key)),
		C.uint64_t(meta.Lastversion),
		C.int(meta.Versioncare),
		(*C.value_item_t)(unsafe.Pointer(&valueItems[0])),
		C.size_t(index),
		&results)

	resultMap := map[string]int{}
	if int(ret) == 0 {
		j := 0
		for i := 0; i < len(fields); i += 1 {
			key := C.GoStringN(valueItems[j].data_, C.int(valueItems[j].data_len_))
			tmpInt := int(0)
			ConvertCIntPointer2Go(results, i, &tmpInt)
			resultMap[key] = tmpInt
			j += 2
		}
	}

	if unsafe.Pointer(results) != CNULL {
		C.free(unsafe.Pointer(results))
	}

	return int(ret), resultMap
}

func (manager *LdbManager) HLen(key string) (ret int, length uint64) {
	id := getLockID(key)
	manager.doLdbKeyLock(id)
	defer manager.doLdbKeyUnlock(id)

	csKey := C.CString(key)

	defer C.free(unsafe.Pointer(csKey))

	cLen := C.uint64_t(0)

	retval := C.ldb_hlen(manager.context, csKey, C.size_t(len(key)), &cLen)

	return int(retval), uint64(cLen)
}

func (manager *LdbManager) HKeys(key string) (ret int, values [][]byte) {
	id := getLockID(key)
	manager.doLdbKeyLock(id)
	defer manager.doLdbKeyUnlock(id)

	csKey := C.CString(key)

	defer C.free(unsafe.Pointer(csKey))

	var valueItems *C.value_item_t
	valueItems = (*C.value_item_t)(CNULL)

	cSize := C.size_t(0)

	// get all members in the key
	retval := C.ldb_hkeys(manager.context, csKey, C.size_t(len(key)), &valueItems, &cSize)

	iRet := int(retval)

	keys := make([][]byte, int(cSize))

	if iRet == 0 {
		for i := 0; i < int(cSize); i += 1 {
			ConvertCValueItemPointer2ByteSlice(valueItems, i, &keys[i])
		}
	}

	FreeValueItems(valueItems, cSize)

	return iRet, keys
}

func (manager *LdbManager) HVals(key string) (ret int, values [][]byte) {
	id := getLockID(key)
	manager.doLdbKeyLock(id)
	defer manager.doLdbKeyUnlock(id)

	csKey := C.CString(key)

	defer C.free(unsafe.Pointer(csKey))

	var valueItems *C.value_item_t
	valueItems = (*C.value_item_t)(CNULL)

	cSize := C.size_t(0)

	// get all values
	retval := C.ldb_hvals(manager.context, csKey, C.size_t(len(key)), &valueItems, &cSize)

	iRet := int(retval)

	vals := make([][]byte, int(cSize))

	if iRet == 0 {
		for i := 0; i < int(cSize); i += 1 {
			ConvertCValueItemPointer2ByteSlice(valueItems, i, &vals[i])
		}
	}

	FreeValueItems(valueItems, cSize)

	return iRet, vals
}

func (manager *LdbManager) HGetAll(key string) (ret int, values map[string]StorageByteValueData) {
	id := getLockID(key)
	manager.doLdbKeyLock(id)
	defer manager.doLdbKeyUnlock(id)

	csKey := C.CString(key)
	defer C.free(unsafe.Pointer(csKey))

	var valueItems *C.value_item_t
	valueItems = (*C.value_item_t)(CNULL)

	cSize := C.size_t(0)

	// get all members in the key
	retval := C.ldb_hgetall(manager.context, csKey, C.size_t(len(key)), &valueItems, &cSize)

	iRet := int(retval)

	members := map[string]StorageByteValueData{}

	if iRet == 0 {
		fields := make([]StorageByteValueData, int(cSize/2))
		values := make([]StorageByteValueData, int(cSize/2))
		index := 0
		for i := 0; i < int(cSize); i += 2 {
			ConvertCValueItemPointer2GoByte(valueItems, i, &fields[index])
			ConvertCValueItemPointer2GoByte(valueItems, i+1, &values[index])
			members[string(fields[index].Value)] = values[index]
			index += 1
		}
	}

	FreeValueItems(valueItems, cSize)

	return iRet, members
}

func (manager *LdbManager) ZAdd(key string, scoreValues []StorageScoreValueData, meta StorageMetaData) (int, []int) {
	id := getLockID(key)
	manager.doLdbKeyLock(id)
	defer manager.doLdbKeyUnlock(id)

	csKey := C.CString(key)

	defer C.free(unsafe.Pointer(csKey))

	valueItems := make([]C.value_item_t, len(scoreValues))
	scoreItems := make([]C.int64_t, len(scoreValues))
	index := 0
	for _, v := range scoreValues {
		// value
		csValue := C.CString(v.Value)
		defer C.free(unsafe.Pointer(csValue))
		valueItems[index].data_len_ = C.size_t(len(v.Value))
		valueItems[index].data_ = csValue
		valueItems[index].version_ = C.uint64_t(v.Version)

		if v.Version == 0 {
			log.Errorf("ZAdd value %s version %u is zero! return -1", v.Value, v.Version)
			return -1, nil
		}

		// score
		scoreItems[index] = C.int64_t(v.Score)

		index += 1
	}

	var results *C.int
	results = (*C.int)(CNULL)

	ret := C.ldb_zadd(manager.context,
		csKey,
		C.size_t(len(key)),
		C.uint64_t(meta.Lastversion),
		C.int(meta.Versioncare),
		(*C.value_item_t)(unsafe.Pointer(&valueItems[0])),
		(*C.int64_t)(unsafe.Pointer(&scoreItems[0])),
		C.size_t(index),
		&results)

	retArr := make([]int, len(scoreValues))
	if int(ret) == 0 {
		for i := 0; i < len(scoreValues); i += 1 {
			tmpInt := int(0)
			ConvertCIntPointer2Go(results, i, &tmpInt)
			retArr[i] = tmpInt
		}
	}

	if unsafe.Pointer(results) != CNULL {
		C.free(unsafe.Pointer(results))
	}

	return int(ret), retArr
}

func (manager *LdbManager) ZRem(key string, values []StorageValueData, meta StorageMetaData) (int, []int) {
	id := getLockID(key)
	manager.doLdbKeyLock(id)
	defer manager.doLdbKeyUnlock(id)

	csKey := C.CString(key)

	defer C.free(unsafe.Pointer(csKey))

	valueItems := make([]C.value_item_t, len(values))
	index := 0
	for _, v := range values {
		// value
		csValue := C.CString(v.Value)
		defer C.free(unsafe.Pointer(csValue))
		valueItems[index].data_len_ = C.size_t(len(v.Value))
		valueItems[index].data_ = csValue
		valueItems[index].version_ = C.uint64_t(v.Version)

		index += 1
	}

	var results *C.int
	results = (*C.int)(CNULL)

	ret := C.ldb_zrem(manager.context,
		csKey,
		C.size_t(len(key)),
		C.uint64_t(meta.Lastversion),
		C.int(meta.Versioncare),
		(*C.value_item_t)(unsafe.Pointer(&valueItems[0])),
		C.size_t(index),
		&results)

	retArr := make([]int, len(values))
	if int(ret) == 0 {
		for i := 0; i < len(values); i += 1 {
			tmpInt := int(0)
			ConvertCIntPointer2Go(results, i, &tmpInt)
			retArr[i] = tmpInt
		}
	}

	if unsafe.Pointer(results) != CNULL {
		C.free(unsafe.Pointer(results))
	}

	return int(ret), retArr
}

func (manager *LdbManager) ZCount(key string, start, end string) (int, uint64) {
	id := getLockID(key)
	manager.doLdbKeyLock(id)
	defer manager.doLdbKeyUnlock(id)

	csKey := C.CString(key)

	defer C.free(unsafe.Pointer(csKey))

	cCount := C.uint64_t(0)
	var scoreStart int64
	var scoreEnd int64
	var err error
	scoreStart, err = strconv.ParseInt(start, 10, 64)
	if err != nil {
		return STORAGE_ERR, 0
	}
	scoreEnd, err = strconv.ParseInt(end, 10, 64)
	if err != nil {
		return STORAGE_ERR, 0
	}
	cScoreStart := C.int64_t(scoreStart)
	cScoreEnd := C.int64_t(scoreEnd)

	ret := C.ldb_zcount(manager.context, csKey, C.size_t(len(key)), cScoreStart, cScoreEnd, &cCount)

	return int(ret), uint64(cCount)
}

func (manager *LdbManager) ZCard(key string) (int, uint64) {
	id := getLockID(key)
	manager.doLdbKeyLock(id)
	defer manager.doLdbKeyUnlock(id)

	csKey := C.CString(key)

	defer C.free(unsafe.Pointer(csKey))

	cSize := C.uint64_t(0)

	ret := C.ldb_zcard(manager.context, csKey, C.size_t(len(key)), &cSize)

	return int(ret), uint64(cSize)
}

func (manager *LdbManager) ZScore(key string, value StorageValueData) (int, int64) {
	id := getLockID(key)
	manager.doLdbKeyLock(id)
	defer manager.doLdbKeyUnlock(id)

	csKey := C.CString(key)
	csValue := C.CString(value.Value)

	defer C.free(unsafe.Pointer(csKey))
	defer C.free(unsafe.Pointer(csValue))

	cScore := C.int64_t(0)

	ret := C.ldb_zscore(manager.context, csKey, C.size_t(len(key)), csValue, C.size_t(len(value.Value)), &cScore)

	return int(ret), int64(cScore)
}

func (manager *LdbManager) ZRange(key string, start, end, withscore int) (int, []StorageByteValueData, []int64) {
	id := getLockID(key)
	manager.doLdbKeyLock(id)
	defer manager.doLdbKeyUnlock(id)

	csKey := C.CString(key)
	defer C.free(unsafe.Pointer(csKey))

	var valueItems *C.value_item_t
	valueItems = (*C.value_item_t)(CNULL)
	var scoreItems *C.int64_t
	scoreItems = (*C.int64_t)(CNULL)

	cStart := C.int(start)
	cEnd := C.int(end)
	cWithscore := C.int(withscore)
	cScoreSize := C.size_t(0)
	cSize := C.size_t(0)
	cReverse := C.int(0)

	// get range
	ret := C.ldb_zrange_by_rank(manager.context,
		csKey,
		C.size_t(len(key)),
		cStart,
		cEnd,
		&valueItems,
		&cSize,
		&scoreItems,
		&cScoreSize,
		cReverse,
		cWithscore)
	iRet := int(ret)

	var values []StorageByteValueData
	var scores []int64

	if iRet == 0 {
		values = make([]StorageByteValueData, int(cSize))
		scores = make([]int64, int(cScoreSize))
		for i := 0; i < int(cSize); i += 1 {
			ConvertCValueItemPointer2GoByte(valueItems, i, &values[i])
			if withscore == 1 {
				ConvertCInt64Pointer2Go(scoreItems, i, &scores[i])
			}
		}
	}

	FreeValueItems(valueItems, cSize)

	if unsafe.Pointer(scoreItems) != CNULL {
		C.free(unsafe.Pointer(scoreItems))
	}

	return iRet, values, scores
}

func (manager *LdbManager) ZRevrange(key string, start, end, withscore int) (int, []StorageByteValueData, []int64) {
	id := getLockID(key)
	manager.doLdbKeyLock(id)
	defer manager.doLdbKeyUnlock(id)

	csKey := C.CString(key)

	defer C.free(unsafe.Pointer(csKey))

	var valueItems *C.value_item_t
	valueItems = (*C.value_item_t)(CNULL)
	var scoreItems *C.int64_t
	scoreItems = (*C.int64_t)(CNULL)

	cStart := C.int(start)
	cEnd := C.int(end)
	cWithscore := C.int(withscore)
	cScoreSize := C.size_t(0)
	cSize := C.size_t(0)
	cReverse := C.int(1)

	// get range
	ret := C.ldb_zrange_by_rank(manager.context,
		csKey,
		C.size_t(len(key)),
		cStart,
		cEnd,
		&valueItems,
		&cSize,
		&scoreItems,
		&cScoreSize,
		cReverse,
		cWithscore)
	iRet := int(ret)

	var values []StorageByteValueData
	var scores []int64

	if iRet == 0 {
		values = make([]StorageByteValueData, int(cSize))
		scores = make([]int64, int(cScoreSize))
		for i := 0; i < int(cSize); i += 1 {
			ConvertCValueItemPointer2GoByte(valueItems, i, &values[i])
			if withscore == 1 {
				ConvertCInt64Pointer2Go(scoreItems, i, &scores[i])
			}
		}
	}

	FreeValueItems(valueItems, cSize)

	if unsafe.Pointer(scoreItems) != CNULL {
		C.free(unsafe.Pointer(scoreItems))
	}

	return iRet, values, scores
}

func (manager *LdbManager) ZRangeByScore(key string, min, max string, withscore int, reverse int) (int, []StorageByteValueData, []int64) {
	id := getLockID(key)
	manager.doLdbKeyLock(id)
	defer manager.doLdbKeyUnlock(id)

	csKey := C.CString(key)

	defer C.free(unsafe.Pointer(csKey))

	var valueItems *C.value_item_t
	valueItems = (*C.value_item_t)(CNULL)
	var scoreItems *C.int64_t
	scoreItems = (*C.int64_t)(CNULL)

	var scoreStart int64
	var scoreEnd int64
	var err error
	scoreStart, err = strconv.ParseInt(min, 10, 64)
	if err != nil {
		return STORAGE_ERR, nil, nil
	}
	scoreEnd, err = strconv.ParseInt(max, 10, 64)
	if err != nil {
		return STORAGE_ERR, nil, nil
	}
	cScoreStart := C.int64_t(scoreStart)
	cScoreEnd := C.int64_t(scoreEnd)

	cWithScore := C.int(withscore)
	cReverse := C.int(reverse)
	cScoreSize := C.size_t(0)
	cSize := C.size_t(0)

	// get range by score
	ret := C.ldb_zrange_by_score(manager.context,
		csKey,
		C.size_t(len(key)),
		cScoreStart,
		cScoreEnd,
		&valueItems,
		&cSize,
		&scoreItems,
		&cScoreSize,
		cReverse,
		cWithScore)

	iRet := int(ret)

	var values []StorageByteValueData
	var scores []int64

	if iRet == 0 {
		values = make([]StorageByteValueData, int(cSize))
		scores = make([]int64, int(cScoreSize))
		for i := 0; i < int(cSize); i += 1 {
			ConvertCValueItemPointer2GoByte(valueItems, i, &values[i])
			if withscore == 1 {
				ConvertCInt64Pointer2Go(scoreItems, i, &scores[i])
			}
		}
	}

	FreeValueItems(valueItems, cSize)

	if unsafe.Pointer(scoreItems) != CNULL {
		C.free(unsafe.Pointer(scoreItems))
	}

	return iRet, values, scores
}

func (manager *LdbManager) ZRemRangeByScore(key string, min, max string, version StorageVersionType, meta StorageMetaData) (int, uint64) {
	id := getLockID(key)
	manager.doLdbKeyLock(id)
	defer manager.doLdbKeyUnlock(id)

	csKey := C.CString(key)

	defer C.free(unsafe.Pointer(csKey))

	var scoreStart int64
	var scoreEnd int64
	var err error
	scoreStart, err = strconv.ParseInt(min, 10, 64)
	if err != nil {
		return STORAGE_ERR, 0
	}
	scoreEnd, err = strconv.ParseInt(max, 10, 64)
	if err != nil {
		return STORAGE_ERR, 0
	}

	cScoreStart := C.int64_t(scoreStart)
	cScoreEnd := C.int64_t(scoreEnd)

	deleted := C.uint64_t(0)

	ret := C.ldb_zrem_by_score(manager.context,
		csKey,
		C.size_t(len(key)),
		C.uint64_t(version),
		C.int(meta.Versioncare),
		cScoreStart,
		cScoreEnd,
		&deleted)
	iRet := int(ret)

	return iRet, uint64(deleted)
}

func (manager *LdbManager) ZRank(key string, value StorageValueData) (int, uint64) {
	id := getLockID(key)
	manager.doLdbKeyLock(id)
	defer manager.doLdbKeyUnlock(id)

	csKey := C.CString(key)
	csValue := C.CString(value.Value)

	defer C.free(unsafe.Pointer(csKey))
	defer C.free(unsafe.Pointer(csValue))

	cRank := C.uint64_t(0)
	cReverse := C.int(0)

	ret := C.ldb_zrank(manager.context, csKey, C.size_t(len(key)), csValue, C.size_t(len(value.Value)), cReverse, &cRank)

	return int(ret), uint64(cRank)
}

func (manager *LdbManager) ZRevRank(key string, value StorageValueData) (int, uint64) {
	id := getLockID(key)
	manager.doLdbKeyLock(id)
	defer manager.doLdbKeyUnlock(id)

	csKey := C.CString(key)
	csValue := C.CString(value.Value)

	defer C.free(unsafe.Pointer(csKey))
	defer C.free(unsafe.Pointer(csValue))

	cRank := C.uint64_t(0)
	cReverse := C.int(1)

	ret := C.ldb_zrank(manager.context, csKey, C.size_t(len(key)), csValue, C.size_t(len(value.Value)), cReverse, &cRank)

	return int(ret), uint64(cRank)
}
