package ldb

/*
#cgo linux CFLAGS: -std=gnu99 -W -I/usr/local/include -I../ -DUSE_TCMALLOC=1 -DUSE_INT=1
#cgo  LDFLAGS:	-L/usr/local/lib  -lleveldb-ldb -ltcmalloc
#include "ldb_context.h"
#include "ldb_session.h"
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

func FreeValueItems(valueItems *C.value_item_t, size C.size_t) {
	if unsafe.Pointer(valueItems) != CNULL {
		C.destroy_value_item_array(valueItems, size)
	}
}

func GetRevisedExpireTime(version StorageVersionType, expiretime uint32, unit int) uint32 {
	// revise expiretime
	// ms
	oldTime := int64(version>>8) / 1000
	// ms
	nowTime := int64(time.Now().UnixNano()/1000) / 1000
	diff := nowTime - oldTime
	// covert to ms
	t := int64(expiretime)
	if unit == 0 {
		//0: ex, s
		//1: px, ms
		t *= 1000
	}

	if log.V(2) {
		log.Infof("unit %v time %v diff %v", unit, expiretime, diff)
	}

	if t-diff > 0 {
		if unit == 0 {
			// millonseconds
			expiretime = uint32(t - diff)
		} else {
			// millionseconds
			expiretime = uint32(t - diff)
		}
	} else {
		expiretime = 1
	}
	return expiretime
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

func (manager *LdbManager) Expire(key string, seconds uint32, version StorageVersionType) int {
	id := getLockID(key)
	manager.doLdbKeyLock(id)
	defer manager.doLdbKeyUnlock(id)

	csKey := C.CString(key)
	defer C.free(unsafe.Pointer(csKey))

	seconds = GetRevisedExpireTime(version, seconds, 0)

	log.Infof("Expire key %v seconds %v", key, seconds)

	cExpireTime := C.long(seconds)
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

	seconds = GetRevisedExpireTime(version, seconds, 1)

	cExpireTime := C.long(seconds)
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

	cRemain := C.long(0)

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

	cRemain := C.long(0)

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
		C.long(meta.Expiretime),
		valueItem,
		cEn)
	return int(ret)
}

func (manager *LdbManager) SetEx(key string, value StorageValueData, expiretime uint32) int {
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
	expiretime = GetRevisedExpireTime(value.Version, expiretime, 0)

	log.Infof("SetEx key %v val %v ex %v", key, value, expiretime)

	ret := C.ldb_set(manager.context,
		csKey,
		C.size_t(len(key)),
		C.uint64_t(0),
		C.int(0),
		C.long(expiretime),
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

		seconds = int(GetRevisedExpireTime(value.Version, uint32(seconds), unit))
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
		C.long(seconds),
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
