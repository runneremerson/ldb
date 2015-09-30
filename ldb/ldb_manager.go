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
	"sync"
	"time"
	"unsafe"

	log "github.com/golang/glog"
)

var KEY_LOCK_NUM int = 8

// StringPointer returns &s[0], which is not allowed in go
func StringPointer(s string) unsafe.Pointer {
	pstring := (*reflect.StringHeader)(unsafe.Pointer(&s))
	return unsafe.Pointer(pstring.Data)
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
	manager.ldbLock.Lock()
	defer manager.ldbLock.Unlock()

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
	manager.ldbLock.Lock()
	defer manager.ldbLock.Unlock()
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
	return 0
}
