package ldb

/*
#cgo linux CFLAGS: -std=gnu99 -W -I/usr/local/include -I../ -DUSE_TCMALLOC=1 -DUSE_INT=1
#cgo  LDFLAGS:	-L/usr/local/lib  -lleveldb-ldb -ltcmalloc
#include "ldb_context.h"
#include "ldb_session.h"
*/
import "C"

import (
	log "github.com/golang/glog"
	"reflect"
	"sync"
	"unsafe"
)

// StringPointer returns &s[0], which is not allowed in go
func StringPointer(s string) unsafe.Pointer {
	pstring := (*reflect.StringHeader)(unsafe.Pointer(&s))
	return unsafe.Pointer(pstring.Data)
}

type LdbManager struct {
	inited   bool
	readCnt  uint64
	writeCnt uint64
	context  *C.ldb_context_t
	ldbLock  sync.RWMutex
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
