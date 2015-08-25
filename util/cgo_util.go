package cgo_util

/*
#cgo linux CFLAGS: -std=gnu99 -W  -I../../ -DUSE_TCMALLOC=1 -DUSE_INT=1
#include "cgo_util_base.h"
#include "cgo_util_slice.h"

#include <sys/types.h>
#include <unistd.h>
#include <pthread.h>
*/
import "C"
import "syscall"
import "fmt"
import "unsafe"

func FreeGoByteSliceMembersInC(slice [][]byte) {
	C.freeByteSliceMembers((*C.GoByteSliceSlice)(unsafe.Pointer(&slice)))
}

func GetTid() (uint64, uint64) {
	tid1 := C.pthread_self()
	tid2 := syscall.Gettid()

	fmt.Printf("tid %v %v\n", uint64(tid1), uint64(tid2))

	return uint64(tid1), uint64(tid2)
}
