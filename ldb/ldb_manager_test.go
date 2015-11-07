package ldb

import (
	"fmt"
	"testing"
	_ "time"
)

var _engine *LdbManager
var _err error

func GetLdbManager() (*LdbManager, error) {
	if _engine == nil {
		_engine, _err = NewLdbManager()
		_engine.InitDB("/tmp/ldb", 64, 128)
	}
	return _engine, _err
}

//func TestLdbFinalise(t *testing.T) {
//	engine, err := GetLdbManager()
//	if err != nil {
//		t.Fatalf("GetLdbManager error\n")
//	}
//
//	engine.FinaliseDB()
//}

func TestString(t *testing.T) {
	engine, err := GetLdbManager()
	if err != nil {
		t.Fatalf("GetLdbManager error\n")
	}
	key := string("key1")
	value := StorageValueData{
		"value1",
		1 << 8,
	}
	meta := StorageMetaData{
		0,
		0,
		0,
	}

	fmt.Printf("set key %s value %s \n", key, value)

	ret := engine.Set(key, value, meta, IS_EXIST)
	if ret != STORAGE_OK {
		t.Errorf("Set key %s value %s error, ret %d", key, value, ret)
	}

	version := StorageVersionType(1 << 16)
	ret = engine.Expire(key, 5, version)
	if ret != STORAGE_OK {
		t.Errorf("Expire key %s error ,ret %v\n", ret)
	}

	var ttl uint32 = 0
	ret = engine.Ttl(key, &ttl)
	if ret != STORAGE_OK {
		t.Errorf("Ttl error, ret %v\n", ret)
	}
	fmt.Printf("Ttl key %s ttl %v\n", key, ttl)
}
