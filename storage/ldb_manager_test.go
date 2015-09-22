package ldb

import "testing"

var _engine *LdbManager
var _err error

func GetLdbManager() (*LdbManager, error) {
	if _engine == nil {
		_engine, _err = NewLdbManager()
	}
	return _engine, _err
}

func TestLdbFinalise(t *testing.T) {
	engine, err := GetLdbManager()
	if err != nil {
		t.Fatalf("GetLdbManager error\n")
	}
	engine.FinaliseDB()
}
