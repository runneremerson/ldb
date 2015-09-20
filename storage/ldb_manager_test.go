package ldb

import "testing"

func TestLdbOK(t *testing.T) {
	engine, err := NewLdbManager()
	if err != nil {
		t.Fatalf("NewLdbManager error\n")
	}
	ret := engine.InitDB("/tmp/ldb", 64, 128)
	if ret != 0 {
		t.Fatalf("engine InitDB error\n")
	}
	engine.FinaliseDB()
}
