package files

import (
	"reflect"
	"testing"

	"github.com/HouzuoGuo/tiedot/db"
	"github.com/calmh/syncthing/scanner"
)

func TestSetGet(t *testing.T) {
	db, err := db.OpenDB("testdata/db")
	if err != nil {
		t.Fatal(err)
	}
	fdb := NewFiledb(db, "test")

	r0 := fileRecord{
		File: scanner.File{
			Name:     "test",
			Version:  32,
			Modified: 1234,
			Size:     56789,
		},
	}

	fdb.Set(r0)
	r1 := fdb.Get(key{
		Name:    "test",
		Version: 32,
	})

	if !reflect.DeepEqual(r0, r1) {
		t.Fatalf("%#v != %#v", r0, r1)
	}
}
