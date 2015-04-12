package model

import (
	"reflect"
	"testing"

	"github.com/syncthing/protocol"
	"github.com/syncthing/syncthing/internal/db"
	"github.com/syncthing/syncthing/internal/ignore"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/storage"
)

func TestCleanSubs(t *testing.T) {
	testcases := [][][]string{
		{{"a"}, {"a"}},
		{{"a", "b"}, {"a", "b"}},
		{{"/a"}, {"a"}}, // absolute path is resolved
		{{"//a"}, {"a"}},
		{{"/a/../b"}, {"b"}},
		{{"a", "nonexistent"}, {".", "a"}}, // nonexistent subdir gets simplified to "."
		{{"a", "a"}, {"a"}},                // duplicates get removed
		{{"a", "a/b", "b"}, {"a", "b"}},    // "a/b" is covered by "a" and duplicates get removed
	}

	ldb, _ := leveldb.Open(storage.NewMemStorage(), nil)
	fs := db.NewFileSet("default", ldb)
	fs.Replace(protocol.LocalDeviceID, []protocol.FileInfo{
		protocol.FileInfo{Name: "a", Flags: protocol.FlagDirectory},
		protocol.FileInfo{Name: "b", Flags: protocol.FlagDirectory},
		protocol.FileInfo{Name: "c"},
		protocol.FileInfo{Name: "a/b", Flags: protocol.FlagDirectory},
		protocol.FileInfo{Name: "b/c"},
		protocol.FileInfo{Name: "a/b/c"},
	})

	sc := folderScanner{
		path:    "foo",
		fs:      fs,
		ignores: ignore.New(false),
	}

	for _, tc := range testcases {
		if err := sc.validSubs(tc[0]); err != nil {
			t.Error(err)
			continue
		}

		actual := sc.cleanSubs(tc[0])
		expected := tc[1]
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("%v != %v", actual, expected)
		}
	}
}
