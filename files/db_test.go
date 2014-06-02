// Copyright (C) 2014 Jakob Borg and other contributors. All rights reserved.
// Use of this source code is governed by an MIT-style license that can be
// found in the LICENSE file.

package files

import (
	"crypto/rand"
	"testing"

	"github.com/calmh/syncthing/scanner"
)

var fdb *fileDB

func init() {
	var err error
	fdb, err = newFileDB("testdata/test.db")
	if err != nil {
		panic(err)
	}
}

func TestUpdateFile(t *testing.T) {
	rfdb := &repoFileDB{
		fileDB: *fdb,
		repo:   "test",
	}

	f := scanner.File{
		Name:       "foo",
		Flags:      2134,
		Modified:   3249879,
		Version:    876324,
		Suppressed: false,
	}

	for i := 0; i < 10; i++ {
		bs := make([]byte, 32)
		rand.Reader.Read(bs)
		b := scanner.Block{
			Size:   128 * 1024,
			Offset: int64(i * 128 * 1024),
			Hash:   bs,
		}
		f.Blocks = append(f.Blocks, b)
	}

	err := rfdb.updateFile(32, f)
	if err != nil {
		t.Fatal(err)
	}
}
