// Copyright (C) 2014 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

package db

import (
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/boltdb/bolt"
	"github.com/syncthing/protocol"
)

func genBlocks(n int) []protocol.BlockInfo {
	b := make([]protocol.BlockInfo, n)
	for i := range b {
		h := make([]byte, 32)
		for i := 0; i < len(h)/4; i += 4 {
			r := rand.Uint32()
			h[i*4] = byte(r)
			h[i*4+1] = byte(r >> 8)
			h[i*4+2] = byte(r >> 16)
			h[i*4+3] = byte(r >> 24)
		}
		b[i].Hash = h

		if i == n-1 {
			b[i].Size = 1234
		} else {
			b[i].Size = protocol.BlockSize
		}
	}
	return b
}

var f1, f2, f3 protocol.FileInfo

func init() {
	blocks := genBlocks(30)

	f1 = protocol.FileInfo{
		Name:   "f1",
		Blocks: blocks[:10],
	}

	f2 = protocol.FileInfo{
		Name:   "f2",
		Blocks: blocks[10:20],
	}

	f3 = protocol.FileInfo{
		Name:   "f3",
		Blocks: blocks[20:],
	}
}

func tmpDb() *bolt.DB {
	db, err := bolt.Open(fmt.Sprintf("testdata/test-%d.db", time.Now().UnixNano()), 0644, nil)
	if err != nil {
		panic(err)
	}
	db.NoSync = true
	return db
}

func TestBlockMapAddUpdateWipe(t *testing.T) {
	db := tmpDb()
	defer func() {
		db.Close()
		os.RemoveAll(db.Path())
	}()
	m := NewBlockMap(db, "foo")

	f3.Flags |= protocol.FlagDirectory

	m.Add([]protocol.FileInfo{f1, f2, f3})

	m.Iterate(f1.Blocks[0].Hash, func(file string, index int) bool {
		if file != "f1" || index != 0 {
			t.Fatal("Mismatch")
		}
		return true
	})

	m.Iterate(f2.Blocks[0].Hash, func(file string, index int) bool {
		if file != "f2" || index != 0 {
			t.Fatal("Mismatch")
		}
		return true
	})

	m.Iterate(f3.Blocks[0].Hash, func(file string, index int) bool {
		t.Fatal("Unexpected block")
		return true
	})

	f3.Flags = f1.Flags
	f1.Flags |= protocol.FlagDeleted
	f2.Flags |= protocol.FlagInvalid

	// Should remove
	m.Update([]protocol.FileInfo{f1, f2, f3})

	m.Iterate(f1.Blocks[0].Hash, func(file string, index int) bool {
		t.Fatal("Unexpected block")
		return false
	})

	m.Iterate(f2.Blocks[0].Hash, func(file string, index int) bool {
		t.Fatal("Unexpected block")
		return false
	})

	m.Iterate(f3.Blocks[0].Hash, func(file string, index int) bool {
		if file != "f3" || index != 0 {
			t.Fatal("Mismatch")
		}
		return true
	})

	f1.Flags = 0
	f2.Flags = 0
	f3.Flags = 0
}

func TestBlockFix(t *testing.T) {
	db := tmpDb()
	defer func() {
		db.Close()
		os.RemoveAll(db.Path())
	}()

	iterFn := func(file string, index int) bool {
		return true
	}

	m := NewBlockMap(db, "folder1")
	m.Add([]protocol.FileInfo{f1})

	if !m.Iterate(f1.Blocks[0].Hash, iterFn) {
		t.Fatal("Block not found")
	}

	m.Fix(f1.Name, 0, f1.Blocks[0].Hash, f2.Blocks[0].Hash)

	if m.Iterate(f1.Blocks[0].Hash, iterFn) {
		t.Fatal("Unexpected block")
	}

	if !m.Iterate(f2.Blocks[0].Hash, iterFn) {
		t.Fatal("Block not found")
	}
}

func BenchmarkBlockMapAdd(b *testing.B) {
	db := tmpDb()
	defer func() {
		db.Close()
		os.RemoveAll(db.Path())
	}()
	m := NewBlockMap(db, "foo")

	f := protocol.FileInfo{
		Name:   "A moderately long filename such as would be seen when things are a few directories deep or are movie files or something",
		Blocks: genBlocks(100000), // This is a 12 GB file
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		m.Add([]protocol.FileInfo{f})
	}

	b.ReportAllocs()
}

/*
func TestBlockMapAdd_12GB(t *testing.T) {
	testBlockMapAdd(t, 1)
}

func TestBlockMapAdd_250GB(t *testing.T) {
	testBlockMapAdd(t, 250/12)
}

func TestBlockMapAdd_500GB(t *testing.T) {
	testBlockMapAdd(t, 500/12)
}

func TestBlockMapAdd_1TB(t *testing.T) {
	testBlockMapAdd(t, 1000/12)
}

func TestBlockMapAdd_5TB(t *testing.T) {
	testBlockMapAdd(t, 5000/12)
}

func TestBlockMapAdd_8TB(t *testing.T) {
	testBlockMapAdd(t, 8000/12)
}

func testBlockMapAdd(t *testing.T, files int) {
	db := tmpDb()
	defer func() {
		db.Close()
		os.RemoveAll(db.Path())
	}()
	m := NewBlockMap(db, "foo")

	var ms0, ms1 runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&ms0)

	// We add 500 * 12 GB files to the repository, for a total of 6 TB of data.
	for i := 0; i < files; i++ {
		f := protocol.FileInfo{
			Name:   fmt.Sprintf("A moderately long filename such as would be seen when things are a few directories deep or are movie files or something %d", i),
			Blocks: genBlocks(100000), // This is a 12 GB file
		}
		m.Add([]protocol.FileInfo{f})
	}

	runtime.GC()
	runtime.ReadMemStats(&ms1)

	log.Println("Heap:", ms1.HeapInuse/1024, "KiB, increase:", (ms1.HeapInuse-ms0.HeapInuse)/1024, "KiB")
}
*/
