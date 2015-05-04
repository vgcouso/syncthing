// Copyright (C) 2014 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

// Package db provides a set type to track local/remote files with newness
// checks. We must do a certain amount of normalization in here. We will get
// fed paths with either native or wire-format separators and encodings
// depending on who calls us. We transform paths to wire-format (NFC and
// slashes) on the way to the database, and transform to native format
// (varying separator and encoding) on the way back out.
package db

import (
	"bytes"
	"encoding/binary"
	"sort"

	"github.com/boltdb/bolt"
	"github.com/syncthing/protocol"
	"github.com/syncthing/syncthing/internal/config"
	"github.com/syncthing/syncthing/internal/osutil"
	"github.com/syncthing/syncthing/internal/sync"
)

var blockFinder *BlockFinder
var blocksBucketID = []byte("blocks")

type BlockMap struct {
	db     *bolt.DB
	folder []byte
}

func NewBlockMap(db *bolt.DB, folder string) *BlockMap {
	db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(blocksBucketID)
		if err != nil {
			panic(err)
		}
		return nil
	})
	return &BlockMap{
		db:     db,
		folder: []byte(folder),
	}
}

/*
	"blocks":
		<folder name>:
			<block hash>:
				<file name>: index
*/

// Add files to the block map, ignoring any deleted or invalid files.
func (m *BlockMap) Add(files []protocol.FileInfo) error {
	return m.db.Update(func(tx *bolt.Tx) error {
		folBkt, err := tx.Bucket(blocksBucketID).CreateBucketIfNotExists(m.folder)
		if err != nil {
			panic(err)
		}
		for _, file := range files {
			if file.IsDirectory() || file.IsDeleted() || file.IsInvalid() {
				continue
			}

			name := []byte(file.Name)
			for i, block := range file.Blocks {
				bkt, err := folBkt.CreateBucketIfNotExists(block.Hash)
				if err != nil {
					panic(err)
				}
				buf := make([]byte, 4)
				binary.BigEndian.PutUint32(buf, uint32(i))
				bkt.Put(name, buf)
			}
		}
		return nil
	})
}

// Update block map state, removing any deleted or invalid files.
func (m *BlockMap) Update(files []protocol.FileInfo) error {
	return m.db.Update(func(tx *bolt.Tx) error {
		folBkt, err := tx.Bucket(blocksBucketID).CreateBucketIfNotExists(m.folder)
		if err != nil {
			panic(err)
		}
		for _, file := range files {
			if file.IsDirectory() {
				continue
			}

			name := []byte(file.Name)
			if file.IsDeleted() || file.IsInvalid() {
				for _, block := range file.Blocks {
					folBkt.Bucket(block.Hash).Delete(name)
				}
				continue
			}

			for i, block := range file.Blocks {
				bkt, err := folBkt.CreateBucketIfNotExists(block.Hash)
				if err != nil {
					panic(err)
				}
				buf := make([]byte, 4)
				binary.BigEndian.PutUint32(buf, uint32(i))
				bkt.Put(name, buf)
			}
		}
		return nil
	})
}

// Discard block map state, removing the given files
func (m *BlockMap) Discard(files []protocol.FileInfo) error {
	m.db.Update(func(tx *bolt.Tx) error {
		folBkt, err := tx.Bucket(blocksBucketID).CreateBucketIfNotExists(m.folder)
		if err != nil {
			panic(err)
		}
		for _, file := range files {
			name := []byte(file.Name)
			for _, block := range file.Blocks {
				if bkt := folBkt.Bucket(block.Hash); bkt != nil {
					bkt.Delete(name)
				}
			}
		}
		return nil
	})
	return nil
}

// Drop block map, removing all entries related to this block map from the db.
func (m *BlockMap) Drop() error {
	return m.db.Update(func(tx *bolt.Tx) error {
		if err := tx.Bucket(blocksBucketID).DeleteBucket(m.folder); err != nil && err != bolt.ErrBucketNotFound {
			panic(err)
		}
		return nil
	})
}

type BlockFinder struct {
	db      *bolt.DB
	folders []string
	mut     sync.RWMutex
}

func NewBlockFinder(db *bolt.DB, cfg *config.Wrapper) *BlockFinder {
	if blockFinder != nil {
		return blockFinder
	}

	db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(blocksBucketID)
		if err != nil {
			panic(err)
		}
		return nil
	})

	f := &BlockFinder{
		db:  db,
		mut: sync.NewRWMutex(),
	}
	f.Changed(cfg.Raw())
	cfg.Subscribe(f)
	return f
}

// Changed implements config.Handler interface
func (f *BlockFinder) Changed(cfg config.Configuration) error {
	folders := make([]string, len(cfg.Folders))
	for i, folder := range cfg.Folders {
		folders[i] = folder.ID
	}

	sort.Strings(folders)

	f.mut.Lock()
	f.folders = folders
	f.mut.Unlock()

	return nil
}

// Iterate takes an iterator function which iterates over all matching blocks
// for the given hash. The iterator function has to return either true (if
// they are happy with the block) or false to continue iterating for whatever
// reason. The iterator finally returns the result, whether or not a
// satisfying block was eventually found.
func (f *BlockFinder) Iterate(hash []byte, iterFn func(string, string, int32) bool) (found bool) {
	f.mut.RLock()
	folders := f.folders
	f.mut.RUnlock()

	f.db.View(func(tx *bolt.Tx) error {
		blocBuc := tx.Bucket(blocksBucketID)
		for _, folder := range folders {
			folBkt := blocBuc.Bucket([]byte(folder))
			if folBkt == nil {
				continue
			}
			bkt := folBkt.Bucket(hash)
			if bkt == nil {
				continue
			}

			c := bkt.Cursor()
			for file, v := c.First(); file != nil; file, v = c.Next() {
				index := int32(binary.BigEndian.Uint32(v))
				if iterFn(folder, osutil.NativeFilename(string(file)), index) {
					found = true
					return nil
				}
			}
		}
		return nil
	})

	return
}

// Fix repairs incorrect blockmap entries, removing the old entry and
// replacing it with a new entry for the given block
func (f *BlockFinder) Fix(folder, file string, index int32, oldHash, newHash []byte) error {
	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(buf, uint32(index))
	f.db.Update(func(tx *bolt.Tx) error {
		folBkt, err := tx.Bucket(blocksBucketID).CreateBucketIfNotExists([]byte(folder))
		if err != nil {
			panic(err)
		}

		bkt := folBkt.Bucket(oldHash)
		if bkt != nil {
			bkt.Delete([]byte(file))
		}

		bkt, err = folBkt.CreateBucketIfNotExists(newHash)
		if err != nil {
			panic(err)
		}
		bkt.Put([]byte(file), buf)
		return nil
	})
	return nil
}

// m.blockKey returns a byte slice encoding the following information:
//	   keyTypeBlock (1 byte)
//	   folder (64 bytes)
//	   block hash (32 bytes)
//	   file name (variable size)
func toBlockKey(hash []byte, folder, file string) []byte {
	o := make([]byte, 1+64+32+len(file))
	o[0] = KeyTypeBlock
	copy(o[1:], []byte(folder))
	copy(o[1+64:], []byte(hash))
	copy(o[1+64+32:], []byte(file))
	return o
}

func fromBlockKey(data []byte) (string, string) {
	if len(data) < 1+64+32+1 {
		panic("Incorrect key length")
	}
	if data[0] != KeyTypeBlock {
		panic("Incorrect key type")
	}

	file := string(data[1+64+32:])

	slice := data[1 : 1+64]
	izero := bytes.IndexByte(slice, 0)
	if izero > -1 {
		return string(slice[:izero]), file
	}
	return string(slice), file
}
