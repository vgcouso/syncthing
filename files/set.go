// Copyright (C) 2014 Jakob Borg and other contributors. All rights reserved.
// Use of this source code is governed by an MIT-style license that can be
// found in the LICENSE file.

// Package files provides a set type to track local/remote files with newness checks.
package files

import (
	"bytes"
	"errors"
	"sync"

	"github.com/boltdb/bolt"
	"github.com/calmh/syncthing/protocol"
	"github.com/calmh/syncthing/scanner"
)

type fileRecord struct {
	File   scanner.File
	Usage  int
	Global bool
}

type bitset uint64

type Set struct {
	changes [64]uint64
	cMut    sync.Mutex

	repo string
	db   *bolt.DB
}

func NewSet(repo string, db *bolt.DB) *Set {
	var m = Set{
		repo: repo,
		db:   db,
	}
	return &m
}

func (m *Set) Replace(id uint, fs []scanner.File) {
	if debug {
		l.Debugf("%s Replace(%d, [%d])", m.repo, id, len(fs))
	}
	if id > 63 {
		panic("Connection ID must be in the range 0 - 63 inclusive")
	}

	m.cMut.Lock()
	m.changes[id] = 0
	for _, f := range fs {
		if f.Version > m.changes[id] {
			m.changes[id] = f.Version
		}
	}
	m.cMut.Unlock()

	err := m.db.Update(boltReplace(id, m.repo, fs))
	l.FatalErr(err)
}

func (m *Set) ReplaceWithDelete(id uint, fs []scanner.File) {
	if debug {
		l.Debugf("%s ReplaceWithDelete(%d, [%d])", m.repo, id, len(fs))
	}
	if id > 63 {
		panic("Connection ID must be in the range 0 - 63 inclusive")
	}

	m.cMut.Lock()
	m.changes[id] = 0
	for _, f := range fs {
		if f.Version > m.changes[id] {
			m.changes[id] = f.Version
		}
	}
	m.cMut.Unlock()

	err := m.db.Update(boltReplaceWithDelete(id, m.repo, fs))
	l.FatalErr(err)
}

func (m *Set) Update(id uint, fs []scanner.File) {
	if debug {
		l.Debugf("%s Update(%d, [%d])", m.repo, id, len(fs))
	}
	if id > 63 {
		panic("Connection ID must be in the range 0 - 63 inclusive")
	}

	m.cMut.Lock()
	for _, f := range fs {
		if f.Version > m.changes[id] {
			m.changes[id] = f.Version
		}
	}
	m.cMut.Unlock()

	err := m.db.Update(boltUpdate(id, m.repo, fs))
	l.FatalErr(err)
}

func (m *Set) Need(id uint) []scanner.File {
	if debug {
		l.Debugf("%s Need(%d)", m.repo, id)
	}
	if id > 63 {
		panic("Connection ID must be in the range 0 - 63 inclusive")
	}

	var need []scanner.File
	var f scanner.File

	appendNeed := func(bs []byte) {
		err := f.UnmarshalXDR(bs)
		l.FatalErr(err)
		if !protocol.IsDeleted(f.Flags) {
			need = append(need, f)
		}
	}

	err := m.db.View(func(tx *bolt.Tx) error {
		gbkt := tx.Bucket([]byte("global"))
		if gbkt == nil {
			return errors.New("no global bucket")
		}

		gbkt = gbkt.Bucket([]byte(m.repo))
		if gbkt == nil {
			// The repo is unknown
			return nil
		}

		hbkt := tx.Bucket([]byte("files"))
		if hbkt == nil {
			return errors.New("no files bucket")
		}

		hbkt = hbkt.Bucket([]byte(m.repo))
		if hbkt == nil {
			// The repo is unknown
			return nil
		}

		hbkt = hbkt.Bucket([]byte{byte(id)})
		if hbkt == nil {
			// The node has no files
			need = boltBucketFiles(gbkt)
			return nil
		}

		hc := hbkt.Cursor()
		gc := gbkt.Cursor()

		hk, hv := hc.First()
		gk, gv := gc.First()

		var hf scanner.File
		var gf scanner.File

		for gk != nil {
			if hk == nil {
				// Node index is at the end
				appendNeed(gv)
			} else {
				d := bytes.Compare(gk, hk)
				if d < 0 {
					// Node is missing a file
					appendNeed(gv)
				} else if d == 0 {
					// File is there, need to compare version
					err := hf.UnmarshalXDR(hv)
					l.FatalErr(err)
					err = gf.UnmarshalXDR(gv)
					l.FatalErr(err)
					if gf.NewerThan(hf) {
						need = append(need, gf)
					}
					hk, hv = hc.Next()
				} else {
					panic("global index corrupt")
				}
			}

			gk, gv = gc.Next()
		}

		return nil
	})
	l.FatalErr(err)

	return need
}

func (m *Set) Have(id uint) []scanner.File {
	if debug {
		l.Debugf("%s Have(%d)", m.repo, id)
	}
	if id > 63 {
		panic("Connection ID must be in the range 0 - 63 inclusive")
	}

	var fs []scanner.File

	err := m.db.View(func(tx *bolt.Tx) error {
		bkt := tx.Bucket([]byte("files"))
		if bkt == nil {
			return errors.New("no root bucket")
		}

		bkt = bkt.Bucket([]byte(m.repo))
		if bkt == nil {
			return nil
		}

		bkt = bkt.Bucket([]byte{byte(id)})
		if bkt == nil {
			return nil
		}

		fs = boltBucketFiles(bkt)

		return nil
	})
	l.FatalErr(err)

	return fs
}

func (m *Set) Global() []scanner.File {
	if debug {
		l.Debugf("%s Global()", m.repo)
	}

	var fs []scanner.File

	err := m.db.View(func(tx *bolt.Tx) error {
		bkt := tx.Bucket([]byte("global"))
		if bkt == nil {
			return errors.New("no global bucket")
		}

		bkt = bkt.Bucket([]byte(m.repo))
		if bkt == nil {
			return nil
		}

		fs = boltBucketFiles(bkt)
		return nil
	})
	l.FatalErr(err)

	return fs
}

func (m *Set) Get(id uint, file string) scanner.File {
	if id > 63 {
		panic("Connection ID must be in the range 0 - 63 inclusive")
	}

	var f scanner.File
	err := m.db.View(func(tx *bolt.Tx) error {
		bkt := tx.Bucket([]byte("files"))
		if bkt == nil {
			return nil
		}

		bkt = bkt.Bucket([]byte(m.repo))
		if bkt == nil {
			return nil
		}

		bkt = bkt.Bucket([]byte{byte(id)})
		if bkt == nil {
			return nil
		}

		v := bkt.Get([]byte(file))
		if v == nil {
			return nil
		}

		return f.UnmarshalXDR(v)
	})
	l.FatalErr(err)
	return f
}

func (m *Set) GetGlobal(file string) scanner.File {
	var f scanner.File
	err := m.db.View(func(tx *bolt.Tx) error {
		bkt := tx.Bucket([]byte("global"))
		if bkt == nil {
			return errors.New("no global bucket")
		}

		bkt = bkt.Bucket([]byte(m.repo))
		if bkt == nil {
			return errors.New("no repo bucket")
		}

		v := bkt.Get([]byte(file))
		if v == nil {
			return nil
		}

		return f.UnmarshalXDR(v)
	})
	l.FatalErr(err)
	return f
}

func (m *Set) Availability(name string) bitset {
	var av bitset

	m.db.View(func(tx *bolt.Tx) error {
		bkt := tx.Bucket([]byte("global"))
		if bkt == nil {
			return nil
		}

		bkt = bkt.Bucket([]byte(m.repo))
		if bkt == nil {
			return nil
		}

		gv := bkt.Get([]byte(name))
		if gv == nil {
			panic("availability for nonexistent file")
		}
		var gf scanner.File
		err := gf.UnmarshalXDR(gv)
		if err != nil {
			return err
		}

		bkt = tx.Bucket([]byte("files"))
		if bkt == nil {
			return nil
		}

		bkt = bkt.Bucket([]byte(m.repo))
		if bkt == nil {
			return nil
		}

		c := bkt.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			if v != nil {
				panic("non-bucket key under repo")
			}

			nbkt := bkt.Bucket(k)
			lv := nbkt.Get([]byte(name))
			if lv == nil {
				continue
			}
			var lf scanner.File
			lf.UnmarshalXDR(lv)
			if lf.Version == gf.Version {
				av |= 1 << uint(k[0])
			}
		}
		return nil
	})

	return av
}

func (m *Set) Changes(id uint) uint64 {
	if id > 63 {
		panic("Connection ID must be in the range 0 - 63 inclusive")
	}
	m.cMut.Lock()
	defer m.cMut.Unlock()
	return m.changes[id]
}
