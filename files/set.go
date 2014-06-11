// Copyright (C) 2014 Jakob Borg and other contributors. All rights reserved.
// Use of this source code is governed by an MIT-style license that can be
// found in the LICENSE file.

// Package files provides a set type to track local/remote files with newness checks.
package files

import (
	"sync"

	"github.com/calmh/syncthing/scanner"
)

type fileRecord struct {
	File   scanner.File
	Usage  int
	Global bool
}

type bitset uint64

type Set struct {
	sync.Mutex
	files              map[key]fileRecord
	remoteKey          [64]map[string]key
	changes            [64]uint64
	globalAvailability map[string]bitset
	globalKey          map[string]key
	db                 *fileDB
}

func NewSet(repo string, dbpath string) *Set {
	db, err := newFileDB(repo, dbpath)
	if err != nil {
		l.Fatalln(err)
	}
	var m = Set{
		files:              make(map[key]fileRecord),
		globalAvailability: make(map[string]bitset),
		globalKey:          make(map[string]key),
		db:                 db,
	}
	return &m
}

func (m *Set) Replace(id uint, fs []scanner.File) {
	if debug {
		l.Debugf("Replace(%d, [%d])", id, len(fs))
	}
	if id > 63 {
		panic("Connection ID must be in the range 0 - 63 inclusive")
	}
	if err := m.db.replace(id, fs); err != nil {
		l.Fatalln(err)
	}
}

func (m *Set) ReplaceWithDelete(id uint, fs []scanner.File) {
	if debug {
		l.Debugf("ReplaceWithDelete(%d, [%d])", id, len(fs))
	}
	if id > 63 {
		panic("Connection ID must be in the range 0 - 63 inclusive")
	}
	if err := m.db.updateWithDelete(id, fs); err != nil {
		l.Fatalln(err)
	}
}

func (m *Set) Update(id uint, fs []scanner.File) {
	if debug {
		l.Debugf("Update(%d, [%d])", id, len(fs))
	}
	m.db.update(id, fs)
}

func (m *Set) Need(id uint) []scanner.File {
	if debug {
		l.Debugf("Need(%d)", id)
	}
	return m.db.need(id)
}

func (m *Set) Have(id uint) []scanner.File {
	if debug {
		l.Debugf("Have(%d)", id)
	}
	return m.db.have(id)
}

func (m *Set) Global() []scanner.File {
	if debug {
		l.Debugf("Global()")
	}
	return m.db.global()
}

func (m *Set) Get(id uint, file string) scanner.File {
	if debug {
		l.Debugf("Get(%d, %q)", id, file)
	}
	return m.db.get(id, file)
}

func (m *Set) GetGlobal(file string) scanner.File {
	if debug {
		l.Debugf("GetGlobal(%q)", file)
	}
	return m.db.getGlobal(file)
}

func (m *Set) Availability(file string) bitset {
	if debug {
		l.Debugf("Availability(%q)", file)
	}
	return bitset(m.db.availability(file))
}

func (m *Set) Changes(id uint) uint64 {
	if debug {
		l.Debugf("Changed(%d)", id)
	}
	return m.db.maxID(id)
}
