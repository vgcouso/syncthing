// Copyright (C) 2014 Jakob Borg and other contributors. All rights reserved.
// Use of this source code is governed by an MIT-style license that can be
// found in the LICENSE file.

// Package files provides a set type to track local/remote files with newness checks.
package files

import (
	"sync"

	"github.com/calmh/syncthing/scanner"
	"github.com/cznic/kv"
)

var db *kvdb

func init() {
	odb, err := kv.CreateTemp("/tmp", "foo", "db", &kv.Options{})
	if err != nil {
		panic(err)
	}
	db = &kvdb{odb}
}

type fileRecord struct {
	File   scanner.File
	Usage  int
	Global bool
}

type bitset uint64

type Set struct {
	sync.Mutex
	repo string
}

func NewSet(repo string) *Set {
	var m = Set{
		repo: repo,
	}
	return &m
}

func (m *Set) Replace(id uint, fs []scanner.File) {
}

func (m *Set) ReplaceWithDelete(id uint, fs []scanner.File) {
}

func (m *Set) Update(id uint, fs []scanner.File) {
}

func (m *Set) Need(id uint) []scanner.File {
}

func (m *Set) Have(id uint) []scanner.File {
}

func (m *Set) Global() []scanner.File {
}

func (m *Set) Get(id uint, file string) scanner.File {
}

func (m *Set) GetGlobal(file string) scanner.File {
}

func (m *Set) Availability(name string) bitset {
}

func (m *Set) Changes(id uint) uint64 {

}
