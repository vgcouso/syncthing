// Copyright (C) 2014 Jakob Borg and other contributors. All rights reserved.
// Use of this source code is governed by an MIT-style license that can be
// found in the LICENSE file.

package db

import (
	"database/sql"
	"fmt"
	"log"

	//_ "github.com/mxk/go-sqlite/sqlite3"
	_ "github.com/mattn/go-sqlite3"
	"github.com/syncthing/protocol"
	"github.com/syncthing/syncthing/internal/lamport"
)

var connectionSetup = []string{
	`PRAGMA synchronous = NORMAL`,
	`PRAGMA foreign_keys = ON`,
}

var schemaSetup = []string{
	`CREATE TABLE IF NOT EXISTS File (
		ID INTEGER PRIMARY KEY AUTOINCREMENT,
		Device BLOB NOT NULL,
		Folder TEXT NOT NULL,
		Name TEXT NOT NULL,
		Flags INTEGER NOT NULL,
		Modified INTEGER NOT NULL,
		Version INTEGER NOT NULL,
		Updated BOOLEAN NOT NULL
	)`,
	`CREATE UNIQUE INDEX IF NOT EXISTS DeviceFolderNameIdx ON File (Device, Folder, Name)`,
	`CREATE INDEX IF NOT EXISTS NameVersionIdx ON File (Name, Version)`,
	`CREATE TABLE IF NOT EXISTS Block (
		Hash BLOB NOT NULL,
		FileID INTEGER NOT NULL REFERENCES File(ID) ON DELETE CASCADE,
		Size INTEGER NOT NULL,
		Offs INTEGER NOT NULL
	)`,
	`CREATE INDEX IF NOT EXISTS HashIdx ON Block (Hash)`,
	`CREATE INDEX IF NOT EXISTS FileIDIdx ON Block (FileID)`,
}

var preparedStmts = [][2]string{
	{"selectFileID",
		"SELECT ID, Version FROM File WHERE Device==? AND Folder==? AND Name==?"},
	{"selectFileAll",
		"SELECT ID, Name, Flags, Modified, Version FROM File WHERE Device==? AND Folder==? AND Name==?"},
	{"selectFileAllID",
		"SELECT ID, Name, Flags, Modified, Version FROM File WHERE ID==?"},
	{"selectFileAllVersion",
		fmt.Sprintf("SELECT ID, Name, Flags, Modified, Version FROM File WHERE Name==? AND Version==? AND Flags & %d == 0", protocol.FlagInvalid)},
	{"deleteFile",
		"DELETE FROM File WHERE ID==?"},
	{"updateFile",
		"UPDATE File SET Updated=1 WHERE ID==?"},
	{"deleteBlocksFor",
		"DELETE FROM Block WHERE FileID==?"},
	{"insertFile",
		"INSERT INTO File (Device, Folder, Name, Flags, Modified, Version, Updated) VALUES (?, ?, ?, ?, ?, ?, 1)"},
	{"insertBlock",
		"INSERT INTO Block VALUES (?, ?, ?, ?)"},
	{"selectBlock",
		"SELECT Hash, Size, Offs FROM Block WHERE FileID==?"},
	{"selectFileHave",
		"SELECT ID, Name, Flags, Modified, Version FROM File WHERE Device==? AND Folder==?"},
	{"selectFileGlobal",
		fmt.Sprintf("SELECT ID, Name, Flags, Modified, MAX(Version) FROM File WHERE Folder==? AND Flags & %d == 0 GROUP BY Name ORDER BY Name", protocol.FlagInvalid)},
	{"selectMaxID",
		"SELECT MAX(ID) FROM File WHERE Device==? AND Folder==?"},
	{"selectGlobalID",
		fmt.Sprintf("SELECT MAX(ID) FROM File WHERE Folder==? AND Name==? AND Flags & %d == 0", protocol.FlagInvalid)},
	{"selectMaxVersion",
		"SELECT MAX(Version) FROM File WHERE Folder==? AND Name==?"},
	{"selectWithVersion",
		"SELECT Device, Flags FROM File WHERE Folder==? AND Name==? AND Version==?"},
	{"selectNeed",
		`SELECT Name, MAX(Version) Version FROM File WHERE Folder==? GROUP BY Name EXCEPT
			SELECT Name, Version FROM File WHERE Device==? AND Folder==?`},
}

type FileDB struct {
	db    *sql.DB
	stmts map[string]*sql.Stmt
}

func NewFileDB(name string) (*FileDB, error) {
	db, err := sql.Open("sqlite3", name)
	if err != nil {
		return nil, err
	}

	for _, stmt := range connectionSetup {
		_, err = db.Exec(stmt)
		if err != nil {
			return nil, err
		}
	}

	tx, err := db.Begin()
	if err != nil {
		return nil, err
	}

	for _, stmt := range schemaSetup {
		_, err = tx.Exec(stmt)
		if err != nil {
			return nil, err
		}
	}

	if err = tx.Commit(); err != nil {
		return nil, err
	}

	fdb := FileDB{
		db:    db,
		stmts: make(map[string]*sql.Stmt),
	}

	for _, prep := range preparedStmts {
		stmt, err := db.Prepare(prep[1])
		if err != nil {
			return nil, err
		}
		fdb.stmts[prep[0]] = stmt
	}

	return &fdb, nil
}

func (db *FileDB) update(folder string, device protocol.DeviceID, fs []protocol.FileInfo) error {
	tx, err := db.db.Begin()
	if err != nil {
		panic(err)
	}

	db.updateTx(folder, device, fs, tx)

	return tx.Commit()
}

func (db *FileDB) updateTx(folder string, device protocol.DeviceID, fs []protocol.FileInfo, tx *sql.Tx) error {
	for _, f := range fs {
		var id int64
		var version uint64

		row := tx.Stmt(db.stmts["selectFileID"]).QueryRow(device[:], folder, f.Name)
		err := row.Scan(&id, &version)

		if f.IsInvalid() {
			// Force an update
			version = 0
		}

		if err == nil && version != f.Version {
			_, err = tx.Stmt(db.stmts["deleteFile"]).Exec(id)
			if err != nil {
				panic(err)
			}
			_, err = tx.Stmt(db.stmts["deleteBlocksFor"]).Exec(id)
			if err != nil {
				panic(err)
			}
		} else if err == nil && version == f.Version {
			_, err = tx.Stmt(db.stmts["updateFile"]).Exec(id)
			if err != nil {
				panic(err)
			}
		} else if err != nil && err != sql.ErrNoRows {
			panic(err)
		}

		if version != f.Version {
			rs, err := tx.Stmt(db.stmts["insertFile"]).Exec(device[:], folder, f.Name, f.Flags, f.Modified, f.Version)
			if err != nil {
				panic(err)
			}
			id, _ = rs.LastInsertId()

			for _, b := range f.Blocks {
				_, err = tx.Stmt(db.stmts["insertBlock"]).Exec(b.Hash, id, b.Size, b.Offset)
				if err != nil {
					panic(err)
				}
			}
		}
	}

	return nil
}

func (db *FileDB) updateWithDelete(folder string, device protocol.DeviceID, fs []protocol.FileInfo) error {
	tx, err := db.db.Begin()
	if err != nil {
		panic(err)
	}

	_, err = tx.Exec("UPDATE File SET Updated==0 WHERE Device==? AND Folder==?", device[:], folder)
	if err != nil {
		panic(err)
	}

	db.updateTx(folder, device, fs, tx)

	rows, err := tx.Query("SELECT ID, Flags, Version FROM File WHERE Folder==? AND Device==? AND Updated==0", folder, device[:])
	if err != nil && err != sql.ErrNoRows {
		panic(err)
	}
	for rows.Next() {
		var id, version uint64
		var flags uint32
		err := rows.Scan(&id, &flags, &version)
		if err != nil {
			panic(err)
		}
		if flags&protocol.FlagDeleted == 0 {
			flags |= protocol.FlagDeleted
			_, err = tx.Exec("UPDATE File SET Flags=?, Version=? WHERE ID==?", flags, lamport.Default.Tick(version), id)
			if err != nil {
				panic(err)
			}
			_, err = tx.Exec("DELETE FROM Block WHERE FileID==?", id)
			if err != nil {
				panic(err)
			}
		}
	}

	return tx.Commit()
}

func (db *FileDB) replace(folder string, device protocol.DeviceID, fs []protocol.FileInfo) error {
	tx, err := db.db.Begin()
	if err != nil {
		panic(err)
	}

	db.replaceTx(folder, device, fs, tx)

	return tx.Commit()
}

func (db *FileDB) replaceTx(folder string, device protocol.DeviceID, fs []protocol.FileInfo, tx *sql.Tx) error {
	_, err := tx.Exec("UPDATE File SET Updated==0 WHERE Device==? AND Folder==?", device[:], folder)
	if err != nil {
		panic(err)
	}

	for _, f := range fs {
		var id int64
		var version uint64

		row := tx.Stmt(db.stmts["selectFileID"]).QueryRow(device[:], folder, f.Name)
		err := row.Scan(&id, &version)

		if err == nil && version != f.Version {
			_, err = tx.Stmt(db.stmts["deleteFile"]).Exec(id)
			if err != nil {
				panic(err)
			}
			_, err = tx.Stmt(db.stmts["deleteBlocksFor"]).Exec(id)
			if err != nil {
				panic(err)
			}
		} else if err != nil && err != sql.ErrNoRows {
			panic(err)
		}

		if version != f.Version {
			rs, err := tx.Stmt(db.stmts["insertFile"]).Exec(device[:], folder, f.Name, f.Flags, f.Modified, f.Version)
			if err != nil {
				panic(err)
			}
			id, _ = rs.LastInsertId()

			for _, b := range f.Blocks {
				_, err = tx.Stmt(db.stmts["insertBlock"]).Exec(b.Hash, id, b.Size, b.Offset)
				if err != nil {
					panic(err)
				}
			}
		}
	}

	_, err = tx.Exec("DELETE FROM File WHERE Folder==? AND Device==? AND Updated==0", folder, device[:])
	if err != nil {
		panic(err)
	}

	return nil
}

func (db *FileDB) have(folder string, device protocol.DeviceID, fn Iterator) {
	rows, err := db.stmts["selectFileHave"].Query(device[:], folder)
	if err == sql.ErrNoRows {
		return
	}
	if err != nil {
		panic(err)
	}
	defer rows.Close()

	for rows.Next() {
		var f protocol.FileInfo
		var id int64
		err = rows.Scan(&id, &f.Name, &f.Flags, &f.Modified, &f.Version)
		if err != nil {
			panic(err)
		}

		brows, err := db.stmts["selectBlock"].Query(id)
		if err != nil && err != sql.ErrNoRows {
			panic(err)
		}

		for brows.Next() {
			var b protocol.BlockInfo
			brows.Scan(&b.Hash, &b.Size, &b.Offset)
			f.Blocks = append(f.Blocks, b)
		}

		if !fn(f) {
			return
		}
	}
}

func (db *FileDB) haveTruncated(folder string, device protocol.DeviceID, fn Iterator) {
	rows, err := db.stmts["selectFileHave"].Query(device[:], folder)
	if err == sql.ErrNoRows {
		return
	}
	if err != nil {
		panic(err)
	}
	defer rows.Close()

	for rows.Next() {
		var f FileInfoTruncated
		var id int64
		err = rows.Scan(&id, &f.Name, &f.Flags, &f.Modified, &f.Version)
		if err != nil {
			panic(err)
		}

		if !fn(f) {
			return
		}
	}
}

func (db *FileDB) global(folder string, fn Iterator) {
	rows, err := db.stmts["selectFileGlobal"].Query(folder)
	if err == sql.ErrNoRows {
		return
	}
	if err != nil {
		panic(err)
	}
	defer rows.Close()

	for rows.Next() {
		var f protocol.FileInfo
		var id int64
		err = rows.Scan(&id, &f.Name, &f.Flags, &f.Modified, &f.Version)
		if err != nil {
			panic(err)
		}

		brows, err := db.stmts["selectBlock"].Query(id)
		if err != nil && err != sql.ErrNoRows {
			panic(err)
		}

		for brows.Next() {
			var b protocol.BlockInfo
			brows.Scan(&b.Hash, &b.Size, &b.Offset)
			f.Blocks = append(f.Blocks, b)
		}

		if !fn(f) {
			return
		}
	}
}

func (db *FileDB) globalTruncated(folder string, fn Iterator) {
	rows, err := db.stmts["selectFileGlobal"].Query(folder)
	if err == sql.ErrNoRows {
		return
	}
	if err != nil {
		panic(err)
	}
	defer rows.Close()

	for rows.Next() {
		var f FileInfoTruncated
		var id int64
		err = rows.Scan(&id, &f.Name, &f.Flags, &f.Modified, &f.Version)
		if err != nil {
			panic(err)
		}

		if !fn(f) {
			return
		}
	}
}

func (db *FileDB) need(folder string, device protocol.DeviceID, fn Iterator) {
	rows, err := db.stmts["selectNeed"].Query(folder, device[:], folder)
	if err == sql.ErrNoRows {
		return
	}
	if err != nil {
		panic(err)
	}
	defer rows.Close()

	for rows.Next() {
		var name string
		var version uint64
		var id int64
		err = rows.Scan(&name, &version)
		if err != nil {
			panic(err)
		}

		var f protocol.FileInfo
		row := db.stmts["selectFileAllVersion"].QueryRow(name, version)
		err = row.Scan(&id, &f.Name, &f.Flags, &f.Modified, &f.Version)
		if err == sql.ErrNoRows {
			// There was no file to need, maybe because they're all marked invalid
			continue
		}
		if err != nil {
			panic(err)
		}

		brows, err := db.stmts["selectBlock"].Query(id)
		if err != nil && err != sql.ErrNoRows {
			panic(err)
		}

		for brows.Next() {
			var b protocol.BlockInfo
			brows.Scan(&b.Hash, &b.Size, &b.Offset)
			f.Blocks = append(f.Blocks, b)
		}

		if !fn(f) {
			return
		}
	}
}

func (db *FileDB) needTruncated(folder string, device protocol.DeviceID, fn Iterator) {
	rows, err := db.stmts["selectNeed"].Query(folder, device[:], folder)
	if err == sql.ErrNoRows {
		return
	}
	if err != nil {
		panic(err)
	}
	defer rows.Close()

	for rows.Next() {
		var name string
		var version uint64
		var id int64
		err = rows.Scan(&name, &version)
		if err != nil {
			panic(err)
		}

		log.Println(id, name, version)
		var f FileInfoTruncated
		row := db.stmts["selectFileAllVersion"].QueryRow(name, version)
		err = row.Scan(&id, &f.Name, &f.Flags, &f.Modified, &f.Version)
		if err == sql.ErrNoRows {
			// There was no file to need, maybe because they're all marked invalid
			continue
		}
		if err != nil {
			panic(err)
		}

		if !fn(f) {
			return
		}
	}
}

func (db *FileDB) get(folder string, device protocol.DeviceID, name string) (protocol.FileInfo, bool) {
	var f protocol.FileInfo
	var id int64

	row := db.stmts["selectFileAll"].QueryRow(device[:], folder, name)
	err := row.Scan(&id, &f.Name, &f.Flags, &f.Modified, &f.Version)
	if err == sql.ErrNoRows {
		return f, false
	}
	if err != nil {
		panic(err)
	}

	brows, err := db.stmts["selectBlock"].Query(id)
	if err != nil && err != sql.ErrNoRows {
		panic(err)
	}

	for brows.Next() {
		var b protocol.BlockInfo
		brows.Scan(&b.Hash, &b.Size, &b.Offset)
		f.Blocks = append(f.Blocks, b)
	}

	return f, true
}

func (db *FileDB) getGlobal(folder, name string) (protocol.FileInfo, bool) {
	var gid *uint64

	row := db.stmts["selectGlobalID"].QueryRow(folder, name)
	err := row.Scan(&gid)
	if gid == nil {
		return protocol.FileInfo{}, false
	}
	if err != nil {
		panic(err)
	}

	var id uint64
	var f protocol.FileInfo
	row = db.stmts["selectFileAllID"].QueryRow(*gid)
	err = row.Scan(&id, &f.Name, &f.Flags, &f.Modified, &f.Version)
	if err == sql.ErrNoRows {
		return f, false
	}
	if err != nil {
		panic(err)
	}

	brows, err := db.stmts["selectBlock"].Query(id)
	if err != nil && err != sql.ErrNoRows {
		panic(err)
	}

	for brows.Next() {
		var b protocol.BlockInfo
		brows.Scan(&b.Hash, &b.Size, &b.Offset)
		f.Blocks = append(f.Blocks, b)
	}

	return f, true
}

func (db *FileDB) getGlobalTruncated(folder, name string) (FileInfoTruncated, bool) {
	var gid *uint64

	row := db.stmts["selectGlobalID"].QueryRow(folder, name)
	err := row.Scan(&gid)
	if gid == nil {
		return FileInfoTruncated{}, false
	}
	if err != nil {
		panic(err)
	}

	var id uint64
	var f FileInfoTruncated
	row = db.stmts["selectFileAllID"].QueryRow(*gid)
	err = row.Scan(&id, &f.Name, &f.Flags, &f.Modified, &f.Version)
	if err == sql.ErrNoRows {
		return f, false
	}
	if err != nil {
		panic(err)
	}

	return f, true
}

func (db *FileDB) maxID(folder string, device protocol.DeviceID) uint64 {
	var id *uint64

	row := db.stmts["selectMaxID"].QueryRow(device[:], folder)
	err := row.Scan(&id)
	if id == nil {
		return 0
	}
	if err != nil {
		panic(err)
	}
	return *id
}

func (db *FileDB) availability(folder, name string) []protocol.DeviceID {
	var version *int64
	row := db.stmts["selectMaxVersion"].QueryRow(folder, name)
	err := row.Scan(&version)
	if version == nil {
		return nil
	}
	if err != nil {
		panic(err)
	}

	rows, err := db.stmts["selectWithVersion"].Query(folder, name, *version)
	if err == sql.ErrNoRows {
		return nil
	}
	if err != nil {
		panic(err)
	}

	var available []protocol.DeviceID
	var device []byte
	var flags uint32
	for rows.Next() {
		err = rows.Scan(&device, &flags)
		if err != nil {
			panic(err)
		}
		if flags&(protocol.FlagDeleted|protocol.FlagInvalid) == 0 {
			available = append(available, protocol.DeviceIDFromBytes(device))
		}
	}

	return available
}
