// Copyright (C) 2014 Jakob Borg and other contributors. All rights reserved.
// Use of this source code is governed by an MIT-style license that can be
// found in the LICENSE file.

package files

import (
	"database/sql"

	_ "github.com/mattn/go-sqlite3"
	"github.com/syncthing/syncthing/internal/lamport"
	"github.com/syncthing/syncthing/internal/protocol"
)

var connectionSetup = []string{
	`PRAGMA journal_mode = OFF`,
	`PRAGMA journal_mode = WAL`,
	`PRAGMA synchronous = NORMAL`,
	`PRAGMA foreign_keys = ON`,
}

var schemaSetup = []string{
	`CREATE TABLE IF NOT EXISTS File (
		ID INTEGER PRIMARY KEY AUTOINCREMENT,
		Device BLOB NOT NULL,
		Folder STRING NOT NULL,
		Name STRING NOT NULL,
		Flags INTEGER NOT NULL,
		Modified INTEGER NOT NULL,
		Version INTEGER NOT NULL,
		Updated BOOLEAN NOT NULL
	)`,
	`CREATE UNIQUE INDEX IF NOT EXISTS DeviceFolderNameIdx ON File (Device, Folder, Name)`,
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
		"SELECT ID, Name, Flags, Modified, Version FROM File WHERE Name==? AND Version==?"},
	{"deleteFile",
		"DELETE FROM File WHERE ID==?"},
	{"updateFile",
		"UPDATE File SET Updated=1 WHERE ID==?"},
	{"deleteBlock",
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
		"SELECT ID, Name, Flags, Modified, MAX(Version) FROM File WHERE Folder==? GROUP BY Name ORDER BY Name"},
	{"selectMaxID",
		"SELECT MAX(ID) FROM File WHERE Device==? AND Folder==?"},
	{"selectGlobalID",
		"SELECT MAX(ID) FROM File WHERE Folder==? AND Name==?"},
	{"selectMaxVersion",
		"SELECT MAX(Version) FROM File WHERE Folder==? AND Name==?"},
	{"selectWithVersion",
		"SELECT Device, Flags FROM File WHERE Folder==? AND Name==? AND Version==?"},
	{"selectNeed",
		"SELECT Name, MAX(Version) Version FROM File WHERE Folder==? GROUP BY Name EXCEPT SELECT Name, Version FROM File WHERE Device==? AND Folder==?"},
}

type fileDB struct {
	db    *sql.DB
	stmts map[string]*sql.Stmt
}

func NewFileDB(name string) (*fileDB, error) {
	db, err := sql.Open("sqlite3", "file:"+name)
	if err != nil {
		return nil, err
	}

	for _, stmt := range connectionSetup {
		if debug {
			l.Debugln(stmt)
		}
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
		if debug {
			l.Debugln(stmt)
		}
		_, err = tx.Exec(stmt)
		if err != nil {
			return nil, err
		}
	}

	if err = tx.Commit(); err != nil {
		return nil, err
	}

	fdb := fileDB{
		db:    db,
		stmts: make(map[string]*sql.Stmt),
	}

	for _, prep := range preparedStmts {
		stmt, err := db.Prepare(prep[1])
		if debug {
			l.Debugln(prep[1])
		}
		if err != nil {
			return nil, err
		}
		fdb.stmts[prep[0]] = stmt
	}

	return &fdb, nil
}

func (db *fileDB) update(folder string, device protocol.DeviceID, fs []protocol.FileInfo) error {
	tx, err := db.db.Begin()
	if err != nil {
		l.Fatalln(err)
	}

	db.updateTx(folder, device, fs, tx)

	return tx.Commit()
}

func (db *fileDB) updateTx(folder string, device protocol.DeviceID, fs []protocol.FileInfo, tx *sql.Tx) error {
	for _, f := range fs {
		var id int64
		var version uint64

		row := tx.Stmt(db.stmts["selectFileID"]).QueryRow(device[:], folder, f.Name)
		err := row.Scan(&id, &version)

		if err == nil && version != f.Version {
			_, err = tx.Stmt(db.stmts["deleteFile"]).Exec(id)
			if err != nil {
				l.Fatalln(err)
			}
		} else if err == nil && version == f.Version {
			_, err = tx.Stmt(db.stmts["updateFile"]).Exec(id)
			if err != nil {
				l.Fatalln(err)
			}
		} else if err != nil && err != sql.ErrNoRows {
			l.Fatalln(err)
		}

		if version != f.Version {
			rs, err := tx.Stmt(db.stmts["insertFile"]).Exec(device[:], folder, f.Name, f.Flags, f.Modified, f.Version)
			if err != nil {
				l.Fatalln(err)
			}
			id, _ = rs.LastInsertId()

			for _, b := range f.Blocks {
				_, err = tx.Stmt(db.stmts["insertBlock"]).Exec(b.Hash, id, b.Size, b.Offset)
				if err != nil {
					l.Fatalln(err)
				}
			}
		}
	}

	return nil
}

func (db *fileDB) updateWithDelete(folder string, device protocol.DeviceID, fs []protocol.FileInfo) error {
	tx, err := db.db.Begin()
	if err != nil {
		l.Fatalln(err)
	}

	_, err = tx.Exec("UPDATE File SET Updated==0 WHERE Device==? AND Folder==?", device[:], folder)
	if err != nil {
		l.Fatalln(err)
	}

	db.updateTx(folder, device, fs, tx)

	rows, err := tx.Query("SELECT ID, Flags, Version FROM File WHERE Folder==? AND Device==? AND Updated==0", folder, device[:])
	if err != nil && err != sql.ErrNoRows {
		l.Fatalln(err)
	}
	for rows.Next() {
		var id, version uint64
		var flags uint32
		err := rows.Scan(&id, &flags, &version)
		if err != nil {
			l.Fatalln(err)
		}
		if flags&protocol.FlagDeleted == 0 {
			flags |= protocol.FlagDeleted
			_, err = tx.Exec("UPDATE File SET Flags=?, Version=? WHERE ID==?", flags, lamport.Default.Tick(version), id)
			if err != nil {
				l.Fatalln(err)
			}
		}
	}

	return tx.Commit()
}

func (db *fileDB) replace(folder string, device protocol.DeviceID, fs []protocol.FileInfo) error {
	tx, err := db.db.Begin()
	if err != nil {
		l.Fatalln(err)
	}

	db.replaceTx(folder, device, fs, tx)

	return tx.Commit()
}

func (db *fileDB) replaceTx(folder string, device protocol.DeviceID, fs []protocol.FileInfo, tx *sql.Tx) error {
	_, err := tx.Exec("UPDATE File SET Updated==0 WHERE Device==? AND Folder==?", device[:], folder)
	if err != nil {
		l.Fatalln(err)
	}

	for _, f := range fs {
		var id int64
		var version uint64

		row := tx.Stmt(db.stmts["selectFileID"]).QueryRow(device[:], folder, f.Name)
		err := row.Scan(&id, &version)

		if err == nil && version != f.Version {
			_, err = tx.Stmt(db.stmts["deleteFile"]).Exec(id)
			if err != nil {
				l.Fatalln(err)
			}
		} else if err != nil && err != sql.ErrNoRows {
			l.Fatalln(err)
		}

		if version != f.Version {
			rs, err := tx.Stmt(db.stmts["insertFile"]).Exec(device[:], folder, f.Name, f.Flags, f.Modified, f.Version)
			if err != nil {
				l.Fatalln(err)
			}
			id, _ = rs.LastInsertId()

			for _, b := range f.Blocks {
				_, err = tx.Stmt(db.stmts["insertBlock"]).Exec(b.Hash, id, b.Size, b.Offset)
				if err != nil {
					l.Fatalln(err)
				}
			}
		}
	}

	_, err = tx.Exec("DELETE FROM File WHERE Folder==? AND Device==? AND Updated==0", folder, device[:])
	if err != nil {
		l.Fatalln(err)
	}

	return nil
}

func (db *fileDB) have(folder string, device protocol.DeviceID) []protocol.FileInfo {
	rows, err := db.stmts["selectFileHave"].Query(device[:], folder)
	if err == sql.ErrNoRows {
		return nil
	}
	if err != nil {
		l.Fatalln(err)
	}

	var files []protocol.FileInfo
	for rows.Next() {
		var f protocol.FileInfo
		var id int64
		err = rows.Scan(&id, &f.Name, &f.Flags, &f.Modified, &f.Version)
		if err != nil {
			l.Fatalln(err)
		}

		brows, err := db.stmts["selectBlock"].Query(id)
		if err != nil && err != sql.ErrNoRows {
			l.Fatalln(err)
		}

		for brows.Next() {
			var b protocol.BlockInfo
			brows.Scan(&b.Hash, &b.Size, &b.Offset)
			f.Blocks = append(f.Blocks, b)
		}

		files = append(files, f)
	}

	return files
}

func (db *fileDB) global(folder string) []protocol.FileInfo {
	rows, err := db.stmts["selectFileGlobal"].Query(folder)
	if err == sql.ErrNoRows {
		return nil
	}
	if err != nil {
		l.Fatalln(err)
	}

	var files []protocol.FileInfo
	for rows.Next() {
		var f protocol.FileInfo
		var id int64
		err = rows.Scan(&id, &f.Name, &f.Flags, &f.Modified, &f.Version)
		if err != nil {
			l.Fatalln(err)
		}

		brows, err := db.stmts["selectBlock"].Query(id)
		if err != nil && err != sql.ErrNoRows {
			l.Fatalln(err)
		}

		for brows.Next() {
			var b protocol.BlockInfo
			brows.Scan(&b.Hash, &b.Size, &b.Offset)
			f.Blocks = append(f.Blocks, b)
		}

		files = append(files, f)
	}

	return files
}

func (db *fileDB) need(folder string, device protocol.DeviceID) []protocol.FileInfo {
	rows, err := db.stmts["selectNeed"].Query(folder, device[:], folder)
	if err == sql.ErrNoRows {
		return nil
	}
	if err != nil {
		l.Fatalln(err)
	}

	var files []protocol.FileInfo
	for rows.Next() {
		var name string
		var version uint64
		var id int64
		err = rows.Scan(&name, &version)
		if err != nil {
			l.Fatalln(err)
		}

		var f protocol.FileInfo
		row := db.stmts["selectFileAllVersion"].QueryRow(name, version)
		err = row.Scan(&id, &f.Name, &f.Flags, &f.Modified, &f.Version)
		if err != nil {
			l.Fatalln(err)
		}

		brows, err := db.stmts["selectBlock"].Query(id)
		if err != nil && err != sql.ErrNoRows {
			l.Fatalln(err)
		}

		for brows.Next() {
			var b protocol.BlockInfo
			brows.Scan(&b.Hash, &b.Size, &b.Offset)
			f.Blocks = append(f.Blocks, b)
		}

		files = append(files, f)
	}

	return files
}

func (db *fileDB) get(folder string, device protocol.DeviceID, name string) protocol.FileInfo {
	var f protocol.FileInfo
	var id int64

	row := db.stmts["selectFileAll"].QueryRow(device[:], folder, name)
	err := row.Scan(&id, &f.Name, &f.Flags, &f.Modified, &f.Version)
	if err == sql.ErrNoRows {
		return f
	}
	if err != nil {
		l.Fatalln(err)
	}

	brows, err := db.stmts["selectBlock"].Query(id)
	if err != nil && err != sql.ErrNoRows {
		l.Fatalln(err)
	}

	for brows.Next() {
		var b protocol.BlockInfo
		brows.Scan(&b.Hash, &b.Size, &b.Offset)
		f.Blocks = append(f.Blocks, b)
	}

	return f
}

func (db *fileDB) getGlobal(folder, name string) protocol.FileInfo {
	var f protocol.FileInfo
	var gid *uint64

	row := db.stmts["selectGlobalID"].QueryRow(folder, name)
	err := row.Scan(&gid)
	if gid == nil {
		return f
	}
	if err != nil {
		l.Fatalln(err)
	}

	var id uint64
	row = db.stmts["selectFileAllID"].QueryRow(*gid)
	err = row.Scan(&id, &f.Name, &f.Flags, &f.Modified, &f.Version)
	if err == sql.ErrNoRows {
		return f
	}
	if err != nil {
		l.Fatalln(err)
	}

	brows, err := db.stmts["selectBlock"].Query(id)
	if err != nil && err != sql.ErrNoRows {
		l.Fatalln(err)
	}

	for brows.Next() {
		var b protocol.BlockInfo
		brows.Scan(&b.Hash, &b.Size, &b.Offset)
		f.Blocks = append(f.Blocks, b)
	}

	return f
}

func (db *fileDB) maxID(folder string, device protocol.DeviceID) uint64 {
	var id *uint64

	row := db.stmts["selectMaxID"].QueryRow(device[:], folder)
	err := row.Scan(&id)
	if id == nil {
		return 0
	}
	if err != nil {
		l.Fatalln(err)
	}
	return *id
}

func (db *fileDB) availability(folder, name string) []protocol.DeviceID {
	var version *int64
	row := db.stmts["selectMaxVersion"].QueryRow(folder, name)
	err := row.Scan(&version)
	if version == nil {
		return nil
	}
	if err != nil {
		l.Fatalln(err)
	}

	rows, err := db.stmts["selectWithVersion"].Query(folder, name, *version)
	if err == sql.ErrNoRows {
		return nil
	}
	if err != nil {
		l.Fatalln(err)
	}

	var available []protocol.DeviceID
	var device []byte
	var flags uint32
	for rows.Next() {
		err = rows.Scan(&device, &flags)
		if err != nil {
			l.Fatalln(err)
		}
		if flags&(protocol.FlagDeleted|protocol.FlagInvalid) == 0 {
			available = append(available, protocol.DeviceIDFromBytes(device))
		}
	}

	return available
}
