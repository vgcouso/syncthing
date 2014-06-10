// Copyright (C) 2014 Jakob Borg and other contributors. All rights reserved.
// Use of this source code is governed by an MIT-style license that can be
// found in the LICENSE file.

package files

import (
	"database/sql"

	"github.com/calmh/syncthing/lamport"
	"github.com/calmh/syncthing/protocol"
	"github.com/calmh/syncthing/scanner"
	_ "github.com/mattn/go-sqlite3"
)

var setup = []string{
	`PRAGMA journal_mode = OFF`,
	`PRAGMA journal_mode = WAL`,
	`PRAGMA synchronous = NORMAL`,
	`PRAGMA foreign_keys = ON`,
}

var schema = []string{
	`CREATE TABLE IF NOT EXISTS File (
		ID INTEGER PRIMARY KEY AUTOINCREMENT,
		Node INTEGER NOT NULL,
		Repo STRING NOT NULL,
		Name STRING NOT NULL,
		Flags INTEGER NOT NULL,
		Modified INTEGER NOT NULL,
		Version INTEGER NOT NULL,
		Suppressed BOOLEAN NOT NULL,
		Deleted BOOLEAN NOT NULL,
		Updated BOOLEAN NOT NULL
	)`,
	`CREATE UNIQUE INDEX IF NOT EXISTS NodeRepoNameIdx ON File (Node, Repo, Name)`,
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
	{"selectFileID", "SELECT ID, Version FROM File WHERE Node==? AND Repo==? AND Name==?"},
	{"selectFileAll", "SELECT ID, Name, Flags, Modified, Version, Suppressed FROM File WHERE Node==? AND Repo==? AND Name==?"},
	{"deleteFile", "DELETE FROM File WHERE ID==?"},
	{"updateFile", "UPDATE File SET Updated=1 WHERE ID==?"},
	{"deleteBlock", "DELETE FROM Block WHERE FileID==?"},
	{"insertFile", "INSERT INTO File (Node, Repo, Name, Flags, Modified, Version, Suppressed, Deleted, Updated) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1)"},
	{"insertBlock", "INSERT INTO Block VALUES (?, ?, ?, ?)"},
	{"selectBlock", "SELECT Hash, Size, Offs FROM Block WHERE FileID==?"},
	{"selectFileHave", "SELECT ID, Name, Flags, Modified, Version, Suppressed FROM File WHERE Node==? AND Repo==?"},
	{"selectFileGlobal", "SELECT ID, Name, Flags, Modified, MAX(Version), Suppressed FROM File WHERE Repo==? GROUP BY Name ORDER BY Name"},
}

type fileDB struct {
	db    *sql.DB
	repo  string
	stmts map[string]*sql.Stmt
}

func newFileDB(repo, name string) (*fileDB, error) {
	db, err := sql.Open("sqlite3", "file:"+name+"?cache=shared&mode=rwc")
	if err != nil {
		return nil, err
	}

	for _, stmt := range setup {
		if debug {
			l.Debugln(repo, stmt)
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

	for _, stmt := range schema {
		if debug {
			l.Debugln(repo, stmt)
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
		repo:  repo,
		stmts: make(map[string]*sql.Stmt),
	}

	for _, prep := range preparedStmts {
		stmt, err := db.Prepare(prep[1])
		if debug {
			l.Debugln(repo, prep[1])
		}
		if err != nil {
			return nil, err
		}
		fdb.stmts[prep[0]] = stmt
	}

	return &fdb, nil
}

func (db *fileDB) update(cid uint, fs []scanner.File) error {
	tx, err := db.db.Begin()
	if err != nil {
		l.Fatalln(err)
	}

	db.updateTx(cid, fs, tx)

	return tx.Commit()
}

func (db *fileDB) updateTx(cid uint, fs []scanner.File, tx *sql.Tx) error {
	for _, f := range fs {
		var id int64
		var version uint64

		row := tx.Stmt(db.stmts["selectFileID"]).QueryRow(cid, db.repo, f.Name)
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
			rs, err := tx.Stmt(db.stmts["insertFile"]).Exec(cid, db.repo, f.Name, f.Flags, f.Modified, f.Version, f.Suppressed, protocol.IsDeleted(f.Flags))
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

func (db *fileDB) updateWithDelete(cid uint, fs []scanner.File) error {
	tx, err := db.db.Begin()
	if err != nil {
		l.Fatalln(err)
	}

	_, err = tx.Exec("UPDATE File SET Updated==0 WHERE Node==? AND Repo==?", cid, db.repo)
	if err != nil {
		l.Fatalln(err)
	}

	db.updateTx(cid, fs, tx)

	rows, err := tx.Query("SELECT ID, Flags FROM File WHERE Repo==? AND Node==? AND Updated==0", db.repo, cid)
	if err != nil && err != sql.ErrNoRows {
		l.Fatalln(err)
	}
	for rows.Next() {
		var id uint64
		var flags uint32
		err := rows.Scan(&id, &flags)
		if err != nil {
			l.Fatalln(err)
		}
		flags |= protocol.FlagDeleted
		_, err = tx.Exec("UPDATE File SET Flags=?, Version=? WHERE ID==?", flags, lamport.Default.Tick(0), id)
		if err != nil {
			l.Fatalln(err)
		}
	}

	return tx.Commit()
}

func (db *fileDB) replace(cid uint, fs []scanner.File) error {
	tx, err := db.db.Begin()
	if err != nil {
		l.Fatalln(err)
	}

	db.replaceTx(cid, fs, tx)

	return tx.Commit()
}

func (db *fileDB) replaceTx(cid uint, fs []scanner.File, tx *sql.Tx) error {
	_, err := tx.Exec("UPDATE File SET Updated==0 WHERE Node==? AND Repo==?", cid, db.repo)
	if err != nil {
		l.Fatalln(err)
	}

	for _, f := range fs {
		var id int64
		var version uint64

		row := tx.Stmt(db.stmts["selectFileID"]).QueryRow(cid, db.repo, f.Name)
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
			rs, err := tx.Stmt(db.stmts["insertFile"]).Exec(cid, db.repo, f.Name, f.Flags, f.Modified, f.Version, f.Suppressed, protocol.IsDeleted(f.Flags))
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

	_, err = tx.Exec("DELETE FROM File WHERE Repo==? AND Node==? AND Updated==0", db.repo, cid)
	if err != nil {
		l.Fatalln(err)
	}

	return nil
}

func (db *fileDB) have(cid uint) []scanner.File {
	rows, err := db.stmts["selectFileHave"].Query(cid, db.repo)
	if err == sql.ErrNoRows {
		return nil
	}
	if err != nil {
		l.Fatalln(err)
	}

	var files []scanner.File
	for rows.Next() {
		var f scanner.File
		var id int64
		rows.Scan(&id, &f.Name, &f.Flags, &f.Modified, &f.Version, &f.Suppressed)

		brows, err := db.stmts["selectBlock"].Query(id)
		if err != nil && err != sql.ErrNoRows {
			l.Fatalln(err)
		}

		for brows.Next() {
			var b scanner.Block
			brows.Scan(&b.Hash, &b.Size, &b.Offset)
			f.Blocks = append(f.Blocks, b)
		}

		files = append(files, f)
	}

	return files
}

func (db *fileDB) global() []scanner.File {
	rows, err := db.stmts["selectFileGlobal"].Query(db.repo)
	if err == sql.ErrNoRows {
		return nil
	}
	if err != nil {
		l.Fatalln(err)
	}

	var files []scanner.File
	for rows.Next() {
		var f scanner.File
		var id int64
		rows.Scan(&id, &f.Name, &f.Flags, &f.Modified, &f.Version, &f.Suppressed)

		brows, err := db.stmts["selectBlock"].Query(id)
		if err != nil && err != sql.ErrNoRows {
			l.Fatalln(err)
		}

		for brows.Next() {
			var b scanner.Block
			brows.Scan(&b.Hash, &b.Size, &b.Offset)
			f.Blocks = append(f.Blocks, b)
		}

		files = append(files, f)
	}

	return files
}

func (db *fileDB) get(cid uint, name string) scanner.File {
	var f scanner.File
	var id int64

	row := db.stmts["selectFileAll"].QueryRow(cid, db.repo, name)
	err := row.Scan(&id, &f.Name, &f.Flags, &f.Modified, &f.Version, &f.Suppressed)
	if err != nil && err != sql.ErrNoRows {
		l.Fatalln(err)
	}

	brows, err := db.stmts["selectBlock"].Query(id)
	if err != nil && err != sql.ErrNoRows {
		l.Fatalln(err)
	}

	for brows.Next() {
		var b scanner.Block
		brows.Scan(&b.Hash, &b.Size, &b.Offset)
		f.Blocks = append(f.Blocks, b)
	}

	return f
}
