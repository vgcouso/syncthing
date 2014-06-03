// Copyright (C) 2014 Jakob Borg and other contributors. All rights reserved.
// Use of this source code is governed by an MIT-style license that can be
// found in the LICENSE file.

package files

import (
	"database/sql"

	"github.com/calmh/syncthing/protocol"
	"github.com/calmh/syncthing/scanner"
	_ "github.com/mattn/go-sqlite3"
)

var setup = []string{
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

type fileDB struct {
	db   *sql.DB
	repo string

	selectFileID *sql.Stmt
	deleteFile   *sql.Stmt
	deleteBlock  *sql.Stmt
	insertFile   *sql.Stmt
	insertBlock  *sql.Stmt
}

func newFileDB(repo, name string) (*fileDB, error) {
	db, err := sql.Open("sqlite3", name)
	if err != nil {
		return nil, err
	}

	for _, stmt := range setup {
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
		_, err = tx.Exec(stmt)
		if err != nil {
			return nil, err
		}
	}

	if err = tx.Commit(); err != nil {
		return nil, err
	}

	fdb := fileDB{
		db:   db,
		repo: repo,
	}

	fdb.selectFileID, err = db.Prepare("SELECT rowid, Version FROM File WHERE Node==? AND Repo==? AND Name==?")
	if err != nil {
		return nil, err
	}

	fdb.deleteFile, err = db.Prepare("DELETE FROM File WHERE rowid==?")
	if err != nil {
		return nil, err
	}

	fdb.deleteBlock, err = db.Prepare("DELETE FROM Block WHERE FileID==?")
	if err != nil {
		return nil, err
	}

	fdb.insertFile, err = db.Prepare("INSERT INTO File (Node, Repo, Name, Flags, Modified, Version, Suppressed, Deleted, Updated) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1)")
	if err != nil {
		return nil, err
	}

	fdb.insertBlock, err = db.Prepare("INSERT INTO Block VALUES (?, ?, ?, ?)")
	if err != nil {
		return nil, err
	}

	return &fdb, nil
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

	for _, f := range fs {
		var id int64
		var version uint64

		row := tx.Stmt(db.selectFileID).QueryRow(cid, db.repo, f.Name)
		err := row.Scan(&id, &version)

		if err == nil && version != f.Version {
			_, err = tx.Stmt(db.deleteFile).Exec(id)
			if err != nil {
				l.Fatalln(err)
			}
		} else if err != nil && err != sql.ErrNoRows {
			l.Fatalln(err)
		}

		if version != f.Version {
			rs, err := tx.Stmt(db.insertFile).Exec(cid, db.repo, f.Name, f.Flags, f.Modified, f.Version, f.Suppressed, protocol.IsDeleted(f.Flags))
			if err != nil {
				l.Fatalln(err)
			}
			id, _ = rs.LastInsertId()

			for _, b := range f.Blocks {
				_, err = tx.Stmt(db.insertBlock).Exec(b.Hash, id, b.Size, b.Offset)
				if err != nil {
					l.Fatalln(err)
				}
			}
		}
	}

	rs, err := tx.Exec("DELETE FROM File WHERE Repo==? AND Node==? AND Updated==0", db.repo, cid)
	if err != nil {
		l.Fatalln(err)
	}

	aff, _ := rs.RowsAffected()
	l.Infoln("Deleted", aff)

	return tx.Commit()
}

func (db *fileDB) replace(cid uint, fs []scanner.File) error {
	tx, err := db.db.Begin()
	if err != nil {
		l.Fatalln(err)
	}

	_, err = tx.Exec("UPDATE File SET Updated==0 WHERE Node==? AND Repo==?", cid, db.repo)
	if err != nil {
		l.Fatalln(err)
	}

	for _, f := range fs {
		var id int64
		var version uint64

		row := tx.Stmt(db.selectFileID).QueryRow(cid, db.repo, f.Name)
		err := row.Scan(&id, &version)

		if err == nil && version != f.Version {
			_, err = tx.Stmt(db.deleteFile).Exec(id)
			if err != nil {
				l.Fatalln(err)
			}
		} else if err != nil && err != sql.ErrNoRows {
			l.Fatalln(err)
		}

		if version != f.Version {
			rs, err := tx.Stmt(db.insertFile).Exec(cid, db.repo, f.Name, f.Flags, f.Modified, f.Version, f.Suppressed, protocol.IsDeleted(f.Flags))
			if err != nil {
				l.Fatalln(err)
			}
			id, _ = rs.LastInsertId()

			for _, b := range f.Blocks {
				_, err = tx.Stmt(db.insertBlock).Exec(b.Hash, id, b.Size, b.Offset)
				if err != nil {
					l.Fatalln(err)
				}
			}
		}
	}

	rs, err := tx.Exec("DELETE FROM File WHERE Repo==? AND Node==? AND Updated==0", db.repo, cid)
	if err != nil {
		l.Fatalln(err)
	}

	aff, _ := rs.RowsAffected()
	l.Infoln("Deleted", aff)

	return tx.Commit()
}
