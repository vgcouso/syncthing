// Copyright (C) 2014 Jakob Borg and other contributors. All rights reserved.
// Use of this source code is governed by an MIT-style license that can be
// found in the LICENSE file.

package files

import (
	"database/sql"

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
		Suppressed INTEGER NOT NULL
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

	fdb.insertFile, err = db.Prepare("INSERT INTO File (Node, Repo, Name, Flags, Modified, Version, Suppressed) VALUES (?, ?, ?, ?, ?, ?, ?)")
	if err != nil {
		return nil, err
	}

	fdb.insertBlock, err = db.Prepare("INSERT INTO Block VALUES (?, ?, ?, ?)")
	if err != nil {
		return nil, err
	}

	return &fdb, nil
}

func (db *fileDB) updateFile(cid uint, f scanner.File) error {
	var id int64
	var version uint64
	var tx *sql.Tx

	row := db.selectFileID.QueryRow(cid, db.repo, f.Name)
	err := row.Scan(&id, &version)
	if err == nil && version != f.Version {
		tx, err = db.db.Begin()
		if err != nil {
			return err
		}

		_, err = tx.Stmt(db.deleteFile).Exec(id)
		if err != nil {
			return err
		}
		/*
			_, err = tx.Stmt(db.deleteBlock).Exec(id)
			if err != nil {
				return err
			}*/
	} else if err != sql.ErrNoRows {
		return err
	}

	if version != f.Version {
		if tx == nil {
			tx, err = db.db.Begin()
			if err != nil {
				return err
			}
		}

		rs, err := tx.Stmt(db.insertFile).Exec(cid, db.repo, f.Name, f.Flags, f.Modified, f.Version, f.Suppressed)
		if err != nil {
			return err
		}
		id, _ = rs.LastInsertId()

		for _, b := range f.Blocks {
			_, err = tx.Stmt(db.insertBlock).Exec(b.Hash, id, b.Size, b.Offset)
			if err != nil {
				return err
			}
		}
	}

	if tx != nil {
		return tx.Commit()
	}
	return nil
}
