// Copyright (C) 2014 Jakob Borg && other contributors. All rights reserved.
// Use of this source code is governed by an MIT-style license that can be
// found in the LICENSE file.

package files

import (
	"database/sql"

	"github.com/calmh/syncthing/lamport"
	"github.com/calmh/syncthing/protocol"
	"github.com/calmh/syncthing/scanner"
	_ "github.com/cznic/ql/driver"
)

var schema = []string{
	`CREATE TABLE IF NOT EXISTS File (
		Node int,
		Repo string,
		Name string,
		Flags int,
		Modified int,
		Version int,
		Suppressed bool,
		Deleted bool,
		Updated bool,
	)`,
	`CREATE INDEX IF NOT EXISTS NodeIdx ON File (Node)`,
	`CREATE INDEX IF NOT EXISTS RepoIdx ON File (Repo)`,
	`CREATE INDEX IF NOT EXISTS NameIdx ON File (Name)`,
	`CREATE TABLE IF NOT EXISTS Block (
		Hash blob,
		FileID int,
		Size int,
		Offs int,
	)`,
	`CREATE INDEX IF NOT EXISTS HashIdx ON Block (Hash)`,
	`CREATE INDEX IF NOT EXISTS FileIDIdx ON Block (FileID)`,
}

var preparedStmts = [][2]string{
	{"selectFileID", "SELECT id(), Version FROM File WHERE Node==?1 && Repo==?2 && Name==?3"},
	{"selectFileIDFlags", "SELECT id(), Flags FROM File WHERE Repo==?1 && Node==?2 && Updated==false"},
	{"selectFileAll", "SELECT id(), Name, Flags, Modified, Version, Suppressed FROM File WHERE Node==?1 && Repo==?2 && Name==?3"},
	{"deleteFile", "DELETE FROM File WHERE id()==?1"},
	{"updateFile", "UPDATE File SET Updated=true WHERE id()==?1"},
	{"deleteBlock", "DELETE FROM Block WHERE FileID==?1"},
	{"insertFile", "INSERT INTO File (Node, Repo, Name, Flags, Modified, Version, Suppressed, Deleted, Updated) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, true)"},
	{"insertBlock", "INSERT INTO Block VALUES (?1, ?2, ?3, ?4)"},
	{"selectBlock", "SELECT Hash, Size, Offs FROM Block WHERE FileID==?1 ORDER BY Offs"},
	{"selectFileHave", "SELECT id(), Name, Flags, Modified, Version, Suppressed FROM File WHERE Node==?1 && Repo==?2"},
	{"selectFileGlobal", "SELECT id(), Name, Flags, Modified, max(Version), Suppressed FROM File WHERE Repo==?1 GROUP BY Name ORDER BY Name"},
	{"resetFileUpdated", "UPDATE File SET Updated=false WHERE Node==?1 && Repo==?2"},
	{"deleteNotUpdated", "DELETE FROM File WHERE Repo==?1 && Node==?2 && Updated==false"},
	{"updateFileFlags", "UPDATE File SET Flags=?1, Version=?2 WHERE id()==?3"},
}

type fileDB struct {
	db    *sql.DB
	repo  string
	stmts map[string]*sql.Stmt
}

func newFileDB(repo, name string) (*fileDB, error) {
	db, err := sql.Open("ql", name)
	if err != nil {
		return nil, err
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

	_, err = tx.Stmt(db.stmts["resetFileUpdated"]).Exec(cid, db.repo)
	if err != nil {
		l.Fatalln(err)
	}

	db.updateTx(cid, fs, tx)

	rows, err := tx.Stmt(db.stmts["selectFileIDFlags"]).Query(db.repo, cid)
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
		_, err = tx.Stmt(db.stmts["updateFileFlags"]).Exec(flags, lamport.Default.Tick(0), id)
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
	_, err := tx.Stmt(db.stmts["resetFileUpdated"]).Exec(cid, db.repo)
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

	_, err = tx.Stmt(db.stmts["deleteNotUpdated"]).Exec(db.repo, cid)
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
	/*	rows, err := db.stmts["selectFileGlobal"].Query(db.repo)
		if err == sql.ErrNoRows {
			return nil
		}
		if err != nil {
			l.Fatalln(err)
		}
	*/
	var files []scanner.File
	/*	for rows.Next() {
			var f scanner.File
			var id int64
			err = rows.Scan(&id, &f.Name, &f.Flags, &f.Modified, &f.Version, &f.Suppressed)
			if err != nil {
				l.Fatalln(err)
			}

			brows, err := db.stmts["selectBlock"].Query(id)
			if err != nil && err != sql.ErrNoRows {
				l.Fatalln(err)
			}

			for brows.Next() {
				var b scanner.Block
				err = brows.Scan(&b.Hash, &b.Size, &b.Offset)
				if err != nil {
					l.Fatalln(err)
				}
				f.Blocks = append(f.Blocks, b)
			}

			files = append(files, f)
		}
	*/
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
		err = brows.Scan(&b.Hash, &b.Size, &b.Offset)
		if err != nil {
			l.Fatalln(err)
		}
		f.Blocks = append(f.Blocks, b)
	}

	return f
}
