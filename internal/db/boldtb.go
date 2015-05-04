// Copyright (C) 2014 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

//go:generate -command genxdr go run ../../Godeps/_workspace/src/github.com/calmh/xdr/cmd/genxdr/main.go
//go:generate genxdr -o leveldb_xdr.go leveldb.go

package db

import (
	"bytes"
	"runtime"
	"sort"

	"github.com/boltdb/bolt"
	"github.com/syncthing/protocol"
)

type boltDeletionHandler func(folBuc, devBuc *bolt.Bucket, device, name []byte) int64

type boltDB struct {
	*bolt.DB
}

/*
	"folders":
		<folder>:
			<device>:
				<file>: protocol.FileInfo
			"global":
				<file>: versionList
*/

var (
	globalBucketID = []byte("global")
	folderBucketID = []byte("folders")
)

func folderBucket(tx *bolt.Tx, folder []byte) *bolt.Bucket {
	return tx.Bucket(folderBucketID).Bucket(folder)
}

func folderDeviceBucket(tx *bolt.Tx, folder, device []byte) *bolt.Bucket {
	bkt, err := tx.Bucket(folderBucketID).Bucket(folder).CreateBucketIfNotExists(device)
	if err != nil {
		panic(err)
	}
	return bkt
}

func (db *boltDB) init(folder []byte) {
	db.Update(func(tx *bolt.Tx) error {
		bkt, err := tx.CreateBucketIfNotExists(folderBucketID)
		if err != nil {
			panic(err)
		}
		_, err = bkt.CreateBucketIfNotExists(folder)
		if err != nil {
			panic(err)
		}
		return nil
	})
}

func (db *boltDB) genericReplace(folder, device []byte, fs []protocol.FileInfo, deleteFn boltDeletionHandler) int64 {
	runtime.GC()

	sort.Sort(fileList(fs)) // sort list on name, same as in the database

	var maxLocalVer int64
	db.Update(func(tx *bolt.Tx) error {
		folBuc := folderBucket(tx, folder)
		devBuc := folderDeviceBucket(tx, folder, device)

		it := devBuc.Cursor()
		k, v := it.First()

		fsi := 0

		for {
			var newName, oldName []byte
			moreDb := k != nil
			moreFs := fsi < len(fs)

			if !moreDb && !moreFs {
				break
			}

			if moreFs {
				newName = []byte(fs[fsi].Name)
			}

			if moreDb {
				oldName = k
			}

			cmp := bytes.Compare(newName, oldName)

			if debugDB {
				l.Debugf("generic replace; folder=%q device=%v moreFs=%v moreDb=%v cmp=%d newName=%q oldName=%q", folder, protocol.DeviceIDFromBytes(device), moreFs, moreDb, cmp, newName, oldName)
			}

			switch {
			case moreFs && (!moreDb || cmp == -1):
				if debugDB {
					l.Debugln("generic replace; missing - insert")
				}
				// Database is missing this file. Insert it.
				if lv := db.insert(devBuc, fs[fsi]); lv > maxLocalVer {
					maxLocalVer = lv
				}
				if fs[fsi].IsInvalid() {
					db.removeFromGlobal(folBuc, device, newName)
				} else {
					db.updateGlobal(folBuc, device, newName, fs[fsi].Version)
				}
				fsi++

			case moreFs && moreDb && cmp == 0:
				// File exists on both sides - compare versions. We might get an
				// update with the same version and different flags if a device has
				// marked a file as invalid, so handle that too.
				if debugDB {
					l.Debugln("generic replace; exists - compare")
				}
				var ef FileInfoTruncated
				ef.UnmarshalXDR(v)
				if !fs[fsi].Version.Equal(ef.Version) || fs[fsi].Flags != ef.Flags {
					if debugDB {
						l.Debugln("generic replace; differs - insert")
					}
					if lv := db.insert(devBuc, fs[fsi]); lv > maxLocalVer {
						maxLocalVer = lv
					}
					if fs[fsi].IsInvalid() {
						db.removeFromGlobal(folBuc, device, newName)
					} else {
						db.updateGlobal(folBuc, device, newName, fs[fsi].Version)
					}
				} else if debugDB {
					l.Debugln("generic replace; equal - ignore")
				}

				fsi++
				k, v = it.Next()

			case moreDb && (!moreFs || cmp == 1):
				if debugDB {
					l.Debugln("generic replace; exists - remove")
				}
				if lv := deleteFn(folBuc, devBuc, device, oldName); lv > maxLocalVer {
					maxLocalVer = lv
				}
				k, v = it.Next()
			}
		}

		return nil
	})

	return maxLocalVer
}

func (db *boltDB) replace(folder, device []byte, fs []protocol.FileInfo) int64 {
	// TODO: Return the remaining maxLocalVer?
	return db.genericReplace(folder, device, fs, func(folBuc, devBuc *bolt.Bucket, device, name []byte) int64 {
		// Database has a file that we are missing. Remove it.
		db.removeFromGlobal(folBuc, device, name)
		devBuc.Delete(name)
		return 0
	})
}

func (db *boltDB) replaceWithDelete(folder, device []byte, fs []protocol.FileInfo, myID uint64) int64 {
	return db.genericReplace(folder, device, fs, func(folBuc, devBuc *bolt.Bucket, device, name []byte) int64 {
		var tf FileInfoTruncated
		err := tf.UnmarshalXDR(devBuc.Get(name))
		if err != nil {
			panic(err)
		}
		if !tf.IsDeleted() {
			ts := clock(tf.LocalVersion)
			f := protocol.FileInfo{
				Name:         tf.Name,
				Version:      tf.Version.Update(myID),
				LocalVersion: ts,
				Flags:        tf.Flags | protocol.FlagDeleted,
				Modified:     tf.Modified,
			}
			devBuc.Put(name, f.MustMarshalXDR())
			db.updateGlobal(folBuc, device, name, f.Version)
			return ts
		}
		return 0
	})
}

func (db *boltDB) update(folder, device []byte, fs []protocol.FileInfo) int64 {
	runtime.GC()

	var maxLocalVer int64
	db.Update(func(tx *bolt.Tx) error {
		folBuc := folderBucket(tx, folder)
		devBuc := folderDeviceBucket(tx, folder, device)
		for _, f := range fs {
			name := []byte(f.Name)
			bs := devBuc.Get(name)
			if bs == nil {
				if lv := db.insert(devBuc, f); lv > maxLocalVer {
					maxLocalVer = lv
				}
				if f.IsInvalid() {
					db.removeFromGlobal(folBuc, device, name)
				} else {
					db.updateGlobal(folBuc, device, name, f.Version)
				}
				continue
			}

			var ef FileInfoTruncated
			if err := ef.UnmarshalXDR(bs); err != nil {
				panic(err)
			}
			// Flags might change without the version being bumped when we set the
			// invalid flag on an existing file.
			if !ef.Version.Equal(f.Version) || ef.Flags != f.Flags {
				if lv := db.insert(devBuc, f); lv > maxLocalVer {
					maxLocalVer = lv
				}
				if f.IsInvalid() {
					db.removeFromGlobal(folBuc, device, name)
				} else {
					db.updateGlobal(folBuc, device, name, f.Version)
				}
			}
		}

		return nil
	})

	return maxLocalVer
}

func (db *boltDB) insert(devBuc *bolt.Bucket, file protocol.FileInfo) int64 {
	if file.LocalVersion == 0 {
		file.LocalVersion = clock(0)
	}

	devBuc.Put([]byte(file.Name), file.MustMarshalXDR())
	return file.LocalVersion
}

// ldbUpdateGlobal adds this device+version to the version list for the given
// file. If the device is already present in the list, the version is updated.
// If the file does not have an entry in the global list, it is created.
func (db *boltDB) updateGlobal(folBuc *bolt.Bucket, device, file []byte, version protocol.Vector) bool {
	gloBuc, err := folBuc.CreateBucketIfNotExists(globalBucketID)
	if err != nil {
		panic(err)
	}
	svl := gloBuc.Get(file)

	var fl versionList

	// Remove the device from the current version list
	if svl != nil {
		err := fl.UnmarshalXDR(svl)
		if err != nil {
			panic(err)
		}

		for i := range fl.versions {
			if bytes.Compare(fl.versions[i].device, device) == 0 {
				if fl.versions[i].version.Equal(version) {
					// No need to do anything
					return false
				}
				fl.versions = append(fl.versions[:i], fl.versions[i+1:]...)
				break
			}
		}
	}

	nv := fileVersion{
		device:  device,
		version: version,
	}
	for i := range fl.versions {
		// We compare  against ConcurrentLesser as well here because we need
		// to enforce a consistent ordering of versions even in the case of
		// conflicts.
		if comp := fl.versions[i].version.Compare(version); comp == protocol.Equal || comp == protocol.Lesser || comp == protocol.ConcurrentLesser {
			t := append(fl.versions, fileVersion{})
			copy(t[i+1:], t[i:])
			t[i] = nv
			fl.versions = t
			goto done
		}
	}

	fl.versions = append(fl.versions, nv)

done:
	gloBuc.Put(file, fl.MustMarshalXDR())

	return true
}

// ldbRemoveFromGlobal removes the device from the global version list for the
// given file. If the version list is empty after this, the file entry is
// removed entirely.
func (db *boltDB) removeFromGlobal(folBuc *bolt.Bucket, device, file []byte) {
	gloBuc, err := folBuc.CreateBucketIfNotExists(globalBucketID)
	if err != nil {
		panic(err)
	}
	svl := gloBuc.Get(file)
	if svl == nil {
		// We might be called to "remove" a global version that doesn't exist
		// if the first update for the file is already marked invalid.
		return
	}

	var fl versionList
	err = fl.UnmarshalXDR(svl)
	if err != nil {
		panic(err)
	}

	for i := range fl.versions {
		if bytes.Compare(fl.versions[i].device, device) == 0 {
			fl.versions = append(fl.versions[:i], fl.versions[i+1:]...)
			break
		}
	}

	if len(fl.versions) == 0 {
		gloBuc.Delete(file)
	} else {
		gloBuc.Put(file, fl.MustMarshalXDR())
	}
}

func (db *boltDB) withHave(folder, device []byte, truncate bool, fn Iterator) {
	db.View(func(tx *bolt.Tx) error {
		devBuc := folderBucket(tx, folder).Bucket(device)
		if devBuc == nil {
			return nil
		}

		c := devBuc.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			f, err := unmarshalTrunc(v, truncate)
			if err != nil {
				panic(err)
			}
			if cont := fn(f); !cont {
				return nil
			}
		}
		return nil
	})
}

func (db *boltDB) withAllFolderTruncated(folder []byte, fn func(device []byte, f FileInfoTruncated) bool) {
	runtime.GC()

	db.View(func(tx *bolt.Tx) error {
		folBuc := folderBucket(tx, folder)
		if folBuc == nil {
			return nil
		}

		fc := folBuc.Cursor()
		for fk, fv := fc.First(); fk != nil; fk, fv = fc.Next() {
			if bytes.Compare(fk, globalBucketID) == 0 {
				continue
			}

			if fv != nil {
				// This is a top level value directly under a folder. Should
				// not happen.
				l.Debugf("%x", fk)
				l.Debugf("%x", fv)
				panic("top level value?")
			}

			devBuc := folBuc.Bucket(fk)
			dc := devBuc.Cursor()
			for dk, dv := dc.First(); dk != nil; dk, dv = dc.Next() {

				var f FileInfoTruncated
				err := f.UnmarshalXDR(dv)
				if err != nil {
					panic(err)
				}

				if cont := fn(fk, f); !cont {
					return nil
				}
			}
		}

		return nil
	})
}

func (db *boltDB) get(folder, device, file []byte) (fi protocol.FileInfo, ok bool) {
	db.View(func(tx *bolt.Tx) error {
		folBuc := folderBucket(tx, folder)
		if folBuc == nil {
			return nil
		}
		devBuc := folBuc.Bucket(device)
		if devBuc == nil {
			return nil
		}
		bs := devBuc.Get(file)
		if bs == nil {
			return nil
		}

		if err := fi.UnmarshalXDR(bs); err != nil {
			panic(err)
		}

		ok = true
		return nil
	})

	return
}

func (db *boltDB) getGlobal(folder, file []byte, truncate bool) (fi FileIntf, ok bool) {
	db.View(func(tx *bolt.Tx) error {
		folBuc := folderBucket(tx, folder)
		if folBuc == nil {
			return nil
		}
		gloBuc := folBuc.Bucket(globalBucketID)
		if gloBuc == nil {
			return nil
		}

		bs := gloBuc.Get(file)
		if bs == nil {
			return nil
		}

		var vl versionList
		err := vl.UnmarshalXDR(bs)
		if err != nil {
			panic(err)
		}
		if len(vl.versions) == 0 {
			l.Debugf("%x", folder)
			l.Debugf("%x", file)
			panic("no versions?")
		}

		// nil pointer exception here if the db is corrupt
		bs = folBuc.Bucket(vl.versions[0].device).Get(file)

		fi, err = unmarshalTrunc(bs, truncate)
		if err != nil {
			panic(err)
		}

		ok = true
		return nil
	})

	return
}

func (db *boltDB) withGlobal(folder, prefix []byte, truncate bool, fn Iterator) {
	runtime.GC()

	db.View(func(tx *bolt.Tx) error {
		folBuc := folderBucket(tx, folder)
		if folBuc == nil {
			return nil
		}
		gloBuc := folBuc.Bucket(globalBucketID)
		if gloBuc == nil {
			return nil
		}

		c := gloBuc.Cursor()
		for name, v := c.First(); name != nil; name, v = c.Next() {
			var vl versionList
			err := vl.UnmarshalXDR(v)
			if err != nil {
				panic(err)
			}
			if len(vl.versions) == 0 {
				l.Debugf("%x", name)
				l.Debugf("%x", v)
				panic("no versions?")
			}

			bs := folBuc.Bucket(vl.versions[0].device).Get(name)
			if bs == nil {
				l.Debugf("folder: %q (%x)", folder, folder)
				l.Debugf("name: %q (%x)", name, name)
				l.Debugf("vl: %v", vl)
				l.Debugf("vl.versions[0].device: %x", vl.versions[0].device)
				panic("global file missing")
			}

			f, err := unmarshalTrunc(bs, truncate)
			if err != nil {
				panic(err)
			}

			if cont := fn(f); !cont {
				return nil
			}
		}

		return nil
	})
}

func (db *boltDB) availability(folder, file []byte) (devs []protocol.DeviceID) {
	db.View(func(tx *bolt.Tx) error {
		folBuc := folderBucket(tx, folder)
		if folBuc == nil {
			return nil
		}
		gloBuc := folBuc.Bucket(globalBucketID)
		if gloBuc == nil {
			return nil
		}

		bs := gloBuc.Get(file)
		if bs == nil {
			return nil
		}

		var vl versionList
		if err := vl.UnmarshalXDR(bs); err != nil {
			panic(err)
		}

		for _, v := range vl.versions {
			if !v.version.Equal(vl.versions[0].version) {
				break
			}
			n := protocol.DeviceIDFromBytes(v.device)
			devs = append(devs, n)
		}

		return nil
	})

	return
}

func (db *boltDB) withNeed(folder, device []byte, truncate bool, fn Iterator) {
	runtime.GC()

	db.View(func(tx *bolt.Tx) error {
		folBuc := folderBucket(tx, folder)
		if folBuc == nil {
			return nil
		}
		gloBuc := folBuc.Bucket(globalBucketID)
		if gloBuc == nil {
			return nil
		}

		c := gloBuc.Cursor()

	nextFile:
		for file, v := c.First(); file != nil; file, v = c.Next() {
			var vl versionList
			err := vl.UnmarshalXDR(v)
			if err != nil {
				l.Debugf("%x", file)
				l.Debugf("%x", v)
				panic(err)
			}
			if len(vl.versions) == 0 {
				l.Debugf("%x", file)
				l.Debugf("%x", v)
				panic("no versions?")
			}

			have := false // If we have the file, any version
			need := false // If we have a lower version of the file
			for _, v := range vl.versions {
				if bytes.Compare(v.device, device) == 0 {
					have = true
					// XXX: This marks Concurrent (i.e. conflicting) changes as
					// needs. Maybe we should do that, but it needs special
					// handling in the puller.
					need = !v.version.GreaterEqual(vl.versions[0].version)
					break
				}
			}

			if need || !have {
				needVersion := vl.versions[0].version

			nextVersion:
				for i := range vl.versions {
					if !vl.versions[i].version.Equal(needVersion) {
						// We haven't found a valid copy of the file with the needed version.
						continue nextFile
					}

					bs := folBuc.Bucket(vl.versions[i].device).Get(file)
					if bs == nil {
						var id protocol.DeviceID
						copy(id[:], device)
						l.Debugf("device: %v", id)
						l.Debugf("need: %v, have: %v", need, have)
						l.Debugf("vl: %v", vl)
						l.Debugf("i: %v", i)
						l.Debugf("file: %q (%x)", file, file)
						panic("not found")
					}

					gf, err := unmarshalTrunc(bs, truncate)
					if err != nil {
						panic(err)
					}

					if gf.IsInvalid() {
						// The file is marked invalid for whatever reason, don't use it.
						continue nextVersion
					}

					if gf.IsDeleted() && !have {
						// We don't need deleted files that we don't have
						continue nextFile
					}

					if cont := fn(gf); !cont {
						return nil
					}

					// This file is handled, no need to look further in the version list
					continue nextFile
				}
			}
		}
		return nil
	})
}

func (db *boltDB) listFolders() []string {
	runtime.GC()

	folderExists := make(map[string]bool)
	db.View(func(tx *bolt.Tx) error {
		bkt := tx.Bucket(folderBucketID)
		if bkt == nil {
			return nil
		}
		c := bkt.Cursor()
		for folder, v := c.First(); folder != nil; folder, v = c.Next() {
			if v != nil {
				l.Debugf("%x", folder)
				l.Debugf("%x", v)
				panic("top level value")
			}

			fStr := string(folder)
			if !folderExists[fStr] {
				folderExists[fStr] = true
			}
		}
		return nil
	})

	folders := make([]string, 0, len(folderExists))
	for k := range folderExists {
		folders = append(folders, k)
	}

	sort.Strings(folders)
	return folders
}

func (db *boltDB) dropFolder(folder []byte) {
	db.Update(func(tx *bolt.Tx) error {
		if err := tx.Bucket(folderBucketID).DeleteBucket(folder); err != nil {
			panic(err)
		}
		return nil
	})
}
