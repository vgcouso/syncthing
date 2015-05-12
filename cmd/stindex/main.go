// Copyright (C) 2014 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

package main

import (
	"encoding/binary"
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/syncthing/protocol"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

// copied from internal/db/leveldb.go
const (
	KeyTypeDevice = iota
	KeyTypeGlobal
	KeyTypeBlock
	KeyTypeDeviceStatistic
	KeyTypeFolderStatistic
)

func main() {
	log.SetFlags(0)
	log.SetOutput(os.Stdout)

	var verbose bool
	flag.BoolVar(&verbose, "v", false, "Verbose output")
	flag.Parse()

	ldb, err := leveldb.OpenFile(flag.Arg(0), &opt.Options{
		ErrorIfMissing:         true,
		Strict:                 opt.StrictAll,
		OpenFilesCacheCapacity: 100,
	})
	if err != nil {
		log.Fatal(err)
	}

	seenBlocks := make(map[string]struct{})
	seenFiles := make(map[string]int)

	files := 0
	globals := 0
	versions := 0
	blocks := 0
	errors := 0

	it := ldb.NewIterator(nil, nil)
	var dev protocol.DeviceID
	for it.Next() {
		key := it.Key()
		switch key[0] {
		case KeyTypeDevice:
			if files == 0 {
				fmt.Println("Checking files...")
			}
			files++

			folder := nulString(key[1 : 1+64])
			devBytes := key[1+64 : 1+64+32]
			name := nulString(key[1+64+32:])
			copy(dev[:], devBytes)

			seenFiles[dev.String()+"/"+folder+"/"+name] = 1

			var f protocol.FileInfo
			if err := f.UnmarshalXDR(it.Value()); err != nil {
				log.Fatal(err)
			}
			for _, b := range f.Blocks {
				seenBlocks[string(b.Hash)] = struct{}{}
			}

			if verbose {
				fmt.Printf("[device] F:%q N:%q D:%v\n", folder, name, dev)
				fmt.Printf("  N:%q\n  F:%#o\n  M:%d\n  V:%v\n  S:%d\n  B:%d\n", f.Name, f.Flags, f.Modified, f.Version, f.Size(), len(f.Blocks))
			}

		case KeyTypeGlobal:
			if globals == 0 {
				fmt.Println("Checking globals...")
			}
			globals++

			folder := nulString(key[1 : 1+64])
			name := nulString(key[1+64:])

			var vl versionList
			if err := vl.UnmarshalXDR(it.Value()); err != nil {
				log.Fatal(err)
			}

			if verbose {
				fmt.Printf("[global] F:%q N:%q V:\n", folder, name, len(vl.versions))
			}

			if len(vl.versions) == 0 {
				fmt.Printf("*** Zero length version list for %q / %q: %x\n", folder, name, it.Value())
				errors++
				continue
			}

			for _, v := range vl.versions {
				versions++

				dev := protocol.DeviceIDFromBytes(v.device)
				if verbose {
					fmt.Printf("  D:%v  V:%v", dev, v.version)
				}

				seenFiles[dev.String()+"/"+folder+"/"+name]++

				fk := deviceKey([]byte(folder), v.device, []byte(name))
				bs, err := ldb.Get(fk, nil)
				if err == leveldb.ErrNotFound {
					fmt.Printf("*** Missing file for global: %q / %q: %x\n", folder, name, fk)
					errors++
					continue
				}
				if err != nil {
					log.Fatal(err)
				}

				var f protocol.FileInfo
				if err := f.UnmarshalXDR(bs); err != nil {
					log.Fatal(err)
				}
				if f.Name != name {
					fmt.Printf("*** CORRUPTION: %q != %q\n", f.Name, name)
					errors++
				}

				if verbose {
					fmt.Println(" - OK")
				}
			}

		case KeyTypeBlock:
			if blocks == 0 {
				fmt.Println("Checking blocks...")
			}
			blocks++

			folder := nulString(key[1 : 1+64])
			hash := key[1+64 : 1+64+32]
			name := nulString(key[1+64+32:])
			if verbose {
				fmt.Printf("[block] F:%q H:%x N:%q I:%d\n", folder, hash, name, binary.BigEndian.Uint32(it.Value()))
			}
			if _, ok := seenBlocks[string(hash)]; !ok {
				fmt.Printf("*** Unnecessary block %x in block map (non fatal)\n", hash)
				errors++
			}

		case KeyTypeDeviceStatistic:
			if verbose {
				fmt.Printf("[dstat]\n  %x\n  %x\n", it.Key(), it.Value())
			}

		case KeyTypeFolderStatistic:
			if verbose {
				fmt.Printf("[fstat]\n  %x\n  %x\n", it.Key(), it.Value())
			}

		default:
			if verbose {
				fmt.Printf("[???]\n  %x\n  %x\n", it.Key(), it.Value())
			}
		}
	}

	fmt.Println("Checking version consistency...")
	for k, v := range seenFiles {
		if v < 2 {
			fmt.Println("*** %s not present in versions", k)
		}
	}

	fmt.Printf("Checked %d files, %d globals, %d versions, %d blocks\n", files, globals, versions, blocks)
	if errors > 0 {
		log.Fatal(errors, "errors found")
	}
}

func nulString(bs []byte) string {
	for i := range bs {
		if bs[i] == 0 {
			return string(bs[:i])
		}
	}
	return string(bs)
}

func deviceKey(folder, device, file []byte) []byte {
	k := make([]byte, 1+64+32+len(file))
	k[0] = KeyTypeDevice
	if len(folder) > 64 {
		panic("folder name too long")
	}
	copy(k[1:], []byte(folder))
	copy(k[1+64:], device[:])
	copy(k[1+64+32:], []byte(file))
	return k
}

type versionList struct {
	versions []fileVersion
}

type fileVersion struct {
	version protocol.Vector
	device  []byte
}
