// Copyright (C) 2015 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

package model

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/syncthing/protocol"
	"github.com/syncthing/syncthing/internal/config"
	"github.com/syncthing/syncthing/internal/db"
	"github.com/syncthing/syncthing/internal/events"
	"github.com/syncthing/syncthing/internal/lamport"
	"github.com/syncthing/syncthing/internal/osutil"
	"github.com/syncthing/syncthing/internal/scanner"
)

type folderScanner struct {
	folder  string
	fileSet *db.FileSet
	cfg     config.FolderConfiguration

	mut sync.Mutex
}

func (s *folderScanner) Scan() error {
	return s.ScanSubDir("")
}

func (s *folderScanner) ScanSubDir(sub string) error {
	sub = osutil.NativeFilename(sub)

	// Make sure that the supposed "subdirectory" is still inside the folder path.
	if p := filepath.Clean(filepath.Join(s.cfg.Path, sub)); !strings.HasPrefix(p, s.cfg.Path) {
		return errors.New("invalid subpath")
	}

	_ = ignores.Load(filepath.Join(s.cfg.Path, ".stignore")) // Ignore error, there might not be an .stignore

	// Required to make sure that we start indexing at a directory we're already
	// aware off.
	for sub != "" {
		if _, ok = fs.Get(protocol.LocalDeviceID, sub); ok {
			break
		}
		sub = filepath.Dir(sub)
		if sub == "." || sub == string(filepath.Separator) {
			sub = ""
		}
	}

	w := &scanner.Walker{
		Dir:          s.cfg.Path,
		Sub:          sub,
		Matcher:      ignores,
		BlockSize:    protocol.BlockSize,
		TempNamer:    defTempNamer,
		TempLifetime: time.Duration(m.cfg.Options().KeepTemporariesH) * time.Hour,
		CurrentFiler: s,
		IgnorePerms:  s.cfg.IgnorePerms,
		Hashers:      s.cfg.Hashers,
	}

	fchan, err := w.Walk()

	if err != nil {
		return err
	}
	batchSize := 100
	batch := make([]protocol.FileInfo, 0, batchSize)
	for f := range fchan {
		events.Default.Log(events.LocalIndexUpdated, map[string]interface{}{
			"folder":   folder,
			"name":     f.Name,
			"modified": time.Unix(f.Modified, 0),
			"flags":    fmt.Sprintf("0%o", f.Flags),
			"size":     f.Size(),
		})
		if len(batch) == batchSize {
			fs.Update(protocol.LocalDeviceID, batch)
			batch = batch[:0]
		}
		batch = append(batch, f)
	}
	if len(batch) > 0 {
		fs.Update(protocol.LocalDeviceID, batch)
	}

	batch = batch[:0]
	// TODO: We should limit the Have scanning to start at sub
	seenPrefix := false
	fs.WithHaveTruncated(protocol.LocalDeviceID, func(fi db.FileIntf) bool {
		f := fi.(db.FileInfoTruncated)
		if !strings.HasPrefix(f.Name, sub) {
			// Return true so that we keep iterating, until we get to the part
			// of the tree we are interested in. Then return false so we stop
			// iterating when we've passed the end of the subtree.
			return !seenPrefix
		}

		seenPrefix = true
		if !f.IsDeleted() {
			if f.IsInvalid() {
				return true
			}

			if len(batch) == batchSize {
				fs.Update(protocol.LocalDeviceID, batch)
				batch = batch[:0]
			}

			if (ignores != nil && ignores.Match(f.Name)) || symlinkInvalid(f.IsSymlink()) {
				// File has been ignored or an unsupported symlink. Set invalid bit.
				if debug {
					l.Debugln("setting invalid bit on ignored", f)
				}
				nf := protocol.FileInfo{
					Name:     f.Name,
					Flags:    f.Flags | protocol.FlagInvalid,
					Modified: f.Modified,
					Version:  f.Version, // The file is still the same, so don't bump version
				}
				events.Default.Log(events.LocalIndexUpdated, map[string]interface{}{
					"folder":   folder,
					"name":     f.Name,
					"modified": time.Unix(f.Modified, 0),
					"flags":    fmt.Sprintf("0%o", f.Flags),
					"size":     f.Size(),
				})
				batch = append(batch, nf)
			} else if _, err := os.Lstat(filepath.Join(s.cfg.Path, f.Name)); err != nil {
				// File has been deleted.

				// We don't specifically verify that the error is
				// os.IsNotExist because there is a corner case when a
				// directory is suddenly transformed into a file. When that
				// happens, files that were in the directory (that is now a
				// file) are deleted but will return a confusing error ("not a
				// directory") when we try to Lstat() them.

				nf := protocol.FileInfo{
					Name:     f.Name,
					Flags:    f.Flags | protocol.FlagDeleted,
					Modified: f.Modified,
					Version:  lamport.Default.Tick(f.Version),
				}
				events.Default.Log(events.LocalIndexUpdated, map[string]interface{}{
					"folder":   folder,
					"name":     f.Name,
					"modified": time.Unix(f.Modified, 0),
					"flags":    fmt.Sprintf("0%o", f.Flags),
					"size":     f.Size(),
				})
				batch = append(batch, nf)
			}
		}
		return true
	})
	if len(batch) > 0 {
		fs.Update(protocol.LocalDeviceID, batch)
	}

	return nil
}
