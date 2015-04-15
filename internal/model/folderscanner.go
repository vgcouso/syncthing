package model

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/syncthing/protocol"
	"github.com/syncthing/syncthing/internal/db"
	"github.com/syncthing/syncthing/internal/events"
	"github.com/syncthing/syncthing/internal/ignore"
	"github.com/syncthing/syncthing/internal/osutil"
	"github.com/syncthing/syncthing/internal/scanner"
)

// The scanner scans a folder, or a subdirectory in a folder, and handles the diff
type folderScanner struct {
	folder  string
	path    string
	fs      *db.FileSet
	ignores *ignore.Matcher
	shortID uint64
}

func (s *folderScanner) scan(subs []string) error {
	// Subdirectories should use native directory separator and encoding
	for i := range subs {
		subs[i] = osutil.NativeFilename(subs[i])
	}

	// Subdirectories should not attempt to escape the root
	if err := s.validSubs(subs); err != nil {
		return err
	}

	subs = s.cleanSubs(subs)

	_ = s.ignores.Load(filepath.Join(s.path, ".stignore")) // Ignore error, there might not be an .stignore

	w := &scanner.Walker{
		Dir:           s.path,
		Subs:          subs,
		Matcher:       s.ignores,
		BlockSize:     protocol.BlockSize,
		TempNamer:     defTempNamer,
		TempLifetime:  24 * time.Hour, // time.Duration(m.cfg.Options().KeepTemporariesH) * time.Hour,
		CurrentFiler:  cFiler{s.fs},
		IgnorePerms:   false, // folderCfg.IgnorePerms,
		AutoNormalize: true,  // folderCfg.AutoNormalize,
		Hashers:       8,     // folderCfg.Hashers,
		ShortID:       s.shortID,
	}

	fchan, err := w.Walk()
	if err != nil {
		return err
	}

	batchSize := 100
	batch := make([]protocol.FileInfo, 0, batchSize)
	for f := range fchan {
		if len(batch) == batchSize {
			if err := m.CheckFolderHealth(s.folder); err != nil {
				l.Infof("Stopping folder %s mid-scan due to folder error: %s", s.folder, err)
				return err
			}
			s.fs.Update(protocol.LocalDeviceID, batch)
			events.Default.Log(events.LocalIndexUpdated, map[string]interface{}{
				"folder":   s.folder,
				"numFiles": len(batch),
			})
			batch = batch[:0]
		}
		batch = append(batch, f)
	}

	if err := m.CheckFolderHealth(s.folder); err != nil {
		l.Infof("Stopping folder %s mid-scan due to folder error: %s", s.folder, err)
		return err
	} else if len(batch) > 0 {
		s.fs.Update(protocol.LocalDeviceID, batch)
	}

	batch = batch[:0]
	// TODO: We should limit the Have scanning to start at sub
	seenPrefix := false
	s.fs.WithHaveTruncated(protocol.LocalDeviceID, func(fi db.FileIntf) bool {
		f := fi.(db.FileInfoTruncated)
		hasPrefix := len(subs) == 0
		for _, sub := range subs {
			if strings.HasPrefix(f.Name, sub) {
				hasPrefix = true
				break
			}
		}
		// Return true so that we keep iterating, until we get to the part
		// of the tree we are interested in. Then return false so we stop
		// iterating when we've passed the end of the subtree.
		if !hasPrefix {
			return !seenPrefix
		}

		seenPrefix = true
		if !f.IsDeleted() {
			if f.IsInvalid() {
				return true
			}

			if len(batch) == batchSize {
				s.fs.Update(protocol.LocalDeviceID, batch)
				batch = batch[:0]
			}

			if (s.ignores != nil && s.ignores.Match(f.Name)) || symlinkInvalid(f.IsSymlink()) {
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
					"folder":   s.folder,
					"name":     f.Name,
					"modified": time.Unix(f.Modified, 0),
					"flags":    fmt.Sprintf("0%o", f.Flags),
					"size":     f.Size(),
				})
				batch = append(batch, nf)
			} else if _, err := os.Lstat(filepath.Join(s.path, f.Name)); err != nil {
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
					Version:  f.Version.Update(s.shortID),
				}
				events.Default.Log(events.LocalIndexUpdated, map[string]interface{}{
					"folder":   s.folder,
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
		s.fs.Update(protocol.LocalDeviceID, batch)
	}

	return nil
}

// validSubs returns an error if any of the given subdirectories attempt to
// escape the given path.
func (s *folderScanner) validSubs(subs []string) error {
	for _, sub := range subs {
		if p := filepath.Clean(filepath.Join(s.path, sub)); !filepath.HasPrefix(p, s.path) {
			return fmt.Errorf("invalid subpath %q of %q", sub, s.path)
		}
	}

	return nil
}

func (s *folderScanner) cleanSubs(subs []string) []string {
	var cleaned []string
	for _, sub := range subs {
		// An absolute subpath is assumed to be rooted in the folder path, not
		// the actual filesystem root. So we join it with the folder path and
		// let filepath.Rel sort it out. So, a folder root "foo" with subpath
		// "/a/../b" becomes "foo/a/../b" then just "b" (relative to "foo").
		// If the subdir path is something stupidly absolute like "c:\foo" on
		// Windows this will fail and we skip the subpath.
		if filepath.IsAbs(sub) {
			full := filepath.Join(s.path, sub)
			rel, err := filepath.Rel(s.path, full)
			if err != nil {
				continue
			}
			sub = rel
		}

		// Reduce the subpath to the longer subpath we already know about in
		// the index.
		for sub != "." {
			if _, ok := s.fs.Get(protocol.LocalDeviceID, sub); ok {
				break
			}
			sub = filepath.Dir(sub)
		}

		cleaned = append(cleaned, sub)
	}

	// Remove any duplicates (a/b to a/b) or children (a/b/c to a/b).
	sort.Strings(cleaned)
	var reduced []string
nextSub:
	for _, sub := range cleaned {
		for _, seen := range reduced {
			if filepath.HasPrefix(sub, seen) {
				continue nextSub
			}
		}
		reduced = append(reduced, sub)
	}

	return reduced
}

type cFiler struct {
	fs *db.FileSet
}

// Implements scanner.CurrentFiler
func (cf cFiler) CurrentFile(file string) (protocol.FileInfo, bool) {
	return cf.fs.Get(protocol.LocalDeviceID, file)
}
