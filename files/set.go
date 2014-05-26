// Package files provides a set type to track local/remote files with newness checks.
package files

import (
	"sync"

	"github.com/calmh/syncthing/cid"
	"github.com/calmh/syncthing/lamport"
	"github.com/calmh/syncthing/protocol"
	"github.com/calmh/syncthing/scanner"
	"github.com/cznic/kv"
)

type fileRecord struct {
	File   scanner.File
	Usage  int
	Global bool
}

type bitset uint64

type Set struct {
	sync.Mutex
	files              *kv.DB
	remoteKey          [64]map[string]key
	changes            [64]uint64
	globalAvailability map[string]bitset
	globalKey          map[string]key
}

func NewSet() *Set {
	db, err := kv.CreateTemp("/tmp", "foo", "db", &kv.Options{})
	if err != nil {
		panic(err)
	}
	var m = Set{
		files:              db,
		globalAvailability: make(map[string]bitset),
		globalKey:          make(map[string]key),
	}
	return &m
}

func (m *Set) setFile(k key, r fileRecord) {
	kbs := k.MarshalXDR()
	rbs := r.File.MarshalXDR()
	var t [3]byte
	if r.Global {
		t[0] = 1
	} else {
		t[0] = 0
	}
	t[1] = byte(r.Usage >> 8)
	t[2] = byte(r.Usage)
	rbs = append(rbs, t[:]...)
	m.files.Set(kbs, rbs)
}

func (m *Set) getFile(k key) fileRecord {
	r, _ := m.getFileOK(k)
	return r
}

func (m *Set) getFileOK(k key) (fileRecord, bool) {
	kbs := k.MarshalXDR()
	rbs, err := m.files.Get(nil, kbs)
	if err != nil || len(rbs) == 0 {
		return fileRecord{}, false
	}
	return frBytes(rbs), true
}

func frBytes(rbs []byte) fileRecord {
	var f scanner.File
	f.UnmarshalXDR(rbs)
	isGlobal := rbs[len(rbs)-3] != 0
	usage := int(rbs[len(rbs)-2])<<8 + int(rbs[len(rbs)-1])
	return fileRecord{
		File:   f,
		Global: isGlobal,
		Usage:  usage,
	}
}

func (m *Set) forEachFile(fn func(key, fileRecord)) {
	it, err := m.files.SeekFirst()
	if err != nil {
		return
	}
	for {
		kbs, rbs, err := it.Next()
		if err != nil {
			break
		}
		var k key
		k.UnmarshalXDR(kbs)
		r := frBytes(rbs)
		fn(k, r)
	}
}

func (m *Set) deleteFile(k key) {
	kbs := k.MarshalXDR()
	m.files.Delete(kbs)
}

func (m *Set) Replace(id uint, fs []scanner.File) {
	if debug {
		l.Debugf("Replace(%d, [%d])", id, len(fs))
	}
	if id > 63 {
		panic("Connection ID must be in the range 0 - 63 inclusive")
	}

	m.Lock()
	if len(fs) == 0 || !m.equals(id, fs) {
		m.changes[id]++
		m.replace(id, fs)
	}
	m.Unlock()
}

func (m *Set) ReplaceWithDelete(id uint, fs []scanner.File) {
	if debug {
		l.Debugf("ReplaceWithDelete(%d, [%d])", id, len(fs))
	}
	if id > 63 {
		panic("Connection ID must be in the range 0 - 63 inclusive")
	}

	m.Lock()
	if len(fs) == 0 || !m.equals(id, fs) {
		m.changes[id]++

		var nf = make(map[string]key, len(fs))
		for _, f := range fs {
			nf[f.Name] = keyFor(f)
		}

		// For previously existing files not in the list, add them to the list
		// with the relevant delete flags etc set. Previously existing files
		// with the delete bit already set are not modified.

		for _, ck := range m.remoteKey[cid.LocalID] {
			if _, ok := nf[ck.Name]; !ok {
				cf := m.getFile(ck).File
				if !protocol.IsDeleted(cf.Flags) {
					cf.Flags |= protocol.FlagDeleted
					cf.Blocks = nil
					cf.Size = 0
					cf.Version = lamport.Default.Tick(cf.Version)
				}
				fs = append(fs, cf)
				if debug {
					l.Debugln("deleted:", ck.Name)
				}
			}
		}

		m.replace(id, fs)
	}
	m.Unlock()
}

func (m *Set) Update(id uint, fs []scanner.File) {
	if debug {
		l.Debugf("Update(%d, [%d])", id, len(fs))
	}
	m.Lock()
	m.update(id, fs)
	m.changes[id]++
	m.Unlock()
}

func (m *Set) Need(id uint) []scanner.File {
	if debug {
		l.Debugf("Need(%d)", id)
	}
	m.Lock()
	var fs = make([]scanner.File, 0, len(m.globalKey)/2) // Just a guess, but avoids too many reallocations
	rkID := m.remoteKey[id]
	m.forEachFile(func(gk key, gf fileRecord) {
		if !gf.Global || gf.File.Suppressed {
			return
		}
		if gk.newerThan(rkID[gk.Name]) {
			fs = append(fs, gf.File)
		}
	})
	m.Unlock()
	return fs
}

func (m *Set) Have(id uint) []scanner.File {
	if debug {
		l.Debugf("Have(%d)", id)
	}
	var fs = make([]scanner.File, 0, len(m.remoteKey[id]))
	m.Lock()
	for _, rk := range m.remoteKey[id] {
		fs = append(fs, m.getFile(rk).File)
	}
	m.Unlock()
	return fs
}

func (m *Set) Global() []scanner.File {
	if debug {
		l.Debugf("Global()")
	}
	m.Lock()
	var fs = make([]scanner.File, 0, len(m.globalKey))
	m.forEachFile(func(_ key, file fileRecord) {
		if file.Global {
			fs = append(fs, file.File)
		}
	})
	m.Unlock()
	return fs
}

func (m *Set) Get(id uint, file string) scanner.File {
	m.Lock()
	defer m.Unlock()
	if debug {
		l.Debugf("Get(%d, %q)", id, file)
	}
	return m.getFile(m.remoteKey[id][file]).File
}

func (m *Set) GetGlobal(file string) scanner.File {
	m.Lock()
	defer m.Unlock()
	if debug {
		l.Debugf("GetGlobal(%q)", file)
	}
	return m.getFile(m.globalKey[file]).File
}

func (m *Set) Availability(name string) bitset {
	m.Lock()
	defer m.Unlock()
	av := m.globalAvailability[name]
	if debug {
		l.Debugf("Availability(%q) = %0x", name, av)
	}
	return av
}

func (m *Set) Changes(id uint) uint64 {
	m.Lock()
	defer m.Unlock()
	if debug {
		l.Debugf("Changes(%d)", id)
	}
	return m.changes[id]
}

func (m *Set) equals(id uint, fs []scanner.File) bool {
	curWithoutDeleted := make(map[string]key)
	for _, k := range m.remoteKey[id] {
		f := m.getFile(k).File
		if !protocol.IsDeleted(f.Flags) {
			curWithoutDeleted[f.Name] = k
		}
	}
	if len(curWithoutDeleted) != len(fs) {
		return false
	}
	for _, f := range fs {
		if curWithoutDeleted[f.Name] != keyFor(f) {
			return false
		}
	}
	return true
}

func (m *Set) update(cid uint, fs []scanner.File) {
	remFiles := m.remoteKey[cid]
	if remFiles == nil {
		l.Fatalln("update before replace for cid", cid)
	}
	for _, f := range fs {
		n := f.Name
		fk := keyFor(f)

		if ck, ok := remFiles[n]; ok && ck == fk {
			// The remote already has exactly this file, skip it
			continue
		}

		remFiles[n] = fk

		// Keep the block list or increment the usage
		if br, ok := m.getFileOK(fk); !ok {
			m.setFile(fk, fileRecord{
				Usage: 1,
				File:  f,
			})
		} else {
			br.Usage++
			m.setFile(fk, br)
		}

		// Update global view
		gk, ok := m.globalKey[n]
		switch {
		case ok && fk == gk:
			av := m.globalAvailability[n]
			av |= 1 << cid
			m.globalAvailability[n] = av
		case fk.newerThan(gk):
			if ok {
				f := m.getFile(gk)
				f.Global = false
				m.setFile(gk, f)
			}
			f := m.getFile(fk)
			f.Global = true
			m.setFile(fk, f)
			m.globalKey[n] = fk
			m.globalAvailability[n] = 1 << cid
		}
	}
}

func (m *Set) replace(cid uint, fs []scanner.File) {
	// Decrement usage for all files belonging to this remote, and remove
	// those that are no longer needed.
	for _, fk := range m.remoteKey[cid] {
		br, ok := m.getFileOK(fk)
		switch {
		case ok && br.Usage == 1:
			m.deleteFile(fk)
		case ok && br.Usage > 1:
			br.Usage--
			m.setFile(fk, br)
		}
	}

	// Clear existing remote remoteKey
	m.remoteKey[cid] = make(map[string]key)

	// Recalculate global based on all remaining remoteKey
	for n := range m.globalKey {
		var nk key    // newest key
		var na bitset // newest availability

		for i, rem := range m.remoteKey {
			if rk, ok := rem[n]; ok {
				switch {
				case rk == nk:
					na |= 1 << uint(i)
				case rk.newerThan(nk):
					nk = rk
					na = 1 << uint(i)
				}
			}
		}

		if na != 0 {
			// Someone had the file
			m.globalKey[n] = nk
			m.globalAvailability[n] = na
		} else {
			// Noone had the file
			delete(m.globalKey, n)
			delete(m.globalAvailability, n)
		}
	}

	// Add new remote remoteKey to the mix
	m.update(cid, fs)
}
