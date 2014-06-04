package files

import (
	"bytes"

	"github.com/calmh/syncthing/scanner"
	"github.com/calmh/syncthing/xdr"
	"github.com/cznic/kv"
)

type kvdb struct {
	db   *kv.DB
	repo string
}

type kvkey struct {
	node    string
	repo    string
	file    string
	version uint64
}

func keyBytes(k kvkey) []byte {
	var kb bytes.Buffer
	xw := xdr.NewWriter(&kb)

	xw.WriteString(k.repo)
	xw.WriteString(k.file)
	xw.WriteUint64(k.version)
	return kb.Bytes()
}

func bytesKey(bs []byte) kvkey {
	kb := bytes.NewBuffer(bs)
	xr := xdr.NewReader(kb)

	var k kvkey
	k.repo = xr.ReadString()
	k.file = xr.ReadString()
	k.version = xr.ReadUint64()
	return k
}

func (d *kvdb) set(k kvkey, f scanner.File) {
	kbs := keyBytes(k)
	rbs := f.MarshalXDR()
	d.db.Set(kbs, rbs)
}

func (d *kvdb) get(k kvkey) scanner.File {
	f, _ := d.getOK(k)
	return f
}

func (d *kvdb) getOK(k kvkey) (scanner.File, bool) {
	var f scanner.File
	kbs := keyBytes(k)
	rbs, err := d.db.Get(nil, kbs)
	if err != nil || len(rbs) == 0 {
		return f, false
	}

	f.UnmarshalXDR(rbs)
	return f, true
}

func (d *kvdb) forEach(fn func(kvkey, scanner.File)) {
	it, _, err := d.db.Seek(nil)
	if err != nil {
		return
	}
	for {
		kbs, rbs, err := it.Next()
		if err != nil {
			break
		}

		k := bytesKey(kbs)
		var f scanner.File
		f.UnmarshalXDR(rbs)

		fn(k, f)
	}
}

func (d *kvdb) delete(k kvkey) {
	kbs := keyBytes(k)
	d.db.Delete(kbs)
}
