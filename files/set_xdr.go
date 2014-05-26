package files

import (
	"bytes"
	"io"

	"github.com/calmh/syncthing/xdr"
)

func (o key) EncodeXDR(w io.Writer) (int, error) {
	var xw = xdr.NewWriter(w)
	return o.encodeXDR(xw)
}

func (o key) MarshalXDR() []byte {
	var buf bytes.Buffer
	var xw = xdr.NewWriter(&buf)
	o.encodeXDR(xw)
	return buf.Bytes()
}

func (o key) encodeXDR(xw *xdr.Writer) (int, error) {
	xw.WriteString(o.Name)
	xw.WriteUint64(o.Version)
	return xw.Tot(), xw.Error()
}

func (o *key) DecodeXDR(r io.Reader) error {
	xr := xdr.NewReader(r)
	return o.decodeXDR(xr)
}

func (o *key) UnmarshalXDR(bs []byte) error {
	var buf = bytes.NewBuffer(bs)
	var xr = xdr.NewReader(buf)
	return o.decodeXDR(xr)
}

func (o *key) decodeXDR(xr *xdr.Reader) error {
	o.Name = xr.ReadString()
	o.Version = xr.ReadUint64()
	return xr.Error()
}
