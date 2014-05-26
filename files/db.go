package files

import (
	tiedot "github.com/HouzuoGuo/tiedot/db"
	"github.com/calmh/syncthing/scanner"
)

type filedb struct {
	db  *tiedot.DB
	col *tiedot.Col
}

func NewFiledb(db *tiedot.DB, repo string) *filedb {
	db.Create(repo, 4)
	col := db.Use(repo)
	col.Index([]string{"name"})
	col.Index([]string{"version"})
	return &filedb{
		db:  db,
		col: col,
	}
}

func (f *filedb) Set(r fileRecord) {
	doc := docFor(r)
	ex := f.find(r.File.Name, r.File.Version)
	if ex > 0 {
		f.col.Update(ex, doc)
	} else {
		f.col.Insert(doc)
	}
}

func (f *filedb) Get(k key) fileRecord {
	id := f.find(k.Name, k.Version)
	if id == 0 {
		return fileRecord{}
	}

	var doc map[string]interface{}
	id, err := f.col.Read(id, &doc)
	if err != nil {
		panic(err)
	}

	return fromDoc(doc)
}

func (f *filedb) find(name string, version uint64) uint64 {
	q := []interface{}{
		map[string]interface{}{
			"in": []interface{}{"name"},
			"eq": name,
		},
		map[string]interface{}{
			"in": []interface{}{"version"},
			"eq": version,
		},
	}
	res := make(map[uint64]struct{})
	err := tiedot.EvalQuery(q, f.col, &res)
	if err != nil {
		panic(err)
	}
	if len(res) > 1 {
		panic("uniqueness constraint violated")
	}
	for id := range res {
		return id
	}
	return 0
}

func docFor(f fileRecord) map[string]interface{} {
	m := make(map[string]interface{}, 9)
	m["name"] = f.File.Name
	m["flags"] = f.File.Flags
	m["modified"] = f.File.Modified
	m["version"] = f.File.Version
	m["size"] = f.File.Size
	m["suppressed"] = f.File.Suppressed
	m["global"] = f.Global
	m["usage"] = f.Usage
	return m
}

func fromDoc(m map[string]interface{}) fileRecord {
	var f fileRecord
	f.File.Name = m["name"].(string)
	f.File.Flags = m["flags"].(uint32)
	f.File.Modified = m["modified"].(int64)
	f.File.Version = m["version"].(uint64)
	f.File.Size = m["size"].(int64)
	f.File.Suppressed = m["suppressed"].(bool)
	f.Global = m["global"].(bool)
	f.Usage = m["usage"].(int)
	return f
}

func docKeyFor(f scanner.File) map[string]interface{} {
	m := make(map[string]interface{}, 2)
	m["name"] = f.Name
	m["version"] = f.Version
	return m
}
