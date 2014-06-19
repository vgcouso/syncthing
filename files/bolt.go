package files

import (
	"errors"

	"github.com/boltdb/bolt"
	"github.com/calmh/syncthing/lamport"
	"github.com/calmh/syncthing/protocol"
	"github.com/calmh/syncthing/scanner"
)

/*
Bolt DB structure:

- files
	- <repo>
 		- <node id>
 			* name -> scanner.File
- global
 	- repo
		* name -> version

*/

func boltReplace(id uint, repo string, fs []scanner.File) func(tx *bolt.Tx) error {
	return func(tx *bolt.Tx) error {
		bkt, err := tx.CreateBucketIfNotExists([]byte("files"))
		if err != nil {
			return err
		}

		bkt, err = bkt.CreateBucketIfNotExists([]byte(repo))
		if err != nil {
			return err
		}

		bktName := []byte{byte(id)}
		bkt.DeleteBucket(bktName)
		bkt, err = bkt.CreateBucket(bktName)
		if err != nil {
			return err
		}

		err = boltUpdateBucket(bkt, nil, fs)
		if err != nil {
			return err
		}

		return boltRebuildGlobal(repo)(tx)
	}
}

func boltUpdate(id uint, repo string, fs []scanner.File) func(tx *bolt.Tx) error {
	return func(tx *bolt.Tx) error {
		bkt, err := tx.CreateBucketIfNotExists([]byte("files"))
		if err != nil {
			return err
		}

		bkt, err = bkt.CreateBucketIfNotExists([]byte(repo))
		if err != nil {
			return err
		}

		bkt, err = bkt.CreateBucketIfNotExists([]byte{byte(id)})
		if err != nil {
			return err
		}

		gbkt, err := tx.CreateBucketIfNotExists([]byte("global"))
		if err != nil {
			return err
		}

		gbkt, err = gbkt.CreateBucketIfNotExists([]byte(repo))
		if err != nil {
			return err
		}

		return boltUpdateBucket(bkt, gbkt, fs)
	}
}

func boltReplaceWithDelete(id uint, repo string, fs []scanner.File) func(tx *bolt.Tx) error {
	return func(tx *bolt.Tx) error {
		bkt, err := tx.CreateBucketIfNotExists([]byte("files"))
		if err != nil {
			return err
		}

		bkt, err = bkt.CreateBucketIfNotExists([]byte(repo))
		if err != nil {
			return err
		}

		bkt, err = bkt.CreateBucketIfNotExists([]byte{byte(id)})
		if err != nil {
			return err
		}

		err = boltUpdateBucket(bkt, nil, fs)
		if err != nil {
			return err
		}

		var nm = make(map[string]bool, len(fs))
		for _, f := range fs {
			nm[f.Name] = true
		}

		c := bkt.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			var f scanner.File
			f.UnmarshalXDR(v)
			if !protocol.IsDeleted(f.Flags) && !nm[f.Name] {
				f.Version = lamport.Default.Tick(f.Version)
				f.Flags |= protocol.FlagDeleted
				f.Blocks = nil
				f.Size = 0
				err = bkt.Put(k, f.MarshalXDR())
				if err != nil {
					return err
				}
			}
		}

		return boltRebuildGlobal(repo)(tx)
	}
}

func boltUpdateBucket(bkt *bolt.Bucket, gbkt *bolt.Bucket, fs []scanner.File) error {
	var gf scanner.File

	for _, f := range fs {
		key := []byte(f.Name)
		bs := f.MarshalXDR()
		err := bkt.Put(key, bs)
		if err != nil {
			return err
		}

		if gbkt != nil {
			gv := gbkt.Get(key)
			if gv == nil {
				err := gbkt.Put(key, bs)
				if err != nil {
					return err
				}
			} else {
				gf.UnmarshalXDR(gv)
				if f.NewerThan(gf) {
					err := gbkt.Put(key, bs)
					if err != nil {
						return err
					}
				}
			}
		}
	}
	return nil
}

func boltRebuildGlobal(repo string) func(tx *bolt.Tx) error {
	return func(tx *bolt.Tx) error {
		brepo := []byte(repo)

		bkt := tx.Bucket([]byte("files"))
		if bkt == nil {
			return errors.New("no root bucket")
		}

		bkt = bkt.Bucket(brepo)
		if bkt == nil {
			return errors.New("no repo bucket")
		}

		gbkt, err := tx.CreateBucketIfNotExists([]byte("global"))
		if err != nil {
			return err
		}

		gbkt.DeleteBucket(brepo)
		gbkt, err = gbkt.CreateBucket([]byte(repo))
		if err != nil {
			return err
		}

		c := bkt.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			if v != nil {
				panic("non-bucket key under repo")
			}

			c1 := bkt.Bucket(k).Cursor()
			for k, v := c1.First(); k != nil; k, v = c1.Next() {
				if v == nil {
					panic("unexpected sub-bucket under node")
				}

				g := gbkt.Get(k)
				if g == nil {
					// file does not exist in global
					err = gbkt.Put(k, v)
					if err != nil {
						return err
					}
					continue
				}

				var lf scanner.File
				lf.UnmarshalXDR(v)
				var gf scanner.File
				gf.UnmarshalXDR(g)
				if lf.NewerThan(gf) {
					err = gbkt.Put(k, v)
					if err != nil {
						return err
					}
				}
			}
		}
		return nil
	}
}

func boltBucketFiles(bkt *bolt.Bucket) []scanner.File {
	bs := bkt.Stats()
	fs := make([]scanner.File, bs.KeyN)
	c := bkt.Cursor()
	i := 0
	for k, v := c.First(); k != nil; k, v = c.Next() {
		err := fs[i].UnmarshalXDR(v)
		l.FatalErr(err)
		i++
	}
	return fs
}
