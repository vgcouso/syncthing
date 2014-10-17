package files_test

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/syncthing/syncthing/internal/files"
	"github.com/syncthing/syncthing/internal/protocol"
	"github.com/syndtr/goleveldb/leveldb"
)

func setupFiles(nItems int) []protocol.FileInfo {
	fs := make([]protocol.FileInfo, 0, nItems)
	for i := 0; i < nItems; i++ {
		f := protocol.FileInfo{
			Name:    fmt.Sprintf("file%d", rand.Int()),
			Version: uint64(rand.Int63()),
		}
		fs = append(fs, f)
	}
	return fs
}

func setFiles(id protocol.DeviceID, fs []protocol.FileInfo, set *files.Set) {
	for i := 0; i < len(fs)/100; i++ {
		if i == 0 {
			set.Replace(id, fs[i*100:i*100+100])
		} else {
			set.Update(id, fs[i*100:i*100+100])
		}
	}
}

func TestConcurrentSet(t *testing.T) {
	dur := 30 * time.Second
	t0 := time.Now()
	var wg sync.WaitGroup

	os.RemoveAll("testdata/global.db")
	db, err := leveldb.OpenFile("testdata/global.db", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll("testdata/global.db")

	set := files.NewSet("foo", db)

	fs0 := setupFiles(9000)
	fs1 := make([]protocol.FileInfo, len(fs0))
	for i := 0; i < len(fs0); i += 3 {
		fs1[i] = fs0[i]

		fs1[i+1] = fs0[i+1]
		fs0[i+1].Version++

		fs1[i+2] = fs0[i+2]
		fs1[i+2].Version++
	}
	fs0 = append(fs0, setupFiles(1000)...)
	fs1 = append(fs1, setupFiles(1000)...)

	wg.Add(1)
	go func() {
		defer wg.Done()
		for time.Since(t0) < dur {
			setFiles(remoteDevice0, fs0, set)
			time.Sleep(25 * time.Millisecond)
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for time.Since(t0) < dur {
			setFiles(remoteDevice1, fs1, set)
			time.Sleep(50 * time.Millisecond)
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for time.Since(t0) < dur {
			i := 0
			set.WithGlobalTruncated(func(intf protocol.FileIntf) bool {
				i++
				return true
			})
			log.Println("a", i)
		}
	}()

	wg.Wait()
	db.Close()
}
