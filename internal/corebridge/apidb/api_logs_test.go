package apidb

import (
	"bytes"
	"path/filepath"
	"testing"
	"time"

	badger "github.com/dgraph-io/badger/v4"
)

func TestAppendAPILogsBatchAndBufferFlush(t *testing.T) {
	dir := t.TempDir()
	key := bytes.Repeat([]byte{7}, 32)
	db, err := Open(dir, key)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if err := db.AppendAPILog("info", "hello buffered", "api"); err != nil {
		t.Fatal(err)
	}
	if err := db.AppendAPILogs([]APILogRecord{{
		Level: "warn", Message: "direct batch", Component: "http",
	}}); err != nil {
		t.Fatal(err)
	}

	// Force buffer drain before counting.
	db.logBuf.Stop()
	db.logBuf = nil

	n := 0
	err = db.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		prefix := []byte(prefixAPILog)
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			n++
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if n < 2 {
		t.Fatalf("want at least 2 apilog keys, got %d (store %s)", n, filepath.Join(dir, defaultStoreDirName))
	}
}

func TestAPILogBufferIntervalFlush(t *testing.T) {
	dir := t.TempDir()
	key := bytes.Repeat([]byte{9}, 32)
	rawOpts := badger.DefaultOptions(filepath.Join(dir, defaultStoreDirName))
	rawOpts.Logger = nil
	raw, err := badger.Open(rawOpts)
	if err != nil {
		t.Fatal(err)
	}
	d := &DB{dir: filepath.Join(dir, defaultStoreDirName), masterKey: key, db: raw}
	d.logBuf = newAPILogBuffer(d, 100, 50*time.Millisecond)
	defer d.Close()

	if err := d.AppendAPILog("info", "tick flush", "api"); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		var n int
		_ = d.db.View(func(txn *badger.Txn) error {
			it := txn.NewIterator(badger.DefaultIteratorOptions)
			defer it.Close()
			prefix := []byte(prefixAPILog)
			for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
				n++
			}
			return nil
		})
		if n >= 1 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("buffered log did not flush within timeout")
}
