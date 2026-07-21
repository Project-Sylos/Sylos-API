package apidb

import (
	"os"
	"path/filepath"
	"testing"
)

func TestSFTPKnownHostRoundTrip(t *testing.T) {
	dir := t.TempDir()
	key := make([]byte, 32)
	for i := range key {
		key[i] = byte(i + 1)
	}
	db, err := Open(dir, key)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })

	if _, ok, err := db.LookupSFTPKnownHost("example.com", 22); err != nil || ok {
		t.Fatalf("empty lookup ok=%v err=%v", ok, err)
	}
	if err := db.UpsertSFTPKnownHost("Example.COM", 22, "abc", "SHA256:fp"); err != nil {
		t.Fatal(err)
	}
	row, ok, err := db.LookupSFTPKnownHost("example.com", 22)
	if err != nil || !ok {
		t.Fatalf("lookup ok=%v err=%v", ok, err)
	}
	if row.HostKey != "abc" || row.Fingerprint != "SHA256:fp" {
		t.Fatalf("row=%+v", row)
	}

	legacy := filepath.Join(dir, "sftp-known-hosts.json")
	if err := os.WriteFile(legacy, []byte(`{"hosts":{"other.com:22":{"hostKey":"xyz","fingerprint":"SHA256:o"}}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := db.ImportLegacySFTPKnownHostsFile(dir); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := db.LookupSFTPKnownHost("other.com", 22); err != nil || !ok {
		t.Fatalf("legacy import ok=%v err=%v", ok, err)
	}
	if _, err := os.Stat(legacy); !os.IsNotExist(err) {
		t.Fatalf("legacy file should be removed, err=%v", err)
	}
	if err := db.DeleteAllSFTPKnownHosts(); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := db.LookupSFTPKnownHost("example.com", 22); err != nil || ok {
		t.Fatalf("after delete ok=%v err=%v", ok, err)
	}
}
