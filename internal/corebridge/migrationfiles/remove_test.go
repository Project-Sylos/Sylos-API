package migrationfiles

import (
	"os"
	"path/filepath"
	"testing"
)

func TestRemoveMigrationDir(t *testing.T) {
	t.Parallel()
	dataDir := t.TempDir()
	id := "mig-rm"
	dir := filepath.Join(dataDir, id)
	if err := os.MkdirAll(filepath.Join(dir, id+".ops"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := RemoveMigrationDir(dataDir, id); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Fatalf("dir still exists: %v", err)
	}
	if err := RemoveMigrationDir(dataDir, "../escape"); err == nil {
		t.Fatal("expected invalid id error")
	}
}
