package migrationfiles

import (
	"path/filepath"
	"testing"
)

func TestFolderFromDatabasePath(t *testing.T) {
	t.Parallel()

	id := "migration-1"
	dir := filepath.Join(t.TempDir(), "migration-dbs", id)
	dbPath := filepath.Join(dir, id+".db")

	got, err := FolderFromDatabasePath(dbPath, id)
	if err != nil {
		t.Fatal(err)
	}
	if got != dir {
		t.Fatalf("got %q want %q", got, dir)
	}

	got, err = FolderFromDatabasePath(dir, id)
	if err != nil {
		t.Fatal(err)
	}
	if got != dir {
		t.Fatalf("folder path: got %q want %q", got, dir)
	}

	if _, err := FolderFromDatabasePath(dbPath, "other-id"); err == nil {
		t.Fatal("expected mismatch error")
	}
	if _, err := FolderFromDatabasePath("", id); err == nil {
		t.Fatal("expected empty path error")
	}
}
