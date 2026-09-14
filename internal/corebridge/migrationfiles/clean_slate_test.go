package migrationfiles

import (
	"os"
	"path/filepath"
	"testing"
)

func TestCleanMigrationData(t *testing.T) {
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "migration-a"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "migration-a", "migration-a.db"), []byte("db"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(dir, "sylos.api"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "sylos.api", "MANIFEST"), []byte("api"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "migrations.yaml"), []byte("migrations:\n  old:\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	if err := CleanMigrationData(dir); err != nil {
		t.Fatal(err)
	}

	if _, err := os.Stat(filepath.Join(dir, "migration-a")); !os.IsNotExist(err) {
		t.Fatalf("migration folder should be removed, err=%v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "sylos.api")); err != nil {
		t.Fatalf("sylos.api should remain: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "migrations.yaml")); !os.IsNotExist(err) {
		t.Fatalf("migrations.yaml should be removed, err=%v", err)
	}
}
