package database

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
	if err := os.WriteFile(filepath.Join(dir, "sylos.duckdb"), []byte("api"), 0o644); err != nil {
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
	if _, err := os.Stat(filepath.Join(dir, "sylos.duckdb")); err != nil {
		t.Fatalf("sylos.duckdb should remain: %v", err)
	}

	raw, err := os.ReadFile(filepath.Join(dir, "migrations.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	if string(raw) != "migrations:\n" {
		t.Fatalf("migrations.yaml not reset: %q", string(raw))
	}
}
