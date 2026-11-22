package migrationfiles

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// RemoveMigrationDir deletes storageDir/{migrationID}/ (ops store, configs, etc.).
func RemoveMigrationDir(storageDir, migrationID string) error {
	id, err := canonicalMigrationID(migrationID)
	if err != nil {
		return err
	}
	absStorage, err := filepath.Abs(storageDir)
	if err != nil {
		return fmt.Errorf("resolve storage dir: %w", err)
	}
	dir := filepath.Join(absStorage, id)
	rel, err := filepath.Rel(absStorage, dir)
	if err != nil || rel == "." || strings.HasPrefix(rel, "..") {
		return fmt.Errorf("refusing to remove path outside storage dir")
	}
	return RemoveMigrationFolder(dir, id)
}

// RemoveMigrationFolder deletes one migration folder. The last path component must be the id.
func RemoveMigrationFolder(dir, migrationID string) error {
	id, err := canonicalMigrationID(migrationID)
	if err != nil {
		return err
	}
	abs, err := filepath.Abs(dir)
	if err != nil {
		return fmt.Errorf("resolve migration dir: %w", err)
	}
	if filepath.Base(abs) != id {
		return fmt.Errorf("refusing to remove path that does not match migration id")
	}
	if err := os.RemoveAll(abs); err != nil {
		return fmt.Errorf("remove migration dir %q: %w", id, err)
	}
	return nil
}
