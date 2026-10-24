package migrationfiles

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// RemoveMigrationDir deletes dataDir/{migrationID}/ (ops store, configs, etc.).
func RemoveMigrationDir(dataDir, migrationID string) error {
	raw := strings.TrimSpace(migrationID)
	if raw == "" || strings.Contains(raw, "..") || strings.ContainsAny(raw, `/\`) {
		return fmt.Errorf("invalid migration id")
	}
	id := NormalizeMigrationID(raw)
	if id == "" || id == "." || strings.Contains(id, "..") || strings.ContainsAny(id, `/\`) {
		return fmt.Errorf("invalid migration id")
	}
	absDataDir, err := filepath.Abs(dataDir)
	if err != nil {
		return fmt.Errorf("resolve data dir: %w", err)
	}
	dir := filepath.Join(absDataDir, id)
	rel, err := filepath.Rel(absDataDir, dir)
	if err != nil || rel == "." || strings.HasPrefix(rel, "..") {
		return fmt.Errorf("refusing to remove path outside data dir")
	}
	if err := os.RemoveAll(dir); err != nil {
		return fmt.Errorf("remove migration dir %q: %w", id, err)
	}
	return nil
}
