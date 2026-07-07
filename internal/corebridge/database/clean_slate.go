package database

import (
	"fmt"
	"os"
	"path/filepath"
)

// preservedDataDirEntries are never deleted during a clean slate.
var preservedDataDirEntries = map[string]bool{
	"api-runtime.log": true,
	"sylos.duckdb":    true,
	"migrations.yaml": true,
}

// CleanMigrationData removes on-disk migration folders and resets legacy metadata files.
// User accounts, provider OAuth apps, and the encrypted API database file are preserved.
func CleanMigrationData(dataDir string) error {
	absDataDir, err := filepath.Abs(dataDir)
	if err != nil {
		return fmt.Errorf("resolve data dir: %w", err)
	}

	entries, err := os.ReadDir(absDataDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("read data dir: %w", err)
	}

	for _, entry := range entries {
		name := entry.Name()
		if preservedDataDirEntries[name] {
			if name == "migrations.yaml" {
				if err := resetLegacyMigrationsYAML(filepath.Join(absDataDir, name)); err != nil {
					return err
				}
			}
			continue
		}
		path := filepath.Join(absDataDir, name)
		if err := os.RemoveAll(path); err != nil {
			return fmt.Errorf("remove %q: %w", name, err)
		}
	}

	return nil
}

func resetLegacyMigrationsYAML(path string) error {
	content := "migrations:\n"
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("reset migrations.yaml: %w", err)
	}
	return nil
}
