package migrationfiles

import (
	"fmt"
	"path/filepath"
	"strings"
)

// MigrationDir returns the folder for one migration under the migration DB storage dir.
// Convention: storageDir/{migrationID}/ (with {migrationID}.ops/, {migrationID}.logs/, and logical {migrationID}.db inside).
func MigrationDir(storageDir, migrationID string) string {
	return filepath.Join(storageDir, migrationID)
}

// ResolveDatabasePath resolves the database path from migration ID or explicit path.
// Convention: storageDir/{migrationID}/{migrationID}.db
func ResolveDatabasePath(storageDir, explicitPath, migrationID string) (string, error) {
	if explicitPath != "" {
		return explicitPath, nil
	}

	if migrationID == "" {
		return "", fmt.Errorf("migration ID is required when path is not provided")
	}

	migrationDir := MigrationDir(storageDir, migrationID)
	dbPath := filepath.Join(migrationDir, migrationID+".db")
	return dbPath, nil
}

func canonicalMigrationID(raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" || strings.Contains(raw, "..") || strings.ContainsAny(raw, `/\`) {
		return "", fmt.Errorf("invalid migration id")
	}
	id := NormalizeMigrationID(raw)
	if id == "" || id == "." || strings.Contains(id, "..") || strings.ContainsAny(id, `/\`) {
		return "", fmt.Errorf("invalid migration id")
	}
	return id, nil
}

// FolderFromDatabasePath returns the migration folder for a stored logical DB path
// (storage/{id}/{id}.db) or the folder path itself. The last path component must be the id.
func FolderFromDatabasePath(databasePath, migrationID string) (string, error) {
	id, err := canonicalMigrationID(migrationID)
	if err != nil {
		return "", err
	}
	raw := strings.TrimSpace(databasePath)
	if raw == "" {
		return "", fmt.Errorf("database path is empty")
	}
	abs, err := filepath.Abs(raw)
	if err != nil {
		return "", fmt.Errorf("resolve database path: %w", err)
	}
	dir := abs
	if strings.EqualFold(filepath.Ext(abs), ".db") {
		dir = filepath.Dir(abs)
	}
	if filepath.Base(dir) != id {
		return "", fmt.Errorf("database path does not match migration id")
	}
	return dir, nil
}
