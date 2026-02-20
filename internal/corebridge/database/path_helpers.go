package database

import (
	"fmt"
	"path/filepath"
	"strings"

	"codeberg.org/Sylos/Migration-Engine/pkg/migration"
)

// ResolveDatabasePath resolves the database path from migration ID or explicit path.
// Convention: dataDir/{migrationID}/{migrationID}.db
func ResolveDatabasePath(dataDir, explicitPath, migrationID string) (string, error) {
	if explicitPath != "" {
		return explicitPath, nil
	}

	if migrationID == "" {
		return "", fmt.Errorf("migration ID is required when path is not provided")
	}

	migrationDir := GetMigrationDir(dataDir, migrationID)
	dbPath := filepath.Join(migrationDir, migrationID+".db")
	return dbPath, nil
}

// GetMigrationDir returns the directory path for a migration
func GetMigrationDir(dataDir, migrationID string) string {
	return filepath.Join(dataDir, migrationID)
}

// ConfigPathFromDatabasePath derives the config path from a database path.
// Uses engine convention: {dbPath minus .db}.yaml
func ConfigPathFromDatabasePath(dbPath string) string {
	return migration.ConfigPathFromDatabasePath(dbPath)
}

// DatabasePathFromConfigPath derives the database path from a config path.
// Convention: config is in dataDir/{migrationID}/{migrationID}.yaml, DB is in same dir with .db
func DatabasePathFromConfigPath(configPath string) string {
	dir := filepath.Dir(configPath)
	filename := filepath.Base(configPath)
	migrationID := strings.TrimSuffix(filename, ".yaml")
	return filepath.Join(dir, migrationID+".db")
}
