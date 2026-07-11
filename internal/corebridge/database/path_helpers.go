package database

import (
	"fmt"
	"path/filepath"
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

	migrationDir := filepath.Join(dataDir, migrationID)
	dbPath := filepath.Join(migrationDir, migrationID+".db")
	return dbPath, nil
}
