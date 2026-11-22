package migrationfiles

import (
	"fmt"
	"path/filepath"
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
