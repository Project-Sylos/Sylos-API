package database

import (
	"context"
	"fmt"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
	"codeberg.org/Sylos/Migration-Engine/pkg/migration"
	"github.com/rs/zerolog"
)

// InspectMigrationStatusFromDB opens the migration DB at dbPath, inspects it via the engine,
// and returns the MigrationStatus. Use when there is no live controller.
func InspectMigrationStatusFromDB(ctx context.Context, logger zerolog.Logger, dbPath string) (migration.MigrationStatus, error) {
	database, err := db.Open(db.Options{Path: dbPath})
	if err != nil {
		return migration.MigrationStatus{}, fmt.Errorf("failed to open database: %w", err)
	}
	defer func() {
		if err := database.Close(); err != nil {
			logger.Warn().Err(err).Str("db_path", dbPath).Msg("failed to close database after inspection")
		}
	}()

	status, err := migration.InspectMigrationStatus(database)
	if err != nil {
		return migration.MigrationStatus{}, fmt.Errorf("failed to inspect migration status: %w", err)
	}
	return status, nil
}
