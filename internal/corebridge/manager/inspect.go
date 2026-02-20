package manager

import (
	"context"

	"codeberg.org/Sylos/Migration-Engine/pkg/migration"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge/database"
)

func (m *Manager) InspectMigrationStatus(ctx context.Context, migrationID string) (migration.MigrationStatus, error) {
	return m.migrationsMgr.InspectMigrationStatus(ctx, migrationID)
}

func (m *Manager) InspectMigrationStatusFromDB(ctx context.Context, dbPath string) (migration.MigrationStatus, error) {
	return database.InspectMigrationStatusFromDB(ctx, m.logger, dbPath)
}
