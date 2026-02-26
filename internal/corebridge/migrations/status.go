package migrations

import (
	"context"
	"os"

	"codeberg.org/Sylos/Migration-Engine/pkg/migration"
	corebridgeDB "codeberg.org/Sylos/Sylos-API/internal/corebridge/database"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge/metadata"
)

func (m *Manager) GetMigrationStatus(ctx context.Context, id string) (Status, error) {
	m.mu.RLock()
	record, ok := m.migrations[id]
	m.mu.RUnlock()

	if ok {
		return m.recordToStatus(record), nil
	}

	meta, err := m.metadataMgr.GetMigrationMetadata(id)
	if err != nil {
		m.logger.Debug().
			Err(err).
			Str("migration_id", id).
			Msg("failed to get migration metadata")
		return Status{}, ErrMigrationNotFound
	}

	if meta.DatabasePath == "" {
		m.logger.Debug().
			Str("migration_id", id).
			Msg("migration metadata has no database path")
		return Status{}, ErrMigrationNotFound
	}

	if _, err := os.Stat(meta.DatabasePath); os.IsNotExist(err) {
		m.logger.Debug().
			Str("migration_id", id).
			Str("database_path", meta.DatabasePath).
			Msg("migration database file does not exist")
		return Status{}, ErrMigrationNotFound
	}

	engineStatus, err := corebridgeDB.InspectMigrationStatusFromDB(ctx, m.logger, meta.DatabasePath)
	if err != nil {
		m.logger.Debug().
			Err(err).
			Str("migration_id", id).
			Str("database_path", meta.DatabasePath).
			Msg("failed to inspect migration status from database")
		return Status{}, ErrMigrationNotFound
	}

	status := Status{
		Migration: Migration{
			ID:        id,
			StartedAt: meta.CreatedAt,
			Status:    statusFromInspection(engineStatus),
		},
	}

	return status, nil
}

func (m *Manager) InspectMigrationStatus(ctx context.Context, migrationID string) (migration.MigrationStatus, error) {
	dbPath, err := m.resolveDBPath("", migrationID)
	if err != nil {
		return migration.MigrationStatus{}, ErrMigrationNotFound
	}

	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		return migration.MigrationStatus{}, ErrMigrationNotFound
	}

	return corebridgeDB.InspectMigrationStatusFromDB(ctx, m.logger, dbPath)
}

func (m *Manager) GetRecord(migrationID string) *MigrationRecord {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.migrations[migrationID]
}

func (m *Manager) GetController(migrationID string) *migration.MigrationController {
	record := m.GetRecord(migrationID)
	if record == nil {
		return nil
	}
	return record.Controller
}

func (m *Manager) SetRecord(migrationID string, record *MigrationRecord) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.migrations[migrationID] = record
}

func (m *Manager) updateMetadataForMigration(migrationID, name, configPath string) error {
	databasePath := configPath
	meta, err := m.metadataMgr.GetMigrationMetadata(migrationID)
	if err != nil {
		meta = metadata.MigrationMetadata{
			ID:           migrationID,
			Name:         name,
			DatabasePath: databasePath,
		}
	} else {
		if name != "" {
			meta.Name = name
		}
		if databasePath != "" {
			meta.DatabasePath = databasePath
		}
	}

	return m.metadataMgr.UpdateMigrationMetadata(meta)
}

func (m *Manager) recordToStatus(r *MigrationRecord) Status {
	status := Status{
		Migration: Migration{
			ID:            r.ID,
			SourceID:      r.SourceID,
			DestinationID: r.DestinationID,
			StartedAt:     r.StartedAt,
			Status:        r.Status,
		},
	}

	if r.CompletedAt != nil {
		status.CompletedAt = r.CompletedAt
	}
	if r.Error != "" {
		status.Error = r.Error
	}
	if r.Result != nil {
		status.Result = resultToView(r.Result)
	}

	return status
}

func statusFromInspection(s migration.MigrationStatus) string {
	switch {
	case s.IsComplete():
		return "Complete"
	case s.HasPending():
		return "Traversal-In-Progress"
	case s.HasFailures():
		return "Suspended"
	default:
		return MigrationStatusRunning
	}
}
