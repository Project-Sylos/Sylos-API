package manager

import (
	"context"
	"fmt"
	"strings"

	"codeberg.org/Sylos/Sylos-API/internal/corebridge/migrationfiles"
)

// DeleteMigrationResult is returned by the HTTP delete handler.
type DeleteMigrationResult struct {
	ID      string `json:"id"`
	Message string `json:"message"`
}

// DeleteMigration stops a migration if needed, unloads it from memory, and
// permanently removes its registry entry, encryption key, and on-disk folder.
func (m *Manager) DeleteMigration(ctx context.Context, migrationID string) (DeleteMigrationResult, error) {
	id := migrationfiles.NormalizeMigrationID(migrationID)
	if id == "" {
		return DeleteMigrationResult{}, fmt.Errorf("migration id is required")
	}
	if strings.Contains(id, "..") {
		return DeleteMigrationResult{}, fmt.Errorf("invalid migration id")
	}

	m.mu.RLock()
	_, running := m.runtimeByID[id]
	m.mu.RUnlock()
	if running {
		if _, err := m.ForceStopMigration(ctx, id); err != nil {
			m.logger.Warn().Err(err).Str("migration_id", id).Msg("delete migration: force-stop")
		}
	}

	if m.rootsMgr != nil {
		m.rootsMgr.ClearPlan(id)
	}

	dir, err := m.migrationDirFor(id)
	if err != nil {
		return DeleteMigrationResult{}, err
	}
	if m.engineMgr != nil {
		if err := m.engineMgr.DeleteMigration(id, dir, nil); err != nil {
			m.logger.Warn().Err(err).Str("migration_id", id).Msg("delete migration: unload engine")
		}
	}

	m.mu.Lock()
	delete(m.runtimeByID, id)
	delete(m.progressByID, id)
	m.mu.Unlock()

	if m.bgTaskMgr != nil {
		m.bgTaskMgr.ClearMigration(id)
	}

	if m.apiDB != nil {
		if err := m.apiDB.DeleteMigration(id); err != nil {
			return DeleteMigrationResult{}, fmt.Errorf("delete migration registry: %w", err)
		}
	}

	if err := migrationfiles.RemoveMigrationDir(m.cfg.Runtime.DataDir, id); err != nil {
		return DeleteMigrationResult{}, err
	}

	m.logger.Info().Str("migration_id", id).Msg("deleted migration")
	return DeleteMigrationResult{
		ID:      id,
		Message: "Migration deleted.",
	}, nil
}
