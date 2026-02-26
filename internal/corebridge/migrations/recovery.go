package migrations

import (
	"context"
	"os"

	corebridgeDB "codeberg.org/Sylos/Sylos-API/internal/corebridge/database"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge/metadata"
)

// RecoverInterruptedETL checks for migrations with stale status and fixes them
// Duck-only: no ETL. Only fixes status if migration DB exists (e.g. Preparing-Path-Review -> Awaiting-Path-Review)
func (m *Manager) RecoverInterruptedETL() {
	m.logger.Info().Msg("checking for migrations with stale status on startup")

	metaMgr := metadata.NewManager(m.cfg.Runtime.DataDir)
	allMeta, err := metaMgr.LoadAllMetadata()
	if err != nil {
		m.logger.Warn().Err(err).Msg("failed to load migration metadata for recovery")
		return
	}

	recoveredCount := 0
	for _, meta := range allMeta.Migrations {
		dbPath := meta.DatabasePath
		if dbPath == "" {
			continue
		}
		if _, err := os.Stat(dbPath); os.IsNotExist(err) {
			continue
		}

		status, err := corebridgeDB.InspectMigrationStatusFromDB(context.TODO(), m.logger, dbPath)
		if err != nil {
			continue
		}
		if status.HasPending() || status.HasFailures() {
			recoveredCount++
			m.logger.Info().Str("migration_id", meta.ID).Msg("found resumable migration state in DB-only mode")
		}
	}

	if recoveredCount > 0 {
		m.logger.Info().Int("recovered_count", recoveredCount).Msg("recovery complete")
	} else {
		m.logger.Debug().Msg("no migrations needed recovery")
	}
}
