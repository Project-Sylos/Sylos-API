package migrations

import (
	"os"
	"strings"

	"codeberg.org/Sylos/Migration-Engine/pkg/migration"
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
		if meta.ConfigPath == "" {
			continue
		}
		if _, err := os.Stat(meta.ConfigPath); os.IsNotExist(err) {
			continue
		}

		yamlCfg, err := migration.LoadMigrationConfig(meta.ConfigPath)
		if err != nil {
			continue
		}

		currentStatus := strings.TrimSpace(yamlCfg.State.Status)
		dbPath := corebridgeDB.DatabasePathFromConfigPath(meta.ConfigPath)
		if dbPath == "" {
			continue
		}

		// If status is Preparing-Path-Review (obsolete) but DB exists, fix status
		if currentStatus == "Preparing-Path-Review" {
			if _, err := os.Stat(dbPath); err == nil {
				yamlCfg.State.Status = "Awaiting-Path-Review"
				if err := migration.SaveMigrationConfig(meta.ConfigPath, yamlCfg); err != nil {
					m.logger.Error().Err(err).Str("migration_id", meta.ID).Msg("failed to update status during recovery")
				} else {
					m.logger.Info().Str("migration_id", meta.ID).Msg("recovered: updated stale Preparing-Path-Review to Awaiting-Path-Review")
					recoveredCount++
				}
			}
		}
	}

	if recoveredCount > 0 {
		m.logger.Info().Int("recovered_count", recoveredCount).Msg("recovery complete")
	} else {
		m.logger.Debug().Msg("no migrations needed recovery")
	}
}
