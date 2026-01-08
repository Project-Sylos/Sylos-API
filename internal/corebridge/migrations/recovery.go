package migrations

import (
	"os"
	"strings"

	"github.com/Project-Sylos/Migration-Engine/pkg/migration"
	corebridgeDB "github.com/Project-Sylos/Sylos-API/internal/corebridge/database"
	"github.com/Project-Sylos/Sylos-API/internal/corebridge/metadata"
)

// RecoverInterruptedETL scans all migrations and resumes ETL for any that were interrupted
// This is called on API startup to recover from crashes between traversal and path review
func (m *Manager) RecoverInterruptedETL() {
	m.logger.Info().Msg("checking for interrupted ETL processes on startup")

	// Get all migration metadata
	metaMgr := metadata.NewManager(m.cfg.Runtime.DataDir)
	allMeta, err := metaMgr.LoadAllMetadata()
	if err != nil {
		m.logger.Warn().
			Err(err).
			Msg("failed to load migration metadata for ETL recovery")
		return
	}

	recoveredCount := 0
	for _, meta := range allMeta.Migrations {
		if meta.ConfigPath == "" {
			continue
		}

		// Check if config file exists
		if _, err := os.Stat(meta.ConfigPath); os.IsNotExist(err) {
			continue
		}

		// Load YAML config to check status
		yamlCfg, err := migration.LoadMigrationConfig(meta.ConfigPath)
		if err != nil {
			m.logger.Debug().
				Err(err).
				Str("migration_id", meta.ID).
				Str("config_path", meta.ConfigPath).
				Msg("failed to load migration config for recovery check")
			continue
		}

		currentStatus := strings.TrimSpace(yamlCfg.State.Status)

		// Only recover migrations in Preparing-Path-Review status
		if currentStatus != "Preparing-Path-Review" {
			continue
		}

		// Get database path
		dbPath := corebridgeDB.DatabasePathFromConfigPath(meta.ConfigPath)
		if dbPath == "" {
			m.logger.Debug().
				Str("migration_id", meta.ID).
				Msg("cannot determine database path for recovery")
			continue
		}

		// Check if DuckDB already exists (ETL might have completed)
		duckdbExists, err := corebridgeDB.CheckDuckDBExists(dbPath)
		if err != nil {
			m.logger.Warn().
				Err(err).
				Str("migration_id", meta.ID).
				Msg("failed to check DuckDB existence during recovery")
			continue
		}

		if duckdbExists {
			// DuckDB exists, but status is still Preparing-Path-Review
			// This means ETL completed but status wasn't updated
			// Update status to Awaiting-Path-Review
			yamlCfg.State.Status = "Awaiting-Path-Review"
			if err := migration.SaveMigrationConfig(meta.ConfigPath, yamlCfg); err != nil {
				m.logger.Error().
					Err(err).
					Str("migration_id", meta.ID).
					Msg("failed to update status to Awaiting-Path-Review during recovery")
			} else {
				m.logger.Info().
					Str("migration_id", meta.ID).
					Msg("recovered: DuckDB exists, updated status to Awaiting-Path-Review")
				recoveredCount++
			}
			continue
		}

		// DuckDB doesn't exist, need to trigger ETL
		// Get or create migration record
		record := m.GetRecord(meta.ID)
		if record == nil {
			// Create a minimal record for recovery
			record = &MigrationRecord{
				ID: meta.ID,
			}
			m.SetRecord(meta.ID, record)
		}

		// Check if ETL is already running (shouldn't be on startup, but check anyway)
		if record.GetETLRunning() {
			m.logger.Debug().
				Str("migration_id", meta.ID).
				Msg("ETL already marked as running, skipping recovery")
			continue
		}

		// Trigger ETL recovery
		m.logger.Info().
			Str("migration_id", meta.ID).
			Str("db_path", dbPath).
			Msg("recovering interrupted ETL process")

		// For recovery, we need to open BoltDB since we don't have an existing instance
		// Pass nil to runETL so it will open it
		go m.runETL(record, dbPath, meta.ConfigPath, nil)
		recoveredCount++
	}

	if recoveredCount > 0 {
		m.logger.Info().
			Int("recovered_count", recoveredCount).
			Msg("recovery complete: resumed interrupted ETL processes")
	} else {
		m.logger.Debug().Msg("no interrupted ETL processes found")
	}
}
