package migrations

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/Project-Sylos/Migration-Engine/pkg/db"
	"github.com/Project-Sylos/Migration-Engine/pkg/db/etl"
	"github.com/Project-Sylos/Migration-Engine/pkg/migration"
	corebridgeDB "github.com/Project-Sylos/Sylos-API/internal/corebridge/database"
)

// runETL runs the ETL process to migrate data from BoltDB to DuckDB
// This function handles the complete ETL lifecycle: setup, execution, and status updates
// boltDB can be nil, in which case it will be opened from dbPath
func (m *Manager) runETL(record *MigrationRecord, dbPath, configPath string, boltDB *db.DB) {
	// Check if ETL is already running
	record.ETLMutex.Lock()
	etlRunning := record.ETLRunning
	record.ETLMutex.Unlock()

	if etlRunning {
		m.logger.Debug().
			Str("migration_id", record.ID).
			Msg("ETL already running, skipping")
		return
	}

	// Mark ETL as running
	record.ETLMutex.Lock()
	record.ETLRunning = true
	record.ETLMutex.Unlock()

	// Get callbacks for background task management
	completeTaskFunc := m.getCompleteTaskFunc()
	failTaskFunc := m.getFailTaskFunc()

	// Start background task tracking for ETL
	var taskID string
	if m.startBgTaskFunc != nil {
		taskID = m.startBgTaskFunc(record.ID, "etl", "")
	}

	defer func() {
		record.ETLMutex.Lock()
		record.ETLRunning = false
		record.ETLMutex.Unlock()

		// Handle panic recovery
		if r := recover(); r != nil {
			if failTaskFunc != nil && taskID != "" {
				failTaskFunc(record.ID, taskID, fmt.Errorf("panic: %v", r))
			}
			panic(r) // Re-panic
		}
	}()

	// Get DuckDB path and check/delete existing file
	duckdbPath := corebridgeDB.GetDuckDBPath(dbPath)
	duckdbExists, err := corebridgeDB.CheckDuckDBExists(dbPath)
	if err != nil {
		m.logger.Warn().
			Err(err).
			Str("migration_id", record.ID).
			Msg("failed to check DuckDB existence")
	}

	if duckdbExists {
		m.logger.Info().
			Str("migration_id", record.ID).
			Str("duckdb_path", duckdbPath).
			Msg("DuckDB exists, deleting for fresh ETL run")

		if err := corebridgeDB.DeleteDuckDB(dbPath); err != nil {
			m.logger.Error().
				Err(err).
				Str("migration_id", record.ID).
				Msg("failed to delete existing DuckDB file")
			if failTaskFunc != nil && taskID != "" {
				failTaskFunc(record.ID, taskID, err)
			}
			return
		}
	}

	m.logger.Info().
		Str("migration_id", record.ID).
		Str("bolt_db_path", dbPath).
		Str("duckdb_path", duckdbPath).
		Str("task_id", taskID).
		Msg("starting ETL process")

	// Use provided BoltDB instance or open a new one
	needToClose := false
	if boltDB == nil {
		needToClose = true
		var err error
		boltDB, err = db.Open(db.Options{Path: dbPath})
		if err != nil {
			m.logger.Error().
				Err(err).
				Str("migration_id", record.ID).
				Str("bolt_db_path", dbPath).
				Msg("failed to open BoltDB for ETL")
			if failTaskFunc != nil && taskID != "" {
				failTaskFunc(record.ID, taskID, err)
			}
			return
		}
	}

	if needToClose {
		defer func() {
			if err := boltDB.Close(); err != nil {
				m.logger.Warn().
					Err(err).
					Str("migration_id", record.ID).
					Msg("failed to close BoltDB after ETL")
			}
		}()
	}

	// Ensure DuckDB directory exists
	duckdbDir := filepath.Dir(duckdbPath)
	if err := os.MkdirAll(duckdbDir, 0o755); err != nil {
		m.logger.Error().
			Err(err).
			Str("migration_id", record.ID).
			Str("duckdb_dir", duckdbDir).
			Msg("failed to create DuckDB directory")
		if failTaskFunc != nil && taskID != "" {
			failTaskFunc(record.ID, taskID, err)
		}
		return
	}

	// Run ETL
	cfg := etl.BoltToDuckConfig{
		BoltDB:     boltDB,
		DuckDBPath: duckdbPath,
		Overwrite:  true,
	}
	if err := etl.RunBoltToDuck(cfg); err != nil {
		m.logger.Error().
			Err(err).
			Str("migration_id", record.ID).
			Msg("ETL process failed")
		if failTaskFunc != nil && taskID != "" {
			failTaskFunc(record.ID, taskID, err)
		}
		return
	}

	m.logger.Info().
		Str("migration_id", record.ID).
		Str("duckdb_path", duckdbPath).
		Msg("ETL process completed successfully")

	// Complete background task
	if completeTaskFunc != nil && taskID != "" {
		completeTaskFunc(record.ID, taskID)
	}

	// Update status to Awaiting-Path-Review
	yamlCfg, err := migration.LoadMigrationConfig(configPath)
	if err != nil {
		m.logger.Error().
			Err(err).
			Str("migration_id", record.ID).
			Msg("failed to load YAML config to update status after ETL")
		return
	}

	yamlCfg.State.Status = "Awaiting-Path-Review"
	if err := migration.SaveMigrationConfig(configPath, yamlCfg); err != nil {
		m.logger.Error().
			Err(err).
			Str("migration_id", record.ID).
			Msg("failed to update status to Awaiting-Path-Review after ETL")
		return
	}

	// Store DuckDB path in record
	record.ETLMutex.Lock()
	record.DuckDBPath = duckdbPath
	record.ETLMutex.Unlock()

	m.logger.Info().
		Str("migration_id", record.ID).
		Str("status", "Awaiting-Path-Review").
		Msg("updated status to Awaiting-Path-Review after ETL completion")
}

// ensureETLCompleted checks if ETL needs to run and triggers it if needed
// This is called by path review operations to ensure DuckDB is ready
func (m *Manager) ensureETLCompleted(migrationID, configPath, dbPath string) error {
	// Load YAML config to check status
	yamlCfg, err := migration.LoadMigrationConfig(configPath)
	if err != nil {
		return fmt.Errorf("failed to load YAML config: %w", err)
	}

	status := strings.TrimSpace(yamlCfg.State.Status)

	// If status is Awaiting-Path-Review, DuckDB should exist
	if status == "Awaiting-Path-Review" {
		duckdbExists, err := corebridgeDB.CheckDuckDBExists(dbPath)
		if err != nil {
			return fmt.Errorf("failed to check DuckDB existence: %w", err)
		}
		if !duckdbExists {
			// Status says ready but DuckDB doesn't exist - reset to Preparing-Path-Review
			yamlCfg.State.Status = "Preparing-Path-Review"
			if err := migration.SaveMigrationConfig(configPath, yamlCfg); err != nil {
				return fmt.Errorf("failed to reset status: %w", err)
			}
			status = "Preparing-Path-Review"
		}
	}

	// If status is Preparing-Path-Review, check if ETL is running or needs to start
	if status == "Preparing-Path-Review" {
		record := m.GetRecord(migrationID)
		if record == nil {
			return fmt.Errorf("migration record not found")
		}

		record.ETLMutex.Lock()
		etlRunning := record.ETLRunning
		record.ETLMutex.Unlock()

		if !etlRunning {
			// Check DuckDB existence
			duckdbExists, err := corebridgeDB.CheckDuckDBExists(dbPath)
			if err != nil {
				return fmt.Errorf("failed to check DuckDB existence: %w", err)
			}

			if !duckdbExists {
				// Delete any existing DuckDB and start ETL
				if err := corebridgeDB.DeleteDuckDB(dbPath); err != nil {
					m.logger.Warn().
						Err(err).
						Str("migration_id", migrationID).
						Msg("failed to delete DuckDB file (may not exist)")
				}

				// Trigger ETL (no existing BoltDB instance available, so pass nil)
				go m.runETL(record, dbPath, configPath, nil)

				// Wait a bit for ETL to start (non-blocking check)
				// The UI will poll the background tasks endpoint to check status
				return fmt.Errorf("ETL started, please check background tasks status")
			}
		}

		// ETL is running, return error to indicate it's in progress
		return fmt.Errorf("ETL is in progress, please check background tasks status")
	}

	return nil
}
