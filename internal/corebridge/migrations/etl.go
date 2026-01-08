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
	if record.GetETLRunning() {
		m.logger.Debug().
			Str("migration_id", record.ID).
			Msg("ETL already running, skipping")
		return
	}

	// Mark ETL as running
	record.SetETLRunning(true)

	// Get callbacks for background task management
	completeTaskFunc := m.getCompleteTaskFunc()
	failTaskFunc := m.getFailTaskFunc()

	// Start background task tracking for ETL
	var taskID string
	if m.startBgTaskFunc != nil {
		taskID = m.startBgTaskFunc(record.ID, "etl", "")
	}

	defer func() {
		record.SetETLRunning(false)

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

	// Update status to ETL-Bolt-To-Duck-In-Progress before starting ETL
	yamlCfg, err := migration.LoadMigrationConfig(configPath)
	if err == nil {
		currentStatus := strings.TrimSpace(yamlCfg.State.Status)
		// Only update if we're in Preparing-Path-Review (normal traversal) or Filters-Set (after retry)
		if currentStatus == "Preparing-Path-Review" || currentStatus == "Filters-Set" {
			yamlCfg.State.Status = "ETL-Bolt-To-Duck-In-Progress"
			if err := migration.SaveMigrationConfig(configPath, yamlCfg); err != nil {
				m.logger.Warn().
					Err(err).
					Str("migration_id", record.ID).
					Msg("failed to update status to ETL-Bolt-To-Duck-In-Progress")
			} else {
				m.logger.Info().
					Str("migration_id", record.ID).
					Str("old_status", currentStatus).
					Str("new_status", "ETL-Bolt-To-Duck-In-Progress").
					Msg("updated status to ETL-Bolt-To-Duck-In-Progress")
			}
		}
	} else {
		m.logger.Warn().
			Err(err).
			Str("migration_id", record.ID).
			Msg("failed to load YAML config to update status before ETL")
	}

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

	// Update status to Awaiting-Path-Review after ETL completes
	// Note: yamlCfg and err are already declared above, so we reuse them
	yamlCfg, err = migration.LoadMigrationConfig(configPath)
	if err != nil {
		m.logger.Error().
			Err(err).
			Str("migration_id", record.ID).
			Msg("failed to load YAML config to update status after ETL")
		return
	}

	currentStatus := strings.TrimSpace(yamlCfg.State.Status)
	// Only update if we're in ETL-Bolt-To-Duck-In-Progress
	if currentStatus == "ETL-Bolt-To-Duck-In-Progress" {
		yamlCfg.State.Status = "Awaiting-Path-Review"
		if err := migration.SaveMigrationConfig(configPath, yamlCfg); err != nil {
			m.logger.Error().
				Err(err).
				Str("migration_id", record.ID).
				Msg("failed to update status to Awaiting-Path-Review after ETL")
			return
		}
		m.logger.Info().
			Str("migration_id", record.ID).
			Str("old_status", currentStatus).
			Str("new_status", "Awaiting-Path-Review").
			Msg("updated status to Awaiting-Path-Review after ETL completion")
	} else {
		m.logger.Warn().
			Str("migration_id", record.ID).
			Str("current_status", currentStatus).
			Msg("skipping status update to Awaiting-Path-Review (unexpected status)")
	}

	// Store DuckDB path in record
	record.ETLMutex.Lock()
	record.DuckDBPath = duckdbPath
	record.ETLMutex.Unlock()
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

		if !record.GetETLRunning() {
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

// RunETLFromBoltToDuck runs the ETL process to migrate data from BoltDB to DuckDB
// This function handles the complete ETL lifecycle: setup, execution, and status updates
// boltDB can be nil, in which case it will be opened from dbPath
func (m *Manager) RunETLFromBoltToDuck(record *MigrationRecord, dbPath, configPath string, boltDB *db.DB) {
	m.runETL(record, dbPath, configPath, boltDB)
}

// RunETLFromDuckToBolt runs the ETL process to migrate data from DuckDB back to BoltDB
// This function handles the complete ETL lifecycle: setup, execution, and status updates
// It deletes the existing BoltDB file and creates a fresh one from DuckDB
// This function is fully sequential and blocking - it does not return until ETL completes
func (m *Manager) RunETLFromDuckToBolt(record *MigrationRecord, dbPath, configPath string, onComplete func()) {
	// Check if ETL is already running
	if record.GetETLRunning() {
		m.logger.Debug().
			Str("migration_id", record.ID).
			Msg("ETL already running, skipping")
		return
	}

	// Mark ETL as running
	record.SetETLRunning(true)

	// Get callbacks for background task management
	completeTaskFunc := m.getCompleteTaskFunc()
	failTaskFunc := m.getFailTaskFunc()

	// Start background task tracking for ETL
	var taskID string
	if m.startBgTaskFunc != nil {
		taskID = m.startBgTaskFunc(record.ID, "etl-duck-to-bolt", "")
	}

	defer func() {
		// Handle panic recovery
		if r := recover(); r != nil {
			record.SetETLRunning(false)
			if failTaskFunc != nil && taskID != "" {
				failTaskFunc(record.ID, taskID, fmt.Errorf("panic: %v", r))
			}
			panic(r) // Re-panic
		}
		// Note: ETL running flag is cleared before onComplete callback, not in defer
	}()

	// Get DuckDB path and verify it exists
	duckdbPath := corebridgeDB.GetDuckDBPath(dbPath)
	duckdbExists, err := corebridgeDB.CheckDuckDBExists(dbPath)
	if err != nil {
		m.logger.Error().
			Err(err).
			Str("migration_id", record.ID).
			Msg("failed to check DuckDB existence")
		if failTaskFunc != nil && taskID != "" {
			failTaskFunc(record.ID, taskID, err)
		}
		return
	}

	if !duckdbExists {
		err := fmt.Errorf("DuckDB file does not exist at %s, cannot run ETL", duckdbPath)
		m.logger.Error().
			Err(err).
			Str("migration_id", record.ID).
			Str("duckdb_path", duckdbPath).
			Msg("DuckDB file not found")
		if failTaskFunc != nil && taskID != "" {
			failTaskFunc(record.ID, taskID, err)
		}
		return
	}

	m.logger.Info().
		Str("migration_id", record.ID).
		Str("bolt_db_path", dbPath).
		Str("duckdb_path", duckdbPath).
		Str("task_id", taskID).
		Msg("starting ETL from DuckDB to BoltDB")

	// Update status to ETL-Duck-To-Bolt-In-Progress before starting ETL
	yamlCfg, err := migration.LoadMigrationConfig(configPath)
	if err == nil {
		currentStatus := strings.TrimSpace(yamlCfg.State.Status)
		// Only update if we're in Preparing-For-Retry or Awaiting-Path-Review (retry triggered)
		if currentStatus == "Preparing-For-Retry" || currentStatus == "Awaiting-Path-Review" {
			yamlCfg.State.Status = "ETL-Duck-To-Bolt-In-Progress"
			if err := migration.SaveMigrationConfig(configPath, yamlCfg); err != nil {
				m.logger.Warn().
					Err(err).
					Str("migration_id", record.ID).
					Msg("failed to update status to ETL-Duck-To-Bolt-In-Progress")
			} else {
				m.logger.Info().
					Str("migration_id", record.ID).
					Str("old_status", currentStatus).
					Str("new_status", "ETL-Duck-To-Bolt-In-Progress").
					Msg("updated status to ETL-Duck-To-Bolt-In-Progress")
			}
		}
	}

	// Delete existing BoltDB file to create a fresh one
	if _, err := os.Stat(dbPath); err == nil {
		m.logger.Info().
			Str("migration_id", record.ID).
			Str("bolt_db_path", dbPath).
			Msg("deleting existing BoltDB file for fresh ETL")

		// Close the existing BoltDB instance if it's open in the record
		if record.DB != nil {
			if err := record.DB.Close(); err != nil {
				m.logger.Warn().
					Err(err).
					Str("migration_id", record.ID).
					Msg("failed to close existing BoltDB instance from record")
			}
			record.DB = nil
		}

		// Also close the DB from the pool if it's open
		if err := m.dbPool.KillConnection(record.ID); err != nil {
			m.logger.Warn().
				Err(err).
				Str("migration_id", record.ID).
				Msg("failed to close BoltDB instance from pool (may not be open)")
		}

		// Remove the file
		if err := os.Remove(dbPath); err != nil {
			m.logger.Error().
				Err(err).
				Str("migration_id", record.ID).
				Str("bolt_db_path", dbPath).
				Msg("failed to delete existing BoltDB file")
			if failTaskFunc != nil && taskID != "" {
				failTaskFunc(record.ID, taskID, err)
			}
			return
		}
	}

	// Ensure BoltDB directory exists
	boltDBDir := filepath.Dir(dbPath)
	if err := os.MkdirAll(boltDBDir, 0o755); err != nil {
		m.logger.Error().
			Err(err).
			Str("migration_id", record.ID).
			Str("bolt_db_dir", boltDBDir).
			Msg("failed to create BoltDB directory")
		if failTaskFunc != nil && taskID != "" {
			failTaskFunc(record.ID, taskID, err)
		}
		return
	}

	// Run ETL from DuckDB to BoltDB
	cfg := etl.DuckToBoltConfig{
		DuckDBPath:  duckdbPath,
		BoltPath:    dbPath,
		Overwrite:   true,
		RequireOpen: false, // We'll open it in standalone mode since we're creating a new file
	}
	if err := etl.RunDuckToBolt(cfg); err != nil {
		m.logger.Error().
			Err(err).
			Str("migration_id", record.ID).
			Msg("ETL from DuckDB to BoltDB failed")
		if failTaskFunc != nil && taskID != "" {
			failTaskFunc(record.ID, taskID, err)
		}
		return
	}

	m.logger.Info().
		Str("migration_id", record.ID).
		Str("bolt_db_path", dbPath).
		Msg("ETL from DuckDB to BoltDB completed successfully")

	// Complete background task
	if completeTaskFunc != nil && taskID != "" {
		completeTaskFunc(record.ID, taskID)
	}

	// Update status to Filters-Set after ETL completes (ready for retry sweep)
	// Note: yamlCfg and err are already declared above, so we reuse them
	yamlCfg, err = migration.LoadMigrationConfig(configPath)
	if err == nil {
		currentStatus := strings.TrimSpace(yamlCfg.State.Status)
		// Only update if we're in ETL-Duck-To-Bolt-In-Progress
		if currentStatus == "ETL-Duck-To-Bolt-In-Progress" {
			// For now, set state directly
			yamlCfg.State.Status = "Filters-Set"
			if err := migration.SaveMigrationConfig(configPath, yamlCfg); err != nil {
				m.logger.Warn().
					Err(err).
					Str("migration_id", record.ID).
					Msg("failed to update status to Filters-Set")
			} else {
				m.logger.Info().
					Str("migration_id", record.ID).
					Str("old_status", currentStatus).
					Str("new_status", "Filters-Set").
					Msg("updated status to Filters-Set after ETL (ready for retry sweep)")
			}
		}
	}

	// Clear ETL running flag BEFORE calling onComplete
	// This ensures the next ETL (BoltDB to DuckDB) can run immediately
	// The defer will also clear it, but clearing it here ensures sequential flow
	record.SetETLRunning(false)

	// Call the completion callback to trigger the next phase (discovery/copy)
	// This callback will run the retry sweep, then trigger ETL from BoltDB to DuckDB
	if onComplete != nil {
		onComplete()
	}

	m.logger.Info().
		Str("migration_id", record.ID).
		Msg("ETL from DuckDB to BoltDB completed, ready for next phase")
}
