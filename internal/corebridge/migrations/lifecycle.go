package migrations

import (
	"context"
	"fmt"
	"os"
	"time"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
	"codeberg.org/Sylos/Migration-Engine/pkg/migration"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge/metadata"
)

func (m *Manager) applyLogDefaults(opts *MigrationOptions) {
	if opts.LogAddress == "" {
		opts.LogAddress = m.cfg.Runtime.LogAddress
	}
	if opts.LogLevel == "" {
		opts.LogLevel = m.cfg.Runtime.LogLevel
	}
}

// registerNewRunningRecord registers a new running migration record.
// This is the stricter variant used by StartMigration: if an in-memory record already exists, it errors.
func (m *Manager) registerNewRunningRecord(migrationID string, record *MigrationRecord) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.migrations[migrationID]; exists {
		return fmt.Errorf("migration %s is already running", migrationID)
	}

	m.migrations[migrationID] = record
	if _, ok := m.subscribers[migrationID]; !ok {
		m.subscribers[migrationID] = make(map[string]chan ProgressEvent)
	}
	return nil
}

func (m *Manager) StartMigration(ctx context.Context, req StartMigrationRequest) (Migration, error) {
	migrationID := req.MigrationID
	if migrationID == "" {
		migrationID = req.Options.MigrationID
	}
	if migrationID == "" {
		return Migration{}, fmt.Errorf("migration id is required")
	}

	opts := req.Options
	opts.MigrationID = migrationID

	if opts.DatabasePath != "" {
		return m.startMigrationFromUploadedDB(ctx, migrationID, opts)
	}
	return m.startMigrationFromRootsPlan(ctx, migrationID, opts)
}

func (m *Manager) startMigrationFromUploadedDB(_ context.Context, migrationID string, opts MigrationOptions) (Migration, error) {
	// Pre-flight: ensure DB exists and roots are set
	if _, err := os.Stat(opts.DatabasePath); os.IsNotExist(err) {
		return Migration{}, fmt.Errorf("database file not found: %s", opts.DatabasePath)
	}

	options := db.Options{Path: opts.DatabasePath}
	database, err := db.Open(options)
	if err != nil {
		return Migration{}, fmt.Errorf("failed to open database: %w", err)
	}
	defer database.Close()

	status, err := migration.InspectMigrationStatus(database)
	if err != nil {
		return Migration{}, fmt.Errorf("failed to inspect database: %w", err)
	}
	if status.IsEmpty() {
		return Migration{}, fmt.Errorf("database is empty, roots must be set first")
	}

	opts.UsePreseededDB = true
	opts.RemoveExistingDB = false
	m.applyLogDefaults(&opts)
	m.logger.Info().Str("migration_id", migrationID).Str("database_path", opts.DatabasePath).Msg("starting migration from uploaded DB")

	if err := m.updateMetadataForMigration(migrationID, migrationID, opts.DatabasePath); err != nil {
		m.logger.Warn().Err(err).Str("migration_id", migrationID).Msg("failed to update metadata for uploaded DB migration")
	}

	sourceID := "uploaded-db-source"
	destinationID := "uploaded-db-destination"
	if plan := m.rootsMgr.GetPlan(migrationID); plan != nil {
		sourceID = plan.SourceDefinition.ID
		destinationID = plan.DestinationDefinition.ID
	}
	record := &MigrationRecord{
		ID:            migrationID,
		SourceID:      sourceID,
		DestinationID: destinationID,
		Status:        MigrationStatusRunning,
		StartedAt:     time.Now().UTC(),
	}
	if err := m.registerNewRunningRecord(migrationID, record); err != nil {
		return Migration{}, err
	}

	m.publishProgress(record.ID, "started", nil, nil)

	go m.runMigrationEngine(record, RunParams{
		FromRoots: &RunParamsFromRoots{
			DbPath:            opts.DatabasePath,
			Opts:              opts,
		},
	})

	return Migration{
		ID:            record.ID,
		SourceID:      record.SourceID,
		DestinationID: record.DestinationID,
		StartedAt:     record.StartedAt,
		Status:        record.Status,
	}, nil
}

func (m *Manager) startMigrationFromRootsPlan(_ context.Context, migrationID string, opts MigrationOptions) (Migration, error) {
	// Pre-flight: ensure service definitions and roots are set
	plan := m.rootsMgr.GetPlan(migrationID)
	if plan == nil {
		return Migration{}, fmt.Errorf("roots not set for migration %s and no databasePath provided", migrationID)
	}
	if !plan.HasSource || !plan.HasDestination {
		return Migration{}, fmt.Errorf("roots not fully configured for migration %s", migrationID)
	}

	// Resolve DB path (one place)
	if plan.Seeded {
		opts.DatabasePath = plan.DatabasePath
	} else {
		dbPath, err := m.resolveDBPath("", migrationID)
		if err != nil {
			return Migration{}, fmt.Errorf("failed to resolve database path: %w", err)
		}
		opts.DatabasePath = dbPath
	}

	// Log start/resume
	metaMgr := metadata.NewManager(m.cfg.Runtime.DataDir)
	meta, err := metaMgr.GetMigrationMetadata(migrationID)
	isNewMigration := true
	if err == nil {
		isNewMigration = meta.IsNewMigration
	} else {
		meta = metadata.MigrationMetadata{
			ID:             migrationID,
			Name:           migrationID,
			DatabasePath:   opts.DatabasePath,
			IsNewMigration: true,
		}
	}

	if isNewMigration {
		opts.UsePreseededDB = false
		opts.RemoveExistingDB = false
		m.logger.Info().Str("migration_id", migrationID).Msg("starting new migration")
	} else {
		opts.UsePreseededDB = true
		opts.RemoveExistingDB = false
		m.logger.Info().Str("migration_id", migrationID).Msg("resuming existing migration")
	}

	m.applyLogDefaults(&opts)

	// Metadata
	if err := m.updateMetadataForMigration(migrationID, migrationID, opts.DatabasePath); err != nil {
		m.logger.Warn().Err(err).Str("migration_id", migrationID).Msg("failed to update migration metadata")
	}

	if isNewMigration {
		meta.IsNewMigration = false
		if err := metaMgr.UpdateMigrationMetadata(meta); err != nil {
			m.logger.Warn().Err(err).Str("migration_id", migrationID).Msg("failed to clear IsNewMigration flag")
		}
	}

	// Register record and publish started
	record := &MigrationRecord{
		ID:            migrationID,
		SourceID:      plan.SourceDefinition.ID,
		DestinationID: plan.DestinationDefinition.ID,
		Status:        MigrationStatusRunning,
		StartedAt:     time.Now().UTC(),
	}
	if err := m.registerNewRunningRecord(migrationID, record); err != nil {
		return Migration{}, err
	}

	m.publishProgress(record.ID, "started", nil, nil)

	go m.runMigrationEngine(record, RunParams{
		FromRoots: &RunParamsFromRoots{
			DbPath:            opts.DatabasePath,
			Opts:              opts,
		},
	})

	return Migration{
		ID:            record.ID,
		SourceID:      record.SourceID,
		DestinationID: record.DestinationID,
		StartedAt:     record.StartedAt,
		Status:        record.Status,
	}, nil
}

// StopMigration triggers a programmatic shutdown of a running migration
func (m *Manager) StopMigration(ctx context.Context, id string) (*migration.Result, error) {
	m.mu.RLock()
	record, ok := m.migrations[id]
	if !ok {
		m.mu.RUnlock()
		return nil, ErrMigrationNotFound
	}

	if record.Controller == nil {
		m.mu.RUnlock()
		return nil, fmt.Errorf("migration %s is not running (no controller)", id)
	}

	controller := record.Controller
	m.mu.RUnlock()

	m.logger.Info().
		Str("migration_id", id).
		Msg("triggering migration shutdown (killswitch)")

	controller.Shutdown()

	result, err := controller.Wait()

	if err != nil && err.Error() == "migration suspended by force shutdown" {
		m.logger.Info().
			Str("migration_id", id).
			Msg("migration suspended successfully (killswitch)")

		finished := time.Now().UTC()
		m.mu.Lock()
		record.Status = MigrationStatusSuspended
		record.CompletedAt = &finished
		record.Result = &result
		record.Controller = nil
		m.mu.Unlock()

		srcStats := result.Runtime.Src
		dstStats := result.Runtime.Dst
		m.publishProgress(id, "suspended", &srcStats, &dstStats)

		return &result, nil
	}

	if err != nil {
		m.logger.Error().
			Err(err).
			Str("migration_id", id).
			Msg("migration shutdown failed")

		finished := time.Now().UTC()
		m.mu.Lock()
		record.Status = MigrationStatusFailed
		record.Error = err.Error()
		record.CompletedAt = &finished
		record.Controller = nil
		m.mu.Unlock()

		m.publishProgress(id, "failed", nil, nil)
		return nil, fmt.Errorf("migration shutdown failed: %w", err)
	}

	finished := time.Now().UTC()
	m.mu.Lock()
	record.Status = MigrationStatusCompleted
	record.CompletedAt = &finished
	record.Result = &result
	record.Controller = nil
	m.mu.Unlock()

	srcStats := result.Runtime.Src
	dstStats := result.Runtime.Dst
	m.publishProgress(id, "completed", &srcStats, &dstStats)

	return &result, nil
}

// LoadMigrationFromConfigPath remains for backward compatibility. In DB-only mode,
// the provided path is treated as a DB path.
func (m *Manager) LoadMigrationFromConfigPath(ctx context.Context, migrationID, configPath string) (Migration, error) {
	opts := MigrationOptions{
		MigrationID:      migrationID,
		DatabasePath:     configPath,
		UsePreseededDB:   true,
		RemoveExistingDB: false,
		LogAddress:       m.cfg.Runtime.LogAddress,
		LogLevel:         m.cfg.Runtime.LogLevel,
	}
	return m.startMigrationFromUploadedDB(ctx, migrationID, opts)
}
