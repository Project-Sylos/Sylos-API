package migrations

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
	"codeberg.org/Sylos/Migration-Engine/pkg/migration"
	corebridgeDB "codeberg.org/Sylos/Sylos-API/internal/corebridge/database"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge/metadata"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge/services"
)

// checkYAMLStatus checks the status from the YAML config file and returns an error if migration cannot be started
// Returns nil if migration can proceed (suspended, failed, or no YAML found), error if it cannot (running, completed)
func (m *Manager) checkYAMLStatus(migrationID string, configPath string) error {
	if _, err := os.Stat(configPath); os.IsNotExist(err) {
		return nil
	}

	yamlCfg, err := migration.LoadMigrationConfig(configPath)
	if err != nil {
		return nil
	}

	status := strings.ToLower(strings.TrimSpace(yamlCfg.State.Status))

	switch status {
	case "running":
		return fmt.Errorf("migration %s is already running", migrationID)
	case "completed":
		return fmt.Errorf("migration %s has already completed", migrationID)
	case "suspended", "failed":
		return nil
	default:
		return nil
	}
}

func (m *Manager) applyLogDefaults(opts *MigrationOptions) {
	if opts.LogAddress == "" {
		opts.LogAddress = m.cfg.Runtime.LogAddress
	}
	if opts.LogLevel == "" {
		opts.LogLevel = m.cfg.Runtime.LogLevel
	}
}

// setYAMLStatusRunning sets the migration YAML config status to "Running" for traversal phase.
func (m *Manager) setYAMLStatusRunning(configPath string) {
	if configPath == "" {
		return
	}
	yamlCfg, err := migration.LoadMigrationConfig(configPath)
	if err != nil {
		m.logger.Warn().Err(err).Str("config_path", configPath).Msg("failed to load config to set Running status")
		return
	}
	yamlCfg.State.Status = "Running"
	if err := migration.SaveMigrationConfig(configPath, yamlCfg); err != nil {
		m.logger.Warn().Err(err).Str("config_path", configPath).Msg("failed to set YAML status to Running")
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

	configPath := corebridgeDB.ConfigPathFromDatabasePath(opts.DatabasePath)

	overridePath, exists, _ := services.LoadSpectraConfigOverride(m.cfg.Runtime.DataDir, migrationID)
	var spectraConfigPath string
	if exists {
		spectraConfigPath = overridePath
	}

	if err := m.checkYAMLStatus(migrationID, configPath); err != nil {
		return Migration{}, err
	}

	opts.UsePreseededDB = true
	opts.RemoveExistingDB = false
	m.applyLogDefaults(&opts)

	yamlCfg, err := migration.LoadMigrationConfig(configPath)
	if err == nil {
		// Pre-flight: acquire adapters and load full config (from YAML)
		srcAdapter, dstAdapter, shared, err := m.acquireSharedSpectraAdapters(yamlCfg.Services.Source, yamlCfg.Services.Destination, spectraConfigPath)
		if err != nil {
			return Migration{}, fmt.Errorf("failed to acquire shared adapters: %w", err)
		}

		if !shared {
			srcAdapter, err = m.acquireAdapterFromYAMLConfig(yamlCfg.Services.Source, spectraConfigPath)
			if err != nil {
				return Migration{}, fmt.Errorf("failed to acquire source adapter: %w", err)
			}

			dstAdapter, err = m.acquireAdapterFromYAMLConfig(yamlCfg.Services.Destination, spectraConfigPath)
			if err != nil {
				if srcAdapter != nil {
					if closer, ok := srcAdapter.(interface{ Close() error }); ok {
						_ = closer.Close()
					}
				}
				return Migration{}, fmt.Errorf("failed to acquire destination adapter: %w", err)
			}
		}

		cfg, err := migration.LoadMigrationConfigFromYAML(configPath, srcAdapter, dstAdapter)
		if err != nil {
			return Migration{}, fmt.Errorf("failed to load migration config: %w", err)
		}

		cfg.Database.RemoveExisting = false
		cfg.SeedRoots = true

		m.logger.Info().Str("migration_id", migrationID).Msg("resuming migration from uploaded DB config")

		m.setYAMLStatusRunning(configPath)

		record := &MigrationRecord{
			ID:            migrationID,
			SourceID:      cfg.Source.Name,
			DestinationID: cfg.Destination.Name,
			Status:        MigrationStatusRunning,
			StartedAt:     time.Now().UTC(),
		}
		if err := m.registerNewRunningRecord(migrationID, record); err != nil {
			return Migration{}, err
		}

		m.publishProgress(record.ID, "started", nil, nil)

		go m.runMigrationEngine(record, RunParams{
			FromConfig: &RunParamsFromConfig{Cfg: cfg, Opts: opts},
		})

		return Migration{
			ID:            record.ID,
			SourceID:      record.SourceID,
			DestinationID: record.DestinationID,
			StartedAt:     record.StartedAt,
			Status:        record.Status,
		}, nil
	}

	// Uploaded DB without YAML: stub defs, run from roots path (plan may be nil and runner will fail)
	if _, err := os.Stat(configPath); err == nil {
		if err := m.checkYAMLStatus(migrationID, configPath); err != nil {
			return Migration{}, err
		}
	}

	m.logger.Info().Str("migration_id", migrationID).Msg("starting migration from uploaded DB")

	record := &MigrationRecord{
		ID:            migrationID,
		SourceID:      "uploaded-db-source",
		DestinationID: "uploaded-db-destination",
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
			ConfigPath:        configPath,
			Opts:              opts,
			SpectraConfigPath: "",
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

	configPath := corebridgeDB.ConfigPathFromDatabasePath(opts.DatabasePath)

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

	// Spectra override and connection IDs
	var spectraConfigPath string
	isSpectraMigration := plan.SourceDefinition.Type == services.ServiceTypeSpectra || plan.DestinationDefinition.Type == services.ServiceTypeSpectra
	bothSpectra := plan.SourceDefinition.Type == services.ServiceTypeSpectra && plan.DestinationDefinition.Type == services.ServiceTypeSpectra

	if isSpectraMigration {
		var spectraDef services.ServiceDefinition
		if plan.SourceDefinition.Type == services.ServiceTypeSpectra {
			spectraDef = plan.SourceDefinition
		} else {
			spectraDef = plan.DestinationDefinition
		}

		existingOverride, exists, err := services.LoadSpectraConfigOverride(m.cfg.Runtime.DataDir, migrationID)
		if err != nil {
			return Migration{}, fmt.Errorf("failed to check for existing Spectra config override: %w", err)
		}

		if exists {
			spectraConfigPath = existingOverride
		} else {
			overridePath, err := services.SaveSpectraConfigOverride(m.cfg.Runtime.DataDir, migrationID, spectraDef.Spectra.ConfigPath)
			if err != nil {
				return Migration{}, fmt.Errorf("failed to create Spectra config override: %w", err)
			}
			spectraConfigPath = overridePath
		}

		if bothSpectra {
			sharedConnectionID := fmt.Sprintf("spectra-%s", migrationID)
			opts.SourceConnectionID = sharedConnectionID
			opts.DestinationConnectionID = sharedConnectionID
		} else {
			if opts.SourceConnectionID == "" {
				opts.SourceConnectionID = plan.SourceConnectionID
			}
			if opts.DestinationConnectionID == "" {
				opts.DestinationConnectionID = plan.DestinationConnectionID
			}
		}
	} else {
		if opts.SourceConnectionID == "" {
			opts.SourceConnectionID = plan.SourceConnectionID
		}
		if opts.DestinationConnectionID == "" {
			opts.DestinationConnectionID = plan.DestinationConnectionID
		}
	}

	m.applyLogDefaults(&opts)

	// Config path and YAML checks
	if _, err := os.Stat(configPath); err == nil {
		if err := m.checkYAMLStatus(migrationID, configPath); err != nil {
			return Migration{}, err
		}
	}

	// Metadata
	if err := m.updateMetadataForMigration(migrationID, migrationID, configPath); err != nil {
		m.logger.Warn().Err(err).Str("migration_id", migrationID).Msg("failed to update migration metadata")
	}

	if isNewMigration {
		meta.IsNewMigration = false
		if err := metaMgr.UpdateMigrationMetadata(meta); err != nil {
			m.logger.Warn().Err(err).Str("migration_id", migrationID).Msg("failed to clear IsNewMigration flag")
		}
	}

	m.setYAMLStatusRunning(configPath)

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
			ConfigPath:        configPath,
			Opts:              opts,
			SpectraConfigPath: spectraConfigPath,
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

// LoadMigrationFromConfigPath loads and resumes a migration from its YAML config file path
func (m *Manager) LoadMigrationFromConfigPath(ctx context.Context, migrationID, configPath string) (Migration, error) {
	if _, err := os.Stat(configPath); os.IsNotExist(err) {
		return Migration{}, fmt.Errorf("migration config file not found: %s", configPath)
	}

	if err := m.checkYAMLStatus(migrationID, configPath); err != nil {
		return Migration{}, err
	}

	overridePath, exists, _ := services.LoadSpectraConfigOverride(m.cfg.Runtime.DataDir, migrationID)
	var spectraConfigPath string
	if exists {
		spectraConfigPath = overridePath
	}

	yamlCfg, err := migration.LoadMigrationConfig(configPath)
	if err != nil {
		return Migration{}, fmt.Errorf("failed to load YAML config: %w", err)
	}

	srcAdapter, dstAdapter, shared, err := m.acquireSharedSpectraAdapters(yamlCfg.Services.Source, yamlCfg.Services.Destination, spectraConfigPath)
	if err != nil {
		return Migration{}, fmt.Errorf("failed to acquire shared adapters: %w", err)
	}

	if !shared {
		srcAdapter, err = m.acquireAdapterFromYAMLConfig(yamlCfg.Services.Source, spectraConfigPath)
		if err != nil {
			return Migration{}, fmt.Errorf("failed to acquire source adapter: %w", err)
		}

		dstAdapter, err = m.acquireAdapterFromYAMLConfig(yamlCfg.Services.Destination, spectraConfigPath)
		if err != nil {
			if srcAdapter != nil {
				if closer, ok := srcAdapter.(interface{ Close() error }); ok {
					_ = closer.Close()
				}
			}
			return Migration{}, fmt.Errorf("failed to acquire destination adapter: %w", err)
		}
	}

	cfg, err := migration.LoadMigrationConfigFromYAML(configPath, srcAdapter, dstAdapter)
	if err != nil {
		return Migration{}, fmt.Errorf("failed to load migration config: %w", err)
	}

	dbPath := cfg.Database.Path
	if dbPath == "" {
		return Migration{}, fmt.Errorf("migration config has no database path")
	}

	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		return Migration{}, fmt.Errorf("migration database file not found: %s", dbPath)
	}

	cfg.Database.RemoveExisting = false
	cfg.SeedRoots = true

	opts := MigrationOptions{
		MigrationID:      migrationID,
		DatabasePath:     dbPath,
		UsePreseededDB:   true,
		RemoveExistingDB: false,
		LogAddress:       m.cfg.Runtime.LogAddress,
		LogLevel:         m.cfg.Runtime.LogLevel,
	}

	record := &MigrationRecord{
		ID:            migrationID,
		SourceID:      cfg.Source.Name,
		DestinationID: cfg.Destination.Name,
		Status:        MigrationStatusRunning,
		StartedAt:     time.Now().UTC(),
	}

	if err := m.registerNewRunningRecord(migrationID, record); err != nil {
		return Migration{}, err
	}

	m.publishProgress(record.ID, "started", nil, nil)

	go m.runMigrationEngine(record, RunParams{
		FromConfig: &RunParamsFromConfig{Cfg: cfg, Opts: opts},
	})

	return Migration{
		ID:            record.ID,
		SourceID:      record.SourceID,
		DestinationID: record.DestinationID,
		StartedAt:     record.StartedAt,
		Status:        record.Status,
	}, nil
}
