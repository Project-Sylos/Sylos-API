package migrations

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/Project-Sylos/Migration-Engine/pkg/db"
	"github.com/Project-Sylos/Migration-Engine/pkg/logservice"
	"github.com/Project-Sylos/Migration-Engine/pkg/migration"
	"github.com/Project-Sylos/Spectra/sdk"
	corebridgeDB "github.com/Project-Sylos/Sylos-API/internal/corebridge/database"
	"github.com/Project-Sylos/Sylos-API/internal/corebridge/metadata"
	"github.com/Project-Sylos/Sylos-API/internal/corebridge/roots"
	"github.com/Project-Sylos/Sylos-API/internal/corebridge/services"
	"github.com/Project-Sylos/Sylos-API/pkg/config"
	fslib "github.com/Project-Sylos/Sylos-FS/pkg/fs"
	fstypes "github.com/Project-Sylos/Sylos-FS/pkg/types"
	"github.com/rs/zerolog"
)

// MigrationOptions represents options for a migration
type MigrationOptions struct {
	MigrationID             string
	DatabasePath            string
	RemoveExistingDB        bool
	UsePreseededDB          bool
	SourceConnectionID      string
	DestinationConnectionID string
	WorkerCount             int
	MaxRetries              int
	CoordinatorLead         int
	LogAddress              string
	LogLevel                string
	EnableLoggingTerminal   bool
	StartupDelaySec         int
	ProgressTickMillis      int
	Verification            VerificationOptions
}

// VerificationOptions represents verification options
type VerificationOptions struct {
	AllowPending  bool
	AllowFailed   bool
	AllowNotOnSrc bool
}

// StartMigrationRequest represents a request to start a migration
type StartMigrationRequest struct {
	MigrationID string
	Options     MigrationOptions
}

// Migration represents a migration
type Migration struct {
	ID            string
	SourceID      string
	DestinationID string
	StartedAt     time.Time
	Status        string
}

// Status represents migration status
type Status struct {
	Migration
	CompletedAt *time.Time
	Error       string
	Result      *ResultView
}

// ResultView represents a result view
type ResultView struct {
	RootSummary  RootSummaryView
	Runtime      RuntimeStatsView
	Verification VerificationView
}

// RootSummaryView represents root summary
type RootSummaryView struct {
	SrcRoots int
	DstRoots int
}

// RuntimeStatsView represents runtime statistics
type RuntimeStatsView struct {
	Duration string
	Src      QueueStatsView
	Dst      QueueStatsView
}

// VerificationView represents verification view
type VerificationView struct {
	SrcTotal    int
	DstTotal    int
	SrcPending  int
	DstPending  int
	SrcFailed   int
	DstFailed   int
	DstNotOnSrc int
}

// QueueStatsView represents queue statistics
type QueueStatsView struct {
	Name         string
	Round        int
	Pending      int
	InProgress   int
	TotalTracked int
	Workers      int
}

// QueueStatsSnapshot represents a snapshot of queue statistics
type QueueStatsSnapshot struct {
	Round        int
	Pending      int
	InProgress   int
	TotalTracked int
	Workers      int
}

// ProgressEvent represents a progress event
type ProgressEvent struct {
	Event       string
	Timestamp   time.Time
	Migration   Status
	Source      QueueStatsSnapshot
	Destination QueueStatsSnapshot
}

var ErrMigrationNotFound = fmt.Errorf("migration not found")

// Manager handles migration-related operations
type Manager struct {
	logger        zerolog.Logger
	cfg           config.Config
	migrations    map[string]*MigrationRecord
	subscribers   map[string]map[string]chan ProgressEvent
	mu            sync.RWMutex
	serviceMgr    *services.ServiceManager
	rootsMgr      *roots.Manager
	metadataMgr   *metadata.Manager
	resolveDBPath func(path, migrationID string) (string, error)
}

type MigrationRecord struct {
	ID            string
	SourceID      string
	DestinationID string
	Status        string
	StartedAt     time.Time
	CompletedAt   *time.Time
	Result        *migration.Result
	Error         string
	Controller    *migration.MigrationController // Controller for programmatic shutdown
}

const (
	MigrationStatusRunning   = "running"
	MigrationStatusCompleted = "completed"
	MigrationStatusSuspended = "suspended"
	MigrationStatusFailed    = "failed"
)

func NewManager(logger zerolog.Logger, cfg config.Config, serviceMgr *services.ServiceManager, rootsMgr *roots.Manager, resolveDBPath func(path, migrationID string) (string, error)) *Manager {
	return &Manager{
		logger:        logger,
		cfg:           cfg,
		migrations:    make(map[string]*MigrationRecord),
		subscribers:   make(map[string]map[string]chan ProgressEvent),
		serviceMgr:    serviceMgr,
		rootsMgr:      rootsMgr,
		metadataMgr:   metadata.NewManager(cfg.Runtime.DataDir),
		resolveDBPath: resolveDBPath,
	}
}

// checkYAMLStatus checks the status from the YAML config file and returns an error if migration cannot be started
// Returns nil if migration can proceed (suspended, failed, or no YAML found), error if it cannot (running, completed)
func (m *Manager) checkYAMLStatus(migrationID string, configPath string) error {
	// Check if config file exists
	if _, err := os.Stat(configPath); os.IsNotExist(err) {
		// No YAML file - migration can proceed (new migration)
		return nil
	}

	// Load the YAML config directly (without adapters) to check status
	yamlCfg, err := migration.LoadMigrationConfig(configPath)
	if err != nil {
		// If we can't load the YAML, allow migration to proceed (might be corrupted, will be overwritten)
		return nil
	}

	// Check the state.status field from the loaded YAML config
	status := strings.ToLower(strings.TrimSpace(yamlCfg.State.Status))

	switch status {
	case "running":
		return fmt.Errorf("migration %s is already running", migrationID)
	case "completed":
		return fmt.Errorf("migration %s has already completed", migrationID)
	case "suspended", "failed":
		// Suspended and failed migrations can be rerun
		return nil
	default:
		// Unknown status or empty - allow migration to proceed
		return nil
	}
}

// LoadMigrationFromConfigPath loads and resumes a migration from its YAML config file path
func (m *Manager) LoadMigrationFromConfigPath(ctx context.Context, migrationID, configPath string) (Migration, error) {
	// Check if config file exists
	if _, err := os.Stat(configPath); os.IsNotExist(err) {
		return Migration{}, fmt.Errorf("migration config file not found: %s", configPath)
	}

	// Check YAML status before proceeding
	if err := m.checkYAMLStatus(migrationID, configPath); err != nil {
		return Migration{}, err
	}

	// Check if Spectra override config exists
	overridePath, exists, _ := services.LoadSpectraConfigOverride(m.cfg.Runtime.DataDir, migrationID)
	var spectraConfigPath string
	if exists {
		spectraConfigPath = overridePath
	}

	// Load YAML config and reconstruct migration config
	adapterFactory := m.createAdapterFactory(spectraConfigPath)
	cfg, err := migration.LoadMigrationConfigFromYAML(configPath, adapterFactory)
	if err != nil {
		return Migration{}, fmt.Errorf("failed to load migration config: %w", err)
	}

	// Extract database path from config
	dbPath := cfg.Database.Path
	if dbPath == "" {
		return Migration{}, fmt.Errorf("migration config has no database path")
	}

	// Validate that the DB file exists
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		return Migration{}, fmt.Errorf("migration database file not found: %s", dbPath)
	}

	// CRITICAL: Force RemoveExisting to false for resumption (same as test logic)
	// The YAML might have remove_existing: true, but we must never remove existing DB when resuming
	cfg.Database.RemoveExisting = false

	// CRITICAL: Ensure SeedRoots is set (same as test logic)
	// StartMigration will only use it if DB is empty, so it's safe to set to true
	// The YAML might have seed_roots: false, but we should set it to true for consistency
	cfg.SeedRoots = true

	// Create migration options with defaults
	opts := MigrationOptions{
		MigrationID:      migrationID,
		DatabasePath:     dbPath,
		UsePreseededDB:   true,
		RemoveExistingDB: false,
		LogAddress:       m.cfg.Runtime.LogAddress,
		LogLevel:         m.cfg.Runtime.LogLevel,
	}

	// Create migration record
	record := &MigrationRecord{
		ID:            migrationID,
		SourceID:      cfg.Source.Name,
		DestinationID: cfg.Destination.Name,
		Status:        MigrationStatusRunning,
		StartedAt:     time.Now().UTC(),
	}

	m.mu.Lock()
	existing, exists := m.migrations[migrationID]
	if exists {
		// Check if the existing migration is actually running (has controller or status is running)
		if existing.Controller != nil || existing.Status == MigrationStatusRunning {
			m.mu.Unlock()
			return Migration{}, fmt.Errorf("migration %s is already running", migrationID)
		}
		// If it's suspended or failed, we can resume it - remove the old record
		// The YAML status check already validated it's safe to resume
	}
	m.migrations[migrationID] = record
	if _, ok := m.subscribers[migrationID]; !ok {
		m.subscribers[migrationID] = make(map[string]chan ProgressEvent)
	}
	m.mu.Unlock()

	m.publishProgress(record.ID, "started", nil, nil)

	// Resume migration using reconstructed config
	go m.RunMigrationFromConfig(
		record,
		cfg,
		opts,
		spectraConfigPath,
	)

	return Migration{
		ID:            record.ID,
		SourceID:      record.SourceID,
		DestinationID: record.DestinationID,
		StartedAt:     record.StartedAt,
		Status:        record.Status,
	}, nil
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

	// If databasePath is provided, we can start migration directly from uploaded DB
	// Otherwise, we need roots to be set
	if opts.DatabasePath != "" {
		// Validate that the DB file exists
		if _, err := os.Stat(opts.DatabasePath); os.IsNotExist(err) {
			return Migration{}, fmt.Errorf("database file not found: %s", opts.DatabasePath)
		}

		// Check if DB has valid schema and get roots from it
		options := db.Options{
			Path: opts.DatabasePath,
		}
		database, err := db.Open(options)
		if err != nil {
			return Migration{}, fmt.Errorf("failed to open database: %w", err)
		}
		defer database.Close()

		if err := database.ValidateCoreSchema(); err != nil {
			return Migration{}, fmt.Errorf("database schema invalid: %w", err)
		}

		// Get status to check if DB has roots
		status, err := migration.InspectMigrationStatus(database)
		if err != nil {
			return Migration{}, fmt.Errorf("failed to inspect database: %w", err)
		}

		if status.IsEmpty() {
			return Migration{}, fmt.Errorf("database is empty, roots must be set first")
		}

		// Try to load Migration Engine YAML config to restore service info and Spectra config
		configPath := corebridgeDB.ConfigPathFromDatabasePath(opts.DatabasePath)

		// Check if Spectra override config exists
		overridePath, exists, _ := services.LoadSpectraConfigOverride(m.cfg.Runtime.DataDir, migrationID)
		var spectraConfigPath string
		if exists {
			spectraConfigPath = overridePath
		}

		// Check YAML status before proceeding
		if err := m.checkYAMLStatus(migrationID, configPath); err != nil {
			return Migration{}, err
		}

		// Try loading YAML config to reconstruct migration config
		adapterFactory := m.createAdapterFactory(spectraConfigPath)
		cfg, err := migration.LoadMigrationConfigFromYAML(configPath, adapterFactory)
		if err == nil {
			// Successfully reconstructed config - use it to resume migration
			// The config already has all service info, roots, etc.

			// CRITICAL: Force RemoveExisting to false for resumption (same as test logic)
			// The YAML might have remove_existing: true, but we must never remove existing DB when resuming
			cfg.Database.RemoveExisting = false

			// CRITICAL: Ensure SeedRoots is set (same as test logic)
			// StartMigration will only use it if DB is empty, so it's safe to set to true
			cfg.SeedRoots = true

			opts.UsePreseededDB = true
			opts.RemoveExistingDB = false
			if opts.LogAddress == "" {
				opts.LogAddress = m.cfg.Runtime.LogAddress
			}
			if opts.LogLevel == "" {
				opts.LogLevel = m.cfg.Runtime.LogLevel
			}

			record := &MigrationRecord{
				ID:            migrationID,
				SourceID:      cfg.Source.Name,
				DestinationID: cfg.Destination.Name,
				Status:        MigrationStatusRunning,
				StartedAt:     time.Now().UTC(),
			}

			m.mu.Lock()
			if _, exists := m.migrations[migrationID]; exists {
				m.mu.Unlock()
				return Migration{}, fmt.Errorf("migration %s is already running", migrationID)
			}
			m.migrations[migrationID] = record
			if _, ok := m.subscribers[migrationID]; !ok {
				m.subscribers[migrationID] = make(map[string]chan ProgressEvent)
			}
			m.mu.Unlock()

			m.publishProgress(record.ID, "started", nil, nil)

			// Resume migration using reconstructed config
			go m.RunMigrationFromConfig(
				record,
				cfg,
				opts,
				spectraConfigPath,
			)

			return Migration{
				ID:            record.ID,
				SourceID:      record.SourceID,
				DestinationID: record.DestinationID,
				StartedAt:     record.StartedAt,
				Status:        record.Status,
			}, nil
		}
		// If reconstruction failed, fall through to generic values

		// No YAML config found or reconstruction failed - use generic values for uploaded DB
		srcDef := services.ServiceDefinition{
			ID:   "uploaded-db-source",
			Name: "Uploaded DB Source",
			Type: services.ServiceTypeLocal,
		}
		dstDef := services.ServiceDefinition{
			ID:   "uploaded-db-destination",
			Name: "Uploaded DB Destination",
			Type: services.ServiceTypeLocal,
		}

		// Create minimal folder descriptors (not used when resuming from existing DB)
		srcFolder := fstypes.Folder{Id: "root", LocationPath: "/"}
		dstFolder := fstypes.Folder{Id: "root", LocationPath: "/"}

		opts.UsePreseededDB = true
		opts.RemoveExistingDB = false
		if opts.LogAddress == "" {
			opts.LogAddress = m.cfg.Runtime.LogAddress
		}
		if opts.LogLevel == "" {
			opts.LogLevel = m.cfg.Runtime.LogLevel
		}

		// Check YAML status if config exists
		if _, err := os.Stat(configPath); err == nil {
			if err := m.checkYAMLStatus(migrationID, configPath); err != nil {
				return Migration{}, err
			}
		}

		record := &MigrationRecord{
			ID:            migrationID,
			SourceID:      srcDef.ID,
			DestinationID: dstDef.ID,
			Status:        MigrationStatusRunning,
			StartedAt:     time.Now().UTC(),
		}

		m.mu.Lock()
		if _, exists := m.migrations[migrationID]; exists {
			m.mu.Unlock()
			return Migration{}, fmt.Errorf("migration %s is already running", migrationID)
		}
		m.migrations[migrationID] = record
		if _, ok := m.subscribers[migrationID]; !ok {
			m.subscribers[migrationID] = make(map[string]chan ProgressEvent)
		}
		m.mu.Unlock()

		m.publishProgress(record.ID, "started", nil, nil)

		// Migration Engine SDK will spawn log terminal automatically when SkipListener is false
		go m.RunMigration(
			record,
			srcDef,
			dstDef,
			srcFolder,
			dstFolder,
			opts,
			"", // No override config for uploaded DB without metadata
		)

		return Migration{
			ID:            record.ID,
			SourceID:      record.SourceID,
			DestinationID: record.DestinationID,
			StartedAt:     record.StartedAt,
			Status:        record.Status,
		}, nil
	}

	// Original flow: require roots to be set
	plan := m.rootsMgr.GetPlan(migrationID)
	if plan == nil {
		return Migration{}, fmt.Errorf("roots not set for migration %s and no databasePath provided", migrationID)
	}

	if !plan.HasSource || !plan.HasDestination {
		return Migration{}, fmt.Errorf("roots not fully configured for migration %s", migrationID)
	}

	if !plan.Seeded {
		if _, _, _, err := m.rootsMgr.SeedPlanIfReady(migrationID); err != nil {
			return Migration{}, err
		}
		plan = m.rootsMgr.GetPlan(migrationID)
		if plan == nil || !plan.Seeded {
			return Migration{}, fmt.Errorf("roots not fully configured for migration %s", migrationID)
		}
	}

	opts.DatabasePath = plan.DatabasePath

	// Check if this is a new migration (not a resume)
	// If IsNewMigration is true, we should start fresh (remove existing DB)
	metaMgr := metadata.NewManager(m.cfg.Runtime.DataDir)
	meta, err := metaMgr.GetMigrationMetadata(migrationID)
	isNewMigration := true
	if err == nil {
		isNewMigration = meta.IsNewMigration
	} else {
		// Metadata doesn't exist yet - treat as new migration
		meta = metadata.MigrationMetadata{
			ID:             migrationID,
			Name:           migrationID,
			IsNewMigration: true,
		}
	}

	if isNewMigration {
		// New migration - start fresh
		opts.UsePreseededDB = false
		opts.RemoveExistingDB = false // Always false - anti-pattern to remove existing DB
		m.logger.Info().Str("migration_id", migrationID).Msg("starting new migration")
	} else {
		// Resume existing migration - use existing DB
		opts.UsePreseededDB = true
		opts.RemoveExistingDB = false
		m.logger.Info().Str("migration_id", migrationID).Msg("resuming existing migration")
	}

	// Check if Spectra is used and create config override if needed
	var spectraConfigPath string
	isSpectraMigration := plan.SourceDefinition.Type == services.ServiceTypeSpectra || plan.DestinationDefinition.Type == services.ServiceTypeSpectra
	bothSpectra := plan.SourceDefinition.Type == services.ServiceTypeSpectra && plan.DestinationDefinition.Type == services.ServiceTypeSpectra

	if isSpectraMigration {
		// Get the Spectra service definition (same for both src and dst)
		var spectraDef services.ServiceDefinition
		if plan.SourceDefinition.Type == services.ServiceTypeSpectra {
			spectraDef = plan.SourceDefinition
		} else {
			spectraDef = plan.DestinationDefinition
		}

		// Create Spectra config override
		overridePath, err := services.SaveSpectraConfigOverride(m.cfg.Runtime.DataDir, migrationID, spectraDef.Spectra.ConfigPath)
		if err != nil {
			return Migration{}, fmt.Errorf("failed to create Spectra config override: %w", err)
		}
		spectraConfigPath = overridePath

		// If both source and destination are Spectra, they must share the same connection ID
		// to use the same underlying SpectraFS instance (prevents BoltDB lock conflicts)
		if bothSpectra {
			// Generate a shared connection ID based on migration ID and config path
			// This ensures both adapters use the same SpectraFS instance
			sharedConnectionID := fmt.Sprintf("spectra-%s", migrationID)
			opts.SourceConnectionID = sharedConnectionID
			opts.DestinationConnectionID = sharedConnectionID
		} else {
			// Only one is Spectra, use individual connection IDs if provided
			if opts.SourceConnectionID == "" {
				opts.SourceConnectionID = plan.SourceConnectionID
			}
			if opts.DestinationConnectionID == "" {
				opts.DestinationConnectionID = plan.DestinationConnectionID
			}
		}
	} else {
		// Neither is Spectra, use individual connection IDs if provided
		if opts.SourceConnectionID == "" {
			opts.SourceConnectionID = plan.SourceConnectionID
		}
		if opts.DestinationConnectionID == "" {
			opts.DestinationConnectionID = plan.DestinationConnectionID
		}
	}

	if opts.LogAddress == "" {
		opts.LogAddress = m.cfg.Runtime.LogAddress
	}
	if opts.LogLevel == "" {
		opts.LogLevel = m.cfg.Runtime.LogLevel
	}

	// Create Migration Engine YAML config file
	configPath := corebridgeDB.ConfigPathFromDatabasePath(plan.DatabasePath)

	// Check YAML status before proceeding (if YAML exists)
	if _, err := os.Stat(configPath); err == nil {
		if err := m.checkYAMLStatus(migrationID, configPath); err != nil {
			return Migration{}, err
		}
	}

	// The Migration Engine will create/update the YAML config during migration execution
	// For now, just store the config path in our minimal metadata
	if err := m.updateMetadataForMigration(migrationID, migrationID, configPath); err != nil {
		m.logger.Warn().Err(err).Str("migration_id", migrationID).Msg("failed to update migration metadata")
	}

	// Clear IsNewMigration flag after migration starts (it's no longer "new")
	if isNewMigration {
		meta.IsNewMigration = false
		if err := metaMgr.UpdateMigrationMetadata(meta); err != nil {
			m.logger.Warn().Err(err).Str("migration_id", migrationID).Msg("failed to clear IsNewMigration flag")
		}
	}

	record := &MigrationRecord{
		ID:            migrationID,
		SourceID:      plan.SourceDefinition.ID,
		DestinationID: plan.DestinationDefinition.ID,
		Status:        MigrationStatusRunning,
		StartedAt:     time.Now().UTC(),
	}

	m.mu.Lock()
	if _, exists := m.migrations[migrationID]; exists {
		m.mu.Unlock()
		return Migration{}, fmt.Errorf("migration %s is already running", migrationID)
	}
	m.migrations[migrationID] = record
	if _, ok := m.subscribers[migrationID]; !ok {
		m.subscribers[migrationID] = make(map[string]chan ProgressEvent)
	}
	m.mu.Unlock()

	m.publishProgress(record.ID, "started", nil, nil)

	// Migration Engine SDK will spawn log terminal automatically when SkipListener is false
	go m.RunMigration(
		record,
		plan.SourceDefinition,
		plan.DestinationDefinition,
		plan.SourceRoot,
		plan.DestinationRoot,
		opts,
		spectraConfigPath, // Pass override config path
	)

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

	// Check if migration has a controller (is actively running)
	if record.Controller == nil {
		m.mu.RUnlock()
		return nil, fmt.Errorf("migration %s is not running (no controller)", id)
	}

	controller := record.Controller
	m.mu.RUnlock()

	// Trigger shutdown
	m.logger.Info().
		Str("migration_id", id).
		Msg("triggering migration shutdown (killswitch)")

	controller.Shutdown()

	// Wait for shutdown to complete
	result, err := controller.Wait()

	// Check if migration was cleanly suspended
	if err != nil && err.Error() == "migration suspended by force shutdown" {
		// Clean shutdown - migration state is saved for resumption
		m.logger.Info().
			Str("migration_id", id).
			Msg("migration suspended successfully (killswitch)")

		// Update record status
		finished := time.Now().UTC()
		m.mu.Lock()
		record.Status = MigrationStatusSuspended
		record.CompletedAt = &finished
		record.Result = &result
		record.Controller = nil // Clear controller reference
		m.mu.Unlock()

		// Publish suspended event
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

		// Update record with error
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

	// Migration completed normally (shouldn't happen after shutdown, but handle it)
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

func (m *Manager) GetMigrationStatus(ctx context.Context, id string) (Status, error) {
	m.mu.RLock()
	record, ok := m.migrations[id]
	m.mu.RUnlock()

	if ok {
		return m.recordToStatus(record), nil
	}

	// In-memory record not found - try to query database directly
	// This allows checking status of migrations that were started before server restart
	dbPath, err := m.resolveDBPath("", id)
	if err != nil {
		return Status{}, ErrMigrationNotFound
	}

	// Check if database file exists
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		return Status{}, ErrMigrationNotFound
	}

	// Open and inspect database
	dbStatus, err := corebridgeDB.InspectMigrationStatusFromDB(ctx, m.logger, dbPath)
	if err != nil {
		return Status{}, ErrMigrationNotFound
	}

	// Convert migration.MigrationStatus to API Status format
	// We need to infer some fields that aren't in MigrationStatus
	status := Status{
		Migration: Migration{
			ID:        id,
			StartedAt: time.Now().UTC(), // We don't have this in DB, use current time as fallback
			Status:    MigrationStatusCompleted,
		},
	}

	if dbStatus.HasPending() {
		status.Status = MigrationStatusRunning
	} else if dbStatus.HasFailures() {
		status.Status = MigrationStatusFailed
	}

	// Create result view from migration status
	if !dbStatus.IsEmpty() {
		status.Result = &ResultView{
			Verification: VerificationView{
				SrcTotal:   dbStatus.SrcTotal,
				DstTotal:   dbStatus.DstTotal,
				SrcPending: dbStatus.SrcPending,
				DstPending: dbStatus.DstPending,
				SrcFailed:  dbStatus.SrcFailed,
				DstFailed:  dbStatus.DstFailed,
			},
		}
	}

	return status, nil
}

func (m *Manager) InspectMigrationStatus(ctx context.Context, migrationID string) (migration.MigrationStatus, error) {
	// Resolve database path from migration ID
	dbPath, err := m.resolveDBPath("", migrationID)
	if err != nil {
		return migration.MigrationStatus{}, ErrMigrationNotFound
	}

	// Check if database file exists
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

func (m *Manager) SetRecord(migrationID string, record *MigrationRecord) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.migrations[migrationID] = record
}

// updateMetadataForMigration creates or updates minimal metadata (ID, name, config path)
func (m *Manager) updateMetadataForMigration(migrationID, name, configPath string) error {
	meta, err := m.metadataMgr.GetMigrationMetadata(migrationID)
	if err != nil {
		// Create new metadata entry
		meta = metadata.MigrationMetadata{
			ID:         migrationID,
			Name:       name,
			ConfigPath: configPath,
		}
	} else {
		// Update existing metadata
		if name != "" {
			meta.Name = name
		}
		if configPath != "" {
			meta.ConfigPath = configPath
		}
	}

	return m.metadataMgr.UpdateMigrationMetadata(meta)
}

// createAdapterFactory creates an adapter factory for reconstructing adapters from YAML config
func (m *Manager) createAdapterFactory(spectraConfigOverridePath string) migration.AdapterFactory {
	return func(serviceType string, serviceCfg migration.ServiceConfigYAML, serviceConfigs map[string]interface{}) (fstypes.FSAdapter, error) {
		switch strings.ToLower(serviceType) {
		case "spectra":
			// Use override config if provided, otherwise try to extract from serviceConfigs
			configPath := spectraConfigOverridePath
			if configPath == "" {
				// Try to get original config path from service name
				// Look up the service definition to get the original config path
				def, err := m.serviceMgr.GetServiceDefinition(serviceCfg.Name)
				if err == nil && def.Spectra != nil {
					configPath = def.Spectra.ConfigPath
				} else {
					// Try to find by world if name lookup fails
					// Extract world from service name
					world := "primary"
					if strings.Contains(strings.ToLower(serviceCfg.Name), "s1") {
						world = "s1"
					}
					def, err := m.serviceMgr.GetServiceDefinitionByWorld(world)
					if err == nil && def.Spectra != nil {
						configPath = def.Spectra.ConfigPath
					} else {
						return nil, fmt.Errorf("spectra config path not found for service %s", serviceCfg.Name)
					}
				}
			}

			spectraFS, err := sdk.New(configPath)
			if err != nil {
				return nil, fmt.Errorf("failed to create SpectraFS: %w", err)
			}

			rootID := serviceCfg.RootID
			if rootID == "" {
				rootID = "root"
			}

			// Extract world from service name or use default
			world := "primary"
			if strings.Contains(strings.ToLower(serviceCfg.Name), "s1") {
				world = "s1"
			} else {
				// Try to get world from service definition
				def, err := m.serviceMgr.GetServiceDefinition(serviceCfg.Name)
				if err == nil && def.Spectra != nil {
					world = def.Spectra.World
				} else {
					// Try to find by world
					def, err := m.serviceMgr.GetServiceDefinitionByWorld(world)
					if err == nil && def.Spectra != nil {
						world = def.Spectra.World
					}
				}
			}

			adapter, err := fslib.NewSpectraFS(spectraFS, rootID, world)
			if err != nil {
				_ = spectraFS.Close()
				return nil, fmt.Errorf("failed to create SpectraFS adapter: %w", err)
			}

			return adapter, nil

		case "local":
			// For local services, use RootPath
			rootPath := serviceCfg.RootPath
			if rootPath == "" {
				return nil, fmt.Errorf("local service %s missing root path", serviceCfg.Name)
			}

			adapter, err := fslib.NewLocalFS(rootPath)
			if err != nil {
				return nil, fmt.Errorf("failed to create LocalFS adapter: %w", err)
			}

			return adapter, nil

		default:
			return nil, fmt.Errorf("unsupported service type: %s", serviceType)
		}
	}
}

// ExecuteMigrationFromConfig executes a migration using a pre-constructed migration.Config
func (m *Manager) ExecuteMigrationFromConfig(cfg migration.Config, opts MigrationOptions) (*migration.Result, error) {
	// Update config with options if needed
	if opts.WorkerCount > 0 {
		cfg.WorkerCount = opts.WorkerCount
	}
	if opts.MaxRetries > 0 {
		cfg.MaxRetries = opts.MaxRetries
	}
	if opts.CoordinatorLead > 0 {
		cfg.CoordinatorLead = opts.CoordinatorLead
	}
	if opts.LogAddress != "" {
		cfg.LogAddress = opts.LogAddress
	}
	if opts.LogLevel != "" {
		cfg.LogLevel = opts.LogLevel
	}
	cfg.SkipListener = !m.shouldEnableLoggingTerminal(opts)

	// StartMigration runs the migration asynchronously and returns a controller
	controller := migration.StartMigration(cfg)

	// Wait for migration to complete or be shutdown
	result, err := controller.Wait()

	// Safely close log service
	if logservice.LS != nil {
		func() {
			defer func() {
				if r := recover(); r != nil {
					// Channel already closed, ignore the panic
				}
			}()
			_ = logservice.LS.Close()
		}()
		logservice.LS = nil
	}

	// Check if migration was suspended (clean shutdown)
	if err != nil && err.Error() == "migration suspended by force shutdown" {
		// Migration was cleanly suspended - result contains stats up to shutdown point
		return &result, nil
	}

	if err != nil {
		return nil, err
	}

	return &result, nil
}

// ExecuteMigrationFromConfigWithController executes a migration using a pre-constructed migration.Config
// and returns the controller for programmatic shutdown
func (m *Manager) ExecuteMigrationFromConfigWithController(cfg migration.Config, opts MigrationOptions) (*migration.MigrationController, error) {
	// CRITICAL: Always force RemoveExisting to false (anti-pattern to remove existing DB)
	cfg.Database.RemoveExisting = false

	// CRITICAL: Ensure SeedRoots is set (same as test logic)
	// StartMigration will only use it if DB is empty, so it's safe to set to true
	cfg.SeedRoots = true

	// Update config with options if needed
	if opts.WorkerCount > 0 {
		cfg.WorkerCount = opts.WorkerCount
	}
	if opts.MaxRetries > 0 {
		cfg.MaxRetries = opts.MaxRetries
	}
	if opts.CoordinatorLead > 0 {
		cfg.CoordinatorLead = opts.CoordinatorLead
	}
	if opts.LogAddress != "" {
		cfg.LogAddress = opts.LogAddress
	}
	if opts.LogLevel != "" {
		cfg.LogLevel = opts.LogLevel
	}
	cfg.SkipListener = !m.shouldEnableLoggingTerminal(opts)

	// StartMigration runs the migration asynchronously and returns a controller
	// It will automatically detect and resume from suspended state if DB exists
	controller := migration.StartMigration(cfg)

	return controller, nil
}

// RunMigrationFromConfig runs a migration using a pre-constructed migration.Config
func (m *Manager) RunMigrationFromConfig(record *MigrationRecord, cfg migration.Config, opts MigrationOptions, spectraConfigOverridePath string) {
	m.logger.Info().
		Str("migration_id", record.ID).
		Str("source", cfg.Source.Name).
		Str("destination", cfg.Destination.Name).
		Msg("resuming migration from config")

	// Start migration with controller for programmatic shutdown
	controller, err := m.ExecuteMigrationFromConfigWithController(cfg, opts)
	if err != nil {
		m.mu.Lock()
		record.Status = MigrationStatusFailed
		record.Error = err.Error()
		finished := time.Now().UTC()
		record.CompletedAt = &finished
		m.mu.Unlock()

		m.logger.Error().
			Err(err).
			Str("migration_id", record.ID).
			Msg("failed to start migration")

		m.publishProgress(record.ID, "failed", nil, nil)
		m.closeSubscribers(record.ID)
		return
	}

	// Store controller in record for programmatic shutdown
	m.mu.Lock()
	record.Controller = controller
	m.mu.Unlock()

	heartbeat := time.NewTicker(5 * time.Second)
	done := make(chan struct{})
	go func() {
		for {
			select {
			case <-heartbeat.C:
				m.publishProgress(record.ID, "running", nil, nil)
			case <-done:
				heartbeat.Stop()
				return
			}
		}
	}()

	// Wait for migration to complete or be shutdown
	result, err := controller.Wait()
	close(done)

	// Check if migration was suspended (clean shutdown via killswitch)
	if err != nil && err.Error() == "migration suspended by force shutdown" {
		// Migration was cleanly suspended - result contains stats up to shutdown point
		finished := time.Now().UTC()

		m.mu.Lock()
		record.Status = MigrationStatusSuspended
		record.CompletedAt = &finished
		record.Result = &result
		record.Controller = nil // Clear controller reference
		m.mu.Unlock()

		m.logger.Info().
			Str("migration_id", record.ID).
			Msg("migration suspended (killswitch activated)")

		srcStats := result.Runtime.Src
		dstStats := result.Runtime.Dst
		m.publishProgress(record.ID, "suspended", &srcStats, &dstStats)
		m.closeSubscribers(record.ID)
		return
	}

	if err != nil {
		m.mu.Lock()
		record.Status = MigrationStatusFailed
		record.Error = err.Error()
		finished := time.Now().UTC()
		record.CompletedAt = &finished
		record.Controller = nil // Clear controller reference
		m.mu.Unlock()

		m.logger.Error().
			Err(err).
			Str("migration_id", record.ID).
			Msg("migration failed")

		m.publishProgress(record.ID, "failed", nil, nil)
		m.closeSubscribers(record.ID)
		return
	}

	finished := time.Now().UTC()

	m.mu.Lock()
	record.Status = MigrationStatusCompleted
	record.CompletedAt = &finished
	record.Result = &result
	record.Controller = nil // Clear controller reference
	m.mu.Unlock()

	m.logger.Info().
		Str("migration_id", record.ID).
		Msg("migration completed successfully")

	srcStats := result.Runtime.Src
	dstStats := result.Runtime.Dst
	m.publishProgress(record.ID, "completed", &srcStats, &dstStats)
	m.closeSubscribers(record.ID)
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
