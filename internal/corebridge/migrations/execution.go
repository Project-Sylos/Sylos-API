package migrations

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
	"codeberg.org/Sylos/Migration-Engine/pkg/logservice"
	"codeberg.org/Sylos/Migration-Engine/pkg/migration"
	"codeberg.org/Sylos/Migration-Engine/pkg/queue"
	corebridgeDB "codeberg.org/Sylos/Sylos-API/internal/corebridge/database"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge/services"
	fstypes "codeberg.org/Sylos/Sylos-FS/pkg/types"
)

func (m *Manager) ExecuteMigration(migrationID string, srcDef, dstDef services.ServiceDefinition, srcFolder, dstFolder fstypes.Folder, opts MigrationOptions, resolveDBPath func(path, migrationID string) (string, error), acquireAdapter func(services.ServiceDefinition, string, string) (fstypes.FSAdapter, func(), error)) (*migration.Result, error) {
	dbPath, err := resolveDBPath(opts.DatabasePath, migrationID)
	if err != nil {
		return nil, err
	}

	srcAdapter, _, err := acquireAdapter(srcDef, srcFolder.ID(), opts.SourceConnectionID)
	if err != nil {
		return nil, fmt.Errorf("source adapter: %w", err)
	}
	// Note: Cleanup functions are not used. Once migration.StartMigration() is called,
	// the migration engine takes ownership of the adapters and handles cleanup itself.

	dstAdapter, _, err := acquireAdapter(dstDef, dstFolder.ID(), opts.DestinationConnectionID)
	if err != nil {
		return nil, fmt.Errorf("destination adapter: %w", err)
	}
	// Note: Cleanup functions are not called here. Once migration.StartMigration() is called,
	// the migration engine takes ownership of the adapters and handles cleanup itself.

	cfg := migration.Config{
		Database: migration.DatabaseConfig{
			Path:           dbPath,
			RemoveExisting: opts.RemoveExistingDB,
		},
		Source: migration.Service{
			Name:    srcDef.Name,
			Adapter: srcAdapter,
		},
		Destination: migration.Service{
			Name:    dstDef.Name,
			Adapter: dstAdapter,
		},
		WorkerCount:     m.selectWorkerCount(opts.WorkerCount),
		MaxRetries:      m.selectMaxRetries(opts.MaxRetries),
		CoordinatorLead: m.selectCoordinatorLead(opts.CoordinatorLead),
		LogAddress:      m.selectLogAddress(opts.LogAddress),
		LogLevel:        m.selectLogLevel(opts.LogLevel),
		SkipListener:    m.selectSkipListener(opts), // Defaults to true (skip listener)
		StartupDelay:    time.Duration(opts.StartupDelaySec) * time.Second,
		ProgressTick:    time.Duration(opts.ProgressTickMillis) * time.Millisecond,
		Verification:    m.selectVerificationOptions(opts.Verification),
	}

	if cfg.StartupDelay == 0 {
		cfg.StartupDelay = 3 * time.Second
	}
	if cfg.ProgressTick == 0 {
		cfg.ProgressTick = 500 * time.Millisecond
	}

	if err := cfg.SetRootFolders(srcFolder, dstFolder); err != nil {
		return nil, err
	}

	// Set SeedRoots to true - LetsMigrate will only use it if DB is empty
	cfg.SeedRoots = true

	// Create and save Migration Engine YAML config before starting migration
	configPath := corebridgeDB.ConfigPathFromDatabasePath(dbPath)

	// CRITICAL: Ensure config directory exists so SDK can save status updates
	configDir := filepath.Dir(configPath)
	if err := os.MkdirAll(configDir, 0o755); err != nil {
		m.logger.Warn().Err(err).Str("migration_id", migrationID).Str("config_dir", configDir).Msg("failed to create config directory")
		// Continue anyway - SDK might create it, but this ensures it exists
	}

	// Get initial migration status to create YAML config
	status := migration.MigrationStatus{} // Empty status for new migration
	yamlCfg, err := migration.NewMigrationConfigYAML(cfg, status)
	if err != nil {
		// Log error but continue - Migration Engine might create it during execution
		m.logger.Warn().Err(err).Str("migration_id", migrationID).Str("config_path", configPath).Msg("failed to create initial YAML config structure")
	} else {
		// Update migration ID in YAML config
		yamlCfg.Metadata.MigrationID = migrationID
		if err := migration.SaveMigrationConfig(configPath, yamlCfg); err != nil {
			// Log warning but continue - Migration Engine will update it during execution
			m.logger.Warn().Err(err).Str("migration_id", migrationID).Str("config_path", configPath).Msg("failed to save initial YAML config")
		} else {
			m.logger.Info().Str("migration_id", migrationID).Str("config_path", configPath).Msg("saved initial YAML config")
		}
	}

	// StartMigration runs the migration asynchronously and returns a controller
	// The Migration Engine will automatically detect and resume in-progress migrations
	// when provided with an existing database file
	controller := migration.StartMigration(cfg)

	// Wait for migration to complete or be shutdown
	result, err := controller.Wait()

	// Safely close log service - migration may have already closed it
	// Use recover to prevent panic from double-closing
	if logservice.LS != nil {
		func() {
			defer func() {
				if r := recover(); r != nil {
					// Channel already closed, ignore the panic
				}
			}()
			_ = logservice.LS.Close()
		}()
		// Allow Migration Engine to recreate the global logger on next run
		logservice.LS = nil
	}

	// Check if migration was suspended (clean shutdown)
	if err != nil && err.Error() == "migration suspended by force shutdown" {
		// Migration was cleanly suspended - result contains stats up to shutdown point
		// This is expected behavior for killswitch, not an error
		return &result, nil
	}

	if err != nil {
		return nil, err
	}

	return &result, nil
}

// ExecuteMigrationWithController executes a migration and returns the controller for programmatic shutdown
// This allows the caller to control when to shutdown the migration
// The migration engine takes ownership of the adapters and handles cleanup
// dbInstance must be pre-opened by the API - the migration engine does not open databases
// srcAdapter and dstAdapter must be pre-acquired adapter instances
func (m *Manager) ExecuteMigrationWithController(migrationID string, srcDef, dstDef services.ServiceDefinition, srcFolder, dstFolder fstypes.Folder, opts MigrationOptions, dbInstance *db.DB, resolveDBPath func(path, migrationID string) (string, error), srcAdapter, dstAdapter fstypes.FSAdapter) (*migration.MigrationController, error) {

	dbPath, err := resolveDBPath(opts.DatabasePath, migrationID)
	if err != nil {
		m.logger.Error().Err(err).Str("migration_id", migrationID).Msg("failed to resolve DB path")
		return nil, err
	}

	if dbInstance == nil {
		return nil, fmt.Errorf("database instance is required - migration engine does not open databases")
	}

	if srcAdapter == nil {
		return nil, fmt.Errorf("source adapter is required")
	}

	if dstAdapter == nil {
		return nil, fmt.Errorf("destination adapter is required")
	}

	m.logger.Info().Str("migration_id", migrationID).Str("source", srcDef.ID).Str("destination", dstDef.ID).Msg("using pre-acquired adapters for migration")

	cfg := migration.Config{
		// REQUIRED: Pass the pre-opened DB instance (API owns lifecycle)
		DatabaseInstance: dbInstance,
		// Database config kept for path info, but ME doesn't use it to open DB
		Database: migration.DatabaseConfig{
			Path:           dbPath,
			RemoveExisting: opts.RemoveExistingDB,
		},
		Source: migration.Service{
			Name:    srcDef.Name,
			Adapter: srcAdapter,
		},
		Destination: migration.Service{
			Name:    dstDef.Name,
			Adapter: dstAdapter,
		},
		WorkerCount:     m.selectWorkerCount(opts.WorkerCount),
		MaxRetries:      m.selectMaxRetries(opts.MaxRetries),
		CoordinatorLead: m.selectCoordinatorLead(opts.CoordinatorLead),
		LogAddress:      m.selectLogAddress(opts.LogAddress),
		LogLevel:        m.selectLogLevel(opts.LogLevel),
		SkipListener:    m.selectSkipListener(opts), // Defaults to true (skip listener)
		StartupDelay:    time.Duration(opts.StartupDelaySec) * time.Second,
		ProgressTick:    time.Duration(opts.ProgressTickMillis) * time.Millisecond,
		Verification:    m.selectVerificationOptions(opts.Verification),
	}

	if cfg.StartupDelay == 0 {
		cfg.StartupDelay = 3 * time.Second
	}
	if cfg.ProgressTick == 0 {
		cfg.ProgressTick = 500 * time.Millisecond
	}

	m.logger.Info().Str("migration_id", migrationID).Msg("setting root folders")
	if err := cfg.SetRootFolders(srcFolder, dstFolder); err != nil {
		m.logger.Error().Err(err).Str("migration_id", migrationID).Msg("failed to set root folders")
		return nil, err
	}
	m.logger.Info().Str("migration_id", migrationID).Msg("root folders set successfully")

	// Set SeedRoots to true - StartMigration will only use it if DB is empty
	cfg.SeedRoots = true
	m.logger.Info().Str("migration_id", migrationID).Msg("about to create YAML config")

	// Create and save Migration Engine YAML config before starting migration
	configPath := corebridgeDB.ConfigPathFromDatabasePath(dbPath)
	m.logger.Info().Str("migration_id", migrationID).Str("config_path", configPath).Str("db_path", dbPath).Str("source_type", string(srcDef.Type)).Str("dest_type", string(dstDef.Type)).Msg("creating YAML config")

	// Get initial migration status to create YAML config
	status := migration.MigrationStatus{} // Empty status for new migration
	m.logger.Info().Str("migration_id", migrationID).Msg("calling NewMigrationConfigYAML")
	yamlCfg, err := migration.NewMigrationConfigYAML(cfg, status)
	if err != nil {
		m.logger.Error().Err(err).Str("migration_id", migrationID).Str("config_path", configPath).Msg("failed to create initial YAML config structure")
	} else {
		m.logger.Info().Str("migration_id", migrationID).Msg("NewMigrationConfigYAML succeeded, saving config")
		// Update migration ID in YAML config
		yamlCfg.Metadata.MigrationID = migrationID
		if err := migration.SaveMigrationConfig(configPath, yamlCfg); err != nil {
			m.logger.Error().Err(err).Str("migration_id", migrationID).Str("config_path", configPath).Msg("failed to save initial YAML config")
		} else {
			m.logger.Info().Str("migration_id", migrationID).Str("config_path", configPath).Msg("saved initial YAML config")
		}
	}

	// StartMigration runs the migration asynchronously and returns a controller
	// The Migration Engine will automatically detect and resume in-progress migrations
	// At this point, the migration engine takes ownership of the adapters and will handle cleanup
	controller := migration.StartMigration(cfg)

	return controller, nil
}

func (m *Manager) selectWorkerCount(value int) int {
	if value > 0 {
		return value
	}
	if m.cfg.Runtime.DefaultWorkerCount > 0 {
		return m.cfg.Runtime.DefaultWorkerCount
	}
	return 10
}

func (m *Manager) selectMaxRetries(value int) int {
	if value > 0 {
		return value
	}
	if m.cfg.Runtime.DefaultMaxRetries > 0 {
		return m.cfg.Runtime.DefaultMaxRetries
	}
	return 3
}

func (m *Manager) selectCoordinatorLead(value int) int {
	if value > 0 {
		return value
	}
	if m.cfg.Runtime.DefaultCoordinatorLead > 0 {
		return m.cfg.Runtime.DefaultCoordinatorLead
	}
	return 4
}

func (m *Manager) selectLogAddress(value string) string {
	if value != "" {
		return value
	}
	return m.cfg.Runtime.LogAddress
}

func (m *Manager) selectLogLevel(value string) string {
	if value != "" {
		return value
	}
	if m.cfg.Runtime.LogLevel != "" {
		return m.cfg.Runtime.LogLevel
	}
	return "info"
}

// selectSkipListener determines whether to skip the log listener terminal
// Defaults to true (skip listener) if not explicitly set, since we now have UI hooks for logs
func (m *Manager) selectSkipListener(opts MigrationOptions) bool {
	// If explicitly set in options, use that value
	if opts.SkipListener != nil {
		return *opts.SkipListener
	}
	// Default to true (skip listener) - UI can see logs via API hooks
	return true
}

// selectVerificationOptions sets default verification options
// Defaults: AllowPending=false (not allowed), AllowNotOnSrc=true (allowed)
// These defaults apply to all migrations unless explicitly overridden
// Note: AllowNotOnSrc=true is required for ephemeral mode (divergent trees are expected)
func (m *Manager) selectVerificationOptions(opts VerificationOptions) migration.VerifyOptions {
	// Start with defaults
	verifyOpts := migration.VerifyOptions{
		// Default: AllowPending is false (pending nodes should not be allowed)
		AllowPending: false,
		// Default: AllowNotOnSrc is true (nodes on dst but not on src are allowed)
		// This is required for ephemeral mode where trees may diverge between calls
		AllowNotOnSrc: true,
	}

	// Apply user-provided options if they were explicitly set
	// Note: Since Go booleans can't distinguish "not set" from "false", we use a heuristic:
	// Defaults are: AllowPending=false, AllowNotOnSrc=true
	// Zero values (not provided) are: AllowPending=false, AllowNotOnSrc=false
	//
	// We can detect user-provided options by checking if any value differs from zero values:
	// - If AllowPending is true, user provided it (zero is false)
	// - If AllowNotOnSrc is true, user provided it (zero is false, default is true)
	//
	// If user provided any option, we use all their values (even if some match defaults)
	// Otherwise, we use our defaults
	userProvidedOptions := opts.AllowPending || opts.AllowNotOnSrc

	if userProvidedOptions {
		// User has provided explicit options, use their values
		verifyOpts.AllowPending = opts.AllowPending
		verifyOpts.AllowNotOnSrc = opts.AllowNotOnSrc
	}
	// Otherwise, use defaults (already set above)

	return verifyOpts
}

func resultToView(res *migration.Result) *ResultView {
	if res == nil {
		return nil
	}

	return &ResultView{
		RootSummary: RootSummaryView{
			SrcRoots: res.RootSummary.SrcRoots,
			DstRoots: res.RootSummary.DstRoots,
		},
		Runtime: RuntimeStatsView{
			Duration: res.Runtime.Duration.String(),
			Src:      queueStatsToView(res.Runtime.Src),
			Dst:      queueStatsToView(res.Runtime.Dst),
		},
		Verification: VerificationView{
			SrcTotal:    res.Verification.SrcTotal,
			DstTotal:    res.Verification.DstTotal,
			SrcPending:  res.Verification.SrcPending,
			DstPending:  res.Verification.DstPending,
			SrcFailed:   res.Verification.SrcFailed,
			DstFailed:   res.Verification.DstFailed,
			DstNotOnSrc: res.Verification.DstNotOnSrc,
		},
	}
}

func queueStatsToView(stats queue.QueueStats) QueueStatsView {
	return QueueStatsView{
		Name:         stats.Name,
		Round:        stats.Round,
		Pending:      stats.Pending,
		InProgress:   stats.InProgress,
		TotalTracked: stats.TotalTracked,
		Workers:      stats.Workers,
	}
}
