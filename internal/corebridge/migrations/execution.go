package migrations

import (
	"fmt"
	"time"

	"codeberg.org/Sylos/Migration-Engine/pkg/migration"
	"codeberg.org/Sylos/Migration-Engine/pkg/queue"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge/services"
	fstypes "codeberg.org/Sylos/Sylos-FS/pkg/types"
)

// buildMigrationConfig builds engine migration.Config from adapters, folders, and opts.
func (m *Manager) buildMigrationConfig(dbPath string, srcAdapter, dstAdapter fstypes.FSAdapter, srcDef, dstDef services.ServiceDefinition, srcFolder, dstFolder fstypes.Folder, opts MigrationOptions) (migration.Config, error) {
	if srcAdapter == nil || dstAdapter == nil {
		return migration.Config{}, fmt.Errorf("source and destination adapters are required")
	}

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
		SkipListener:    m.selectSkipListener(opts),
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
		return migration.Config{}, err
	}
	cfg.SeedRoots = true
	cfg.Database.RemoveExisting = false

	return cfg, nil
}

// startEngine applies opts to cfg and starts the migration engine. Returns the controller for programmatic shutdown.
func (m *Manager) startEngine(cfg migration.Config, opts MigrationOptions) *migration.MigrationController {
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
	cfg.SkipListener = m.selectSkipListener(opts)
	return migration.StartMigration(cfg)
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
