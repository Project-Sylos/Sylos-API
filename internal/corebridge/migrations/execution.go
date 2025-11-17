package migrations

import (
	"fmt"
	"time"

	"github.com/Project-Sylos/Migration-Engine/pkg/fsservices"
	"github.com/Project-Sylos/Migration-Engine/pkg/logservice"
	"github.com/Project-Sylos/Migration-Engine/pkg/migration"
	"github.com/Project-Sylos/Migration-Engine/pkg/queue"
	"github.com/Project-Sylos/Sylos-API/internal/corebridge/services"
)

func (m *Manager) ExecuteMigration(migrationID string, srcDef, dstDef services.ServiceDefinition, srcFolder, dstFolder fsservices.Folder, opts MigrationOptions, resolveDBPath func(path, migrationID string) (string, error), acquireAdapter func(services.ServiceDefinition, string, string) (fsservices.FSAdapter, func(), error)) (*migration.Result, error) {
	dbPath, err := resolveDBPath(opts.DatabasePath, migrationID)
	if err != nil {
		return nil, err
	}

	srcAdapter, srcCleanup, err := acquireAdapter(srcDef, srcFolder.Id, opts.SourceConnectionID)
	if err != nil {
		return nil, fmt.Errorf("source adapter: %w", err)
	}
	defer srcCleanup()

	dstAdapter, dstCleanup, err := acquireAdapter(dstDef, dstFolder.Id, opts.DestinationConnectionID)
	if err != nil {
		return nil, fmt.Errorf("destination adapter: %w", err)
	}
	defer dstCleanup()

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
		SkipListener:    !m.shouldEnableLoggingTerminal(opts), // SDK will spawn terminal when SkipListener is false
		StartupDelay:    time.Duration(opts.StartupDelaySec) * time.Second,
		ProgressTick:    time.Duration(opts.ProgressTickMillis) * time.Millisecond,
		Verification: migration.VerifyOptions{
			AllowPending:  opts.Verification.AllowPending,
			AllowFailed:   opts.Verification.AllowFailed,
			AllowNotOnSrc: opts.Verification.AllowNotOnSrc,
		},
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

	// LetsMigrate automatically detects and resumes in-progress migrations
	// when provided with an existing database file
	result, err := migration.LetsMigrate(cfg)

	// Safely close log service - LetsMigrate may have already closed it
	// Use recover to prevent panic from double-closing
	if logservice.LS != nil {
		func() {
			defer func() {
				// This is the most bullshit bandaid fix I've ever seen
				// But whatever I have bigger things to work on right now. 
				// TODO: Fix this properly.
				if r := recover(); r != nil {
					// Channel already closed, ignore the panic
				}
			}()
			_ = logservice.LS.Close()
		}()
	}

	if err != nil {
		return nil, err
	}

	return &result, nil
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

func (m *Manager) shouldEnableLoggingTerminal(opts MigrationOptions) bool {
	// If explicitly set in options, use that
	if opts.EnableLoggingTerminal {
		return true
	}
	// Otherwise use config default
	return m.cfg.Runtime.EnableLoggingTerminal
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
