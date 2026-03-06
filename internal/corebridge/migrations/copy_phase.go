package migrations

import (
	"context"
	"fmt"
	"time"

	"codeberg.org/Sylos/Migration-Engine/pkg/migration"
)

// RunCopyPhase executes the copy phase for a migration
func (m *Manager) RunCopyPhase(record *MigrationRecord, dbPath, _ string, opts MigrationOptions, _ string) error {
	m.logger.Info().
		Str("migration_id", record.ID).
		Msg("starting copy phase")

	plan := m.rootsMgr.GetPlan(record.ID)
	if plan == nil {
		return fmt.Errorf("root plan not found for migration %s", record.ID)
	}
	if plan.SourceAdapter == nil || plan.DestinationAdapter == nil {
		return fmt.Errorf("root adapters are not available for migration %s", record.ID)
	}
	srcAdapter := plan.SourceAdapter
	dstAdapter := plan.DestinationAdapter

	dbInstance, err := m.EnsureDB(record.ID)
	if err != nil {
		m.closeAdapters(srcAdapter, dstAdapter, record.ID)
		return fmt.Errorf("failed to ensure database for copy phase: %w", err)
	}

	copyCfg := migration.CopyPhaseConfig{
		DuckDB:       dbInstance,
		SrcAdapter:   srcAdapter,
		DstAdapter:   dstAdapter,
		WorkerCount:  m.selectWorkerCount(opts.WorkerCount),
		MaxRetries:   m.selectMaxRetries(opts.MaxRetries),
		LogAddress:   m.selectLogAddress(opts.LogAddress),
		LogLevel:     m.selectLogLevel(opts.LogLevel),
		SkipListener: m.selectSkipListener(opts),
	}

	if opts.StartupDelaySec > 0 {
		copyCfg.StartupDelay = time.Duration(opts.StartupDelaySec) * time.Second
	} else {
		copyCfg.StartupDelay = 500 * time.Millisecond
	}

	if opts.ProgressTickMillis > 0 {
		copyCfg.ProgressTick = time.Duration(opts.ProgressTickMillis) * time.Millisecond
	} else {
		copyCfg.ProgressTick = 2 * time.Second
	}

	copyCfg.ShutdownContext = context.Background()

	m.logger.Info().
		Str("migration_id", record.ID).
		Int("worker_count", copyCfg.WorkerCount).
		Int("max_retries", copyCfg.MaxRetries).
		Msg("running copy phase")

	stats, err := migration.RunCopyPhase(copyCfg)
	if err != nil {
		m.logger.Error().
			Err(err).
			Str("migration_id", record.ID).
			Msg("copy phase failed")

		m.closeAdapters(srcAdapter, dstAdapter, record.ID)

		return fmt.Errorf("copy phase failed: %w", err)
	}

	m.logger.Info().
		Str("migration_id", record.ID).
		Int("round", stats.Round).
		Int("pending", stats.Pending).
		Int("in_progress", stats.InProgress).
		Int("total_tracked", stats.TotalTracked).
		Int("workers", stats.Workers).
		Msg("copy phase completed successfully")

	m.closeAdapters(srcAdapter, dstAdapter, record.ID)

	return nil
}
