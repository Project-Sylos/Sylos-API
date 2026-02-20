package migrations

import (
	"context"
	"fmt"
	"time"

	"codeberg.org/Sylos/Migration-Engine/pkg/migration"
)

// RunCopyPhase executes the copy phase for a migration
func (m *Manager) RunCopyPhase(record *MigrationRecord, dbPath, configPath string, opts MigrationOptions, spectraConfigPath string) error {
	m.logger.Info().
		Str("migration_id", record.ID).
		Msg("starting copy phase")

	yamlCfg, err := migration.LoadMigrationConfig(configPath)
	if err != nil {
		return fmt.Errorf("failed to load YAML config: %w", err)
	}

	yamlCfg.State.Status = "Copy-In-Progress"
	if err := migration.SaveMigrationConfig(configPath, yamlCfg); err != nil {
		m.logger.Warn().
			Err(err).
			Str("migration_id", record.ID).
			Msg("failed to update status to Copy-In-Progress")
	} else {
		m.logger.Info().
			Str("migration_id", record.ID).
			Msg("updated status to Copy-In-Progress")
	}

	srcAdapter, dstAdapter, shared, err := m.acquireSharedSpectraAdapters(yamlCfg.Services.Source, yamlCfg.Services.Destination, spectraConfigPath)
	if err != nil {
		return fmt.Errorf("failed to acquire shared adapters: %w", err)
	}

	if !shared {
		srcAdapter, err = m.acquireAdapterFromYAMLConfig(yamlCfg.Services.Source, spectraConfigPath)
		if err != nil {
			return fmt.Errorf("failed to acquire source adapter: %w", err)
		}

		dstAdapter, err = m.acquireAdapterFromYAMLConfig(yamlCfg.Services.Destination, spectraConfigPath)
		if err != nil {
			if srcAdapter != nil {
				if closer, ok := srcAdapter.(interface{ Close() error }); ok {
					_ = closer.Close()
				}
			}
			return fmt.Errorf("failed to acquire destination adapter: %w", err)
		}

		m.logger.Info().
			Str("migration_id", record.ID).
			Msg("acquired separate adapters for source and destination")
	}

	copyCfg := migration.CopyPhaseConfig{
		DBPath:       dbPath,
		SrcAdapter:   srcAdapter,
		DstAdapter:   dstAdapter,
		WorkerCount:  m.selectWorkerCount(opts.WorkerCount),
		MaxRetries:   m.selectMaxRetries(opts.MaxRetries),
		LogAddress:   m.selectLogAddress(opts.LogAddress),
		LogLevel:     m.selectLogLevel(opts.LogLevel),
		SkipListener: m.selectSkipListener(opts),
		ConfigPath:   configPath,
		YAMLConfig:   yamlCfg,
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

		yamlCfg.State.Status = "Copy-Failed"
		if saveErr := migration.SaveMigrationConfig(configPath, yamlCfg); saveErr != nil {
			m.logger.Warn().
				Err(saveErr).
				Str("migration_id", record.ID).
				Msg("failed to update status to Copy-Failed")
		}

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

	yamlCfg.State.Status = "Copy-Complete"
	if err := migration.SaveMigrationConfig(configPath, yamlCfg); err != nil {
		m.logger.Warn().
			Err(err).
			Str("migration_id", record.ID).
			Msg("failed to update status to Copy-Complete")
	} else {
		m.logger.Info().
			Str("migration_id", record.ID).
			Msg("updated status to Copy-Complete")
	}

	m.closeAdapters(srcAdapter, dstAdapter, record.ID)

	return nil
}
