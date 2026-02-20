package migrations

import (
	"fmt"
	"time"

	"codeberg.org/Sylos/Migration-Engine/pkg/migration"
	fstypes "codeberg.org/Sylos/Sylos-FS/pkg/types"
)

// runMigrationEngine is the single path that starts the migration engine, waits for completion, and handles result/cleanup.
func (m *Manager) runMigrationEngine(record *MigrationRecord, params RunParams) {
	if params.FromConfig != nil {
		m.runMigrationEngineFromConfig(record, params.FromConfig)
		return
	}
	if params.FromRoots != nil {
		m.runMigrationEngineFromRoots(record, params.FromRoots)
		return
	}
	m.mu.Lock()
	record.Status = MigrationStatusFailed
	record.Error = "invalid run params (no FromRoots or FromConfig)"
	finished := time.Now().UTC()
	record.CompletedAt = &finished
	m.mu.Unlock()
	m.publishProgress(record.ID, "failed", nil, nil)
	m.closeSubscribers(record.ID)
}

func (m *Manager) runMigrationEngineFromConfig(record *MigrationRecord, p *RunParamsFromConfig) {
	srcAdapter := p.Cfg.Source.Adapter
	dstAdapter := p.Cfg.Destination.Adapter

	dbPath := p.Cfg.Database.Path
	if dbPath == "" {
		m.failRecord(record, "migration config has no database path")
		m.publishProgress(record.ID, "failed", nil, nil)
		m.closeSubscribers(record.ID)
		return
	}

	controller := m.startEngine(p.Cfg, p.Opts)

	m.mu.Lock()
	record.Controller = controller
	record.DB = nil
	m.mu.Unlock()

	m.waitAndFinish(record, controller, srcAdapter, dstAdapter, true)
}

func (m *Manager) runMigrationEngineFromRoots(record *MigrationRecord, p *RunParamsFromRoots) {
	plan := m.rootsMgr.GetPlan(record.ID)
	if plan != nil && !plan.Seeded {
		_, _, _, err := m.rootsMgr.SeedPlanIfReady(record.ID)
		if err != nil {
			m.failRecord(record, fmt.Sprintf("failed to seed migration plan: %v", err))
			m.logger.Error().Err(err).Str("migration_id", record.ID).Msg("failed to seed migration plan")
			m.publishProgress(record.ID, "failed", nil, nil)
			m.closeSubscribers(record.ID)
			return
		}
		plan = m.rootsMgr.GetPlan(record.ID)
		if plan != nil && plan.Seeded && plan.DatabasePath != "" {
			p.Opts.DatabasePath = plan.DatabasePath
			p.DbPath = plan.DatabasePath
		}
	}

	dbPath := p.DbPath
	if dbPath == "" {
		var err error
		dbPath, err = m.resolveDBPath(p.Opts.DatabasePath, record.ID)
		if err != nil {
			m.failRecord(record, fmt.Sprintf("failed to resolve database path: %v", err))
			m.logger.Error().Err(err).Str("migration_id", record.ID).Msg("failed to resolve database path")
			m.publishProgress(record.ID, "failed", nil, nil)
			m.closeSubscribers(record.ID)
			return
		}
	}

	plan = m.rootsMgr.GetPlan(record.ID)
	if plan == nil {
		m.failRecord(record, "root plan not found")
		m.logger.Error().Str("migration_id", record.ID).Msg("root plan not found")
		m.publishProgress(record.ID, "failed", nil, nil)
		m.closeSubscribers(record.ID)
		return
	}
	if plan.SourceAdapter == nil || plan.DestinationAdapter == nil {
		m.failRecord(record, "adapters not available in root plan")
		m.logger.Error().Str("migration_id", record.ID).Msg("adapters not available in root plan")
		m.publishProgress(record.ID, "failed", nil, nil)
		m.closeSubscribers(record.ID)
		return
	}

	cfg, err := m.buildMigrationConfig(dbPath, plan.SourceAdapter, plan.DestinationAdapter, plan.SourceDefinition, plan.DestinationDefinition, plan.SourceRoot, plan.DestinationRoot, p.Opts)
	if err != nil {
		m.failRecord(record, err.Error())
		m.logger.Error().Err(err).Str("migration_id", record.ID).Msg("failed to build migration config")
		m.publishProgress(record.ID, "failed", nil, nil)
		m.closeSubscribers(record.ID)
		return
	}

	controller := m.startEngine(cfg, p.Opts)

	m.mu.Lock()
	record.Controller = controller
	record.DB = nil
	m.mu.Unlock()

	m.waitAndFinish(record, controller, plan.SourceAdapter, plan.DestinationAdapter, false)
}

func (m *Manager) failRecord(record *MigrationRecord, errMsg string) {
	m.mu.Lock()
	record.Status = MigrationStatusFailed
	record.Error = errMsg
	finished := time.Now().UTC()
	record.CompletedAt = &finished
	m.mu.Unlock()
}

func (m *Manager) waitAndFinish(record *MigrationRecord, controller *migration.MigrationController, srcAdapter, dstAdapter fstypes.FSAdapter, fromConfig bool) {
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

	result, err := controller.Wait()
	close(done)

	if err != nil && err.Error() == "migration suspended by force shutdown" {
		finished := time.Now().UTC()
		m.mu.Lock()
		record.Status = MigrationStatusSuspended
		record.CompletedAt = &finished
		record.Result = &result
		record.Controller = nil
		m.mu.Unlock()

		m.closeAdapters(srcAdapter, dstAdapter, record.ID)
		if !fromConfig {
			m.rootsMgr.ClearAdapters(record.ID)
		}

		m.logger.Info().Str("migration_id", record.ID).Msg("migration suspended (killswitch activated)")
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
		record.Controller = nil
		m.mu.Unlock()

		m.closeAdapters(srcAdapter, dstAdapter, record.ID)
		if !fromConfig {
			m.rootsMgr.ClearAdapters(record.ID)
		}

		m.logger.Error().Err(err).Str("migration_id", record.ID).Msg("migration failed")
		m.publishProgress(record.ID, "failed", nil, nil)
		m.closeSubscribers(record.ID)
		return
	}

	finished := time.Now().UTC()
	m.mu.Lock()
	record.Status = MigrationStatusCompleted
	record.CompletedAt = &finished
	record.Result = &result
	record.Controller = nil
	m.mu.Unlock()

	m.closeAdapters(srcAdapter, dstAdapter, record.ID)
	if !fromConfig {
		m.rootsMgr.ClearAdapters(record.ID)
	}

	m.logger.Info().Str("migration_id", record.ID).Msg("migration completed successfully")
	srcStats := result.Runtime.Src
	dstStats := result.Runtime.Dst
	m.publishProgress(record.ID, "completed", &srcStats, &dstStats)
	m.closeSubscribers(record.ID)
}

// closeAdapters closes adapters if they implement the Closer interface
func (m *Manager) closeAdapters(srcAdapter, dstAdapter fstypes.FSAdapter, migrationID string) {
	if srcAdapter != nil {
		if closer, ok := srcAdapter.(interface{ Close() error }); ok {
			if err := closer.Close(); err != nil {
				m.logger.Warn().
					Err(err).
					Str("migration_id", migrationID).
					Msg("failed to close source adapter")
			} else {
				m.logger.Debug().
					Str("migration_id", migrationID).
					Msg("closed source adapter")
			}
		}
	}
	if dstAdapter != nil {
		if closer, ok := dstAdapter.(interface{ Close() error }); ok {
			if err := closer.Close(); err != nil {
				m.logger.Warn().
					Err(err).
					Str("migration_id", migrationID).
					Msg("failed to close destination adapter")
			} else {
				m.logger.Debug().
					Str("migration_id", migrationID).
					Msg("closed destination adapter")
			}
		}
	}
}
