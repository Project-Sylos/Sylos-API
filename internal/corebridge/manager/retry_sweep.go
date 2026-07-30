package manager

import (
	"context"
	"fmt"
	"time"

	"codeberg.org/Sylos/Migration-Engine/pkg/migration"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge"
)

func (m *Manager) TriggerRetrySweep(ctx context.Context, migrationID string, config corebridge.SweepConfigRequest) (corebridge.SweepResponse, error) {
	if err := m.checkPhaseLock(migrationID, "retrySweep"); err != nil {
		return corebridge.SweepResponse{
			Success: false,
			Error:   err.Error(),
		}, err
	}

	mig, err := m.GetMigration(context.TODO(), migrationID)
	if err != nil {
		return corebridge.SweepResponse{
			Success: false,
			Error:   err.Error(),
		}, err
	}
	if resp, ok := m.resumeIfAlreadyRunning(migrationID, mig); ok {
		return resp, nil
	}
	if err := m.normalizeDeadInProgressForResume(migrationID, mig); err != nil {
		return corebridge.SweepResponse{
			Success: false,
			Error:   err.Error(),
		}, err
	}

	// Root plans are in-memory only; rebuild adapters/roots from the migration DB after API restart.
	if err := m.ensureFSAdaptersRehydrated(migrationID); err != nil {
		return corebridge.SweepResponse{
			Success: false,
			Error:   err.Error(),
		}, err
	}

	mig, err = m.GetMigration(context.TODO(), migrationID)
	if err != nil {
		return corebridge.SweepResponse{
			Success: false,
			Error:   err.Error(),
		}, err
	}
	plan := m.rootsMgr.GetPlan(migrationID)
	if plan == nil || !plan.HasSource || !plan.HasDestination {
		return corebridge.SweepResponse{
			Success: false,
			Error:   fmt.Sprintf("roots not fully configured for migration %s", migrationID),
		}, fmt.Errorf("roots not configured for migration %s", migrationID)
	}
	runCfg := m.buildTraversalConfig(corebridge.MigrationOptions{}, plan)
	// Persist phase to traversal-in-progress before 202 so polls see the correct phase before the goroutine runs.
	if err := mig.PreparePhase(migration.PreparePhaseRetrySweep); err != nil {
		return corebridge.SweepResponse{
			Success: false,
			Error:   err.Error(),
		}, err
	}
	m.mu.Lock()
	if rec := m.runtimeByID[migrationID]; rec != nil {
		rec.Status = migration.PhaseTraversing
		rec.CompletedAt = nil
		rec.Error = ""
	}
	m.mu.Unlock()

	opts := m.buildRetrySweepOptions(config)

	taskID := m.bgTaskMgr.StartTaskWithPath(migrationID, corebridge.BackgroundTaskTypeRetrySweep, "")
	go func() {
		_, runErr := mig.RunRetrySweep(runCfg, opts)
		doneAt := time.Now().UTC()
		m.mu.Lock()
		if rec := m.runtimeByID[migrationID]; rec != nil {
			rec.CompletedAt = &doneAt
			if runErr != nil {
				rec.Status = corebridge.MigrationStatusFailed
				rec.Error = runErr.Error()
			} else {
				rec.Status = mig.Phase()
				rec.Error = ""
			}
		}
		m.mu.Unlock()
		if runErr != nil {
			m.bgTaskMgr.FailTask(migrationID, taskID, runErr)
			return
		}
		if err := m.MarkPathReviewChanges(context.TODO(), migrationID, false); err != nil {
			m.logger.Warn().Err(err).Str("migration_id", migrationID).Msg("failed to clear path review changes flag")
		}
		m.bgTaskMgr.CompleteTask(migrationID, taskID)
	}()

	return corebridge.SweepResponse{
		Success: true,
		Message: "Retry sweep started",
	}, nil
}

func (m *Manager) buildRetrySweepOptions(config corebridge.SweepConfigRequest) migration.RetrySweepOptions {
	workerCount, maxRetries, logAddress, logLevel, skipListener := m.sweepConfigFields(config)
	return migration.RetrySweepOptions{
		WorkerCount:   workerCount,
		MaxRetries:    maxRetries,
		LogAddress:    logAddress,
		LogLevel:      logLevel,
		MaxKnownDepth: config.MaxKnownDepth,
		SkipListener:  skipListener,
	}
}

func (m *Manager) GetBackgroundTasks(ctx context.Context, migrationID string) ([]corebridge.BackgroundTask, error) {
	return m.bgTaskMgr.GetTasks(migrationID), nil
}

func (m *Manager) GetRunningBackgroundTasks(ctx context.Context, migrationID string) ([]corebridge.BackgroundTask, error) {
	return m.bgTaskMgr.GetRunningTasks(migrationID), nil
}

func (m *Manager) GetBackgroundTask(ctx context.Context, migrationID, taskID string) (*corebridge.BackgroundTask, error) {
	task, err := m.bgTaskMgr.GetBackgroundTask(migrationID, taskID)
	if err != nil {
		return nil, fmt.Errorf("get background task: %w", err)
	}
	return task, nil
}
