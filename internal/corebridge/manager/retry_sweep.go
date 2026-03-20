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

	runningTasks := m.bgTaskMgr.GetRunningTasks(migrationID)
	if len(runningTasks) > 0 {
		return corebridge.SweepResponse{
			Success: false,
			Error:   fmt.Sprintf("cannot start retry sweep: there are %d running background tasks. Please wait for them to complete", len(runningTasks)),
		}, fmt.Errorf("cannot start retry sweep: there are %d running background tasks", len(runningTasks))
	}

	if m.bgTaskMgr.HasRunningTask(migrationID, corebridge.BackgroundTaskTypeRetrySweep) {
		return corebridge.SweepResponse{
			Success: false,
			Error:   "retry sweep is already running for this migration",
		}, fmt.Errorf("retry sweep is already running")
	}

	mig, err := m.GetMigration(context.TODO(), migrationID)
	if err != nil {
		return corebridge.SweepResponse{
			Success: false,
			Error:   err.Error(),
		}, err
	}
	// Persist phase to traversal-in-progress before 202 so polls see the correct phase before the goroutine runs.
	if err := mig.PrepareRetrySweep(); err != nil {
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

	taskID := m.bgTaskMgr.StartTask(migrationID, corebridge.BackgroundTaskTypeRetrySweep)
	go func() {
		_, runErr := mig.RunRetrySweep(opts)
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
	workerCount := config.WorkerCount
	if workerCount <= 0 {
		workerCount = m.cfg.Runtime.DefaultWorkerCount
	}
	if workerCount <= 0 {
		workerCount = 10
	}
	maxRetries := config.MaxRetries
	if maxRetries <= 0 {
		maxRetries = m.cfg.Runtime.DefaultMaxRetries
	}
	if maxRetries <= 0 {
		maxRetries = 3
	}
	logAddress := config.LogAddress
	if logAddress == "" {
		logAddress = m.cfg.Runtime.LogAddress
	}
	logLevel := config.LogLevel
	if logLevel == "" {
		logLevel = m.cfg.Runtime.LogLevel
	}
	if logLevel == "" {
		logLevel = "info"
	}
	skipListener := true
	if config.SkipListener != nil {
		skipListener = *config.SkipListener
	}
	return migration.RetrySweepOptions{
		WorkerCount:   workerCount,
		MaxRetries:    maxRetries,
		LogAddress:    logAddress,
		LogLevel:      logLevel,
		MaxKnownDepth: config.MaxKnownDepth,
		SkipListener:  skipListener,
	}
}
