package manager

import (
	"context"
	"fmt"
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

	mig, err := m.engineMgr.GetMigration(migrationID)
	if err != nil {
		return corebridge.SweepResponse{
			Success: false,
			Error:   err.Error(),
		}, err
	}
	if mig == nil {
		return corebridge.SweepResponse{
			Success: false,
			Error:   "migration not found",
		}, corebridge.ErrMigrationNotFound
	}

	opts := migration.RetrySweepOptions{
		WorkerCount:   config.WorkerCount,
		MaxRetries:    config.MaxRetries,
		LogAddress:    config.LogAddress,
		LogLevel:      config.LogLevel,
		MaxKnownDepth: config.MaxKnownDepth,
		SkipListener:  true,
	}
	if config.SkipListener != nil {
		opts.SkipListener = *config.SkipListener
	}

	taskID := m.bgTaskMgr.StartTask(migrationID, corebridge.BackgroundTaskTypeRetrySweep)
	go func() {
		if _, err := mig.RunRetrySweep(opts); err != nil {
			m.bgTaskMgr.FailTask(migrationID, taskID, err)
			return
		}
		if err := m.markPathReviewChanges(migrationID, false); err != nil {
			m.logger.Warn().Err(err).Str("migration_id", migrationID).Msg("failed to clear path review changes flag")
		}
		m.bgTaskMgr.CompleteTask(migrationID, taskID)
	}()

	return corebridge.SweepResponse{
		Success: true,
		Message: "Retry sweep started",
	}, nil
}
