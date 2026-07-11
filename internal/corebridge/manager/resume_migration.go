package manager

import (
	"context"
	"fmt"
	"time"

	"codeberg.org/Sylos/Migration-Engine/pkg/migration"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge"
)

// ResumeMigration resumes a stopped or failed migration in the correct phase:
// traversal-suspended / awaiting-traversal-review → retry sweep;
// copy-suspended → StartCopy (full copy resume);
// copy-in-progress (dead) / awaiting-copy-review → copy retry (failed copy items).
func (m *Manager) ResumeMigration(ctx context.Context, migrationID string, config corebridge.SweepConfigRequest) (corebridge.SweepResponse, error) {
	mig, err := m.GetMigration(context.TODO(), migrationID)
	if err != nil {
		return corebridge.SweepResponse{Success: false, Error: err.Error()}, err
	}

	switch mig.Phase() {
	case migration.PhaseCopySuspended:
		return m.triggerCopyStartResume(migrationID)
	case migration.PhaseCopyReview, migration.PhaseCopying:
		return m.triggerCopyRetryResume(migrationID, config)
	case migration.PhaseDeleteSuspended:
		return m.triggerDeleteStartResume(migrationID)
	case migration.PhaseDeleteReview, migration.PhaseDeleting:
		return m.triggerDeleteRetryResume(migrationID, config)
	default:
		return m.TriggerRetrySweep(ctx, migrationID, config)
	}
}

func (m *Manager) triggerCopyStartResume(migrationID string) (corebridge.SweepResponse, error) {
	if err := m.ensureCopyResumeAllowed(migrationID, corebridge.BackgroundTaskTypeCopyResume); err != nil {
		return corebridge.SweepResponse{Success: false, Error: err.Error()}, err
	}
	if err := m.ensureFSAdaptersRehydrated(migrationID); err != nil {
		return corebridge.SweepResponse{Success: false, Error: err.Error()}, err
	}

	mig, err := m.GetMigration(context.TODO(), migrationID)
	if err != nil {
		return corebridge.SweepResponse{Success: false, Error: err.Error()}, err
	}
	plan := m.rootsMgr.GetPlan(migrationID)
	if plan == nil || !plan.HasSource || !plan.HasDestination {
		msg := fmt.Sprintf("roots not fully configured for migration %s", migrationID)
		return corebridge.SweepResponse{Success: false, Error: msg}, fmt.Errorf("%s", msg)
	}

	cfg := m.buildTraversalConfig(corebridge.MigrationOptions{}, plan)
	m.mu.Lock()
	if rec := m.runtimeByID[migrationID]; rec != nil {
		rec.Status = migration.PhaseCopying
		rec.CompletedAt = nil
		rec.Error = ""
	}
	m.mu.Unlock()

	taskID := m.bgTaskMgr.StartTaskWithPath(migrationID, corebridge.BackgroundTaskTypeCopyResume, "")
	go func() {
		_, runErr := mig.StartCopy(cfg)
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
		m.bgTaskMgr.CompleteTask(migrationID, taskID)
	}()

	return corebridge.SweepResponse{
		Success: true,
		Message: "Copy phase resume started",
	}, nil
}

func (m *Manager) triggerCopyRetryResume(migrationID string, config corebridge.SweepConfigRequest) (corebridge.SweepResponse, error) {
	if err := m.ensureCopyResumeAllowed(migrationID, corebridge.BackgroundTaskTypeCopyRetry); err != nil {
		return corebridge.SweepResponse{Success: false, Error: err.Error()}, err
	}
	if err := m.ensureFSAdaptersRehydrated(migrationID); err != nil {
		return corebridge.SweepResponse{Success: false, Error: err.Error()}, err
	}

	mig, err := m.GetMigration(context.TODO(), migrationID)
	if err != nil {
		return corebridge.SweepResponse{Success: false, Error: err.Error()}, err
	}
	plan := m.rootsMgr.GetPlan(migrationID)
	if plan == nil || !plan.HasSource || !plan.HasDestination {
		msg := fmt.Sprintf("roots not fully configured for migration %s", migrationID)
		return corebridge.SweepResponse{Success: false, Error: msg}, fmt.Errorf("%s", msg)
	}

	cfg := m.buildTraversalConfig(corebridge.MigrationOptions{}, plan)
	opts := m.buildCopyPhaseOptions(config)
	if err := mig.PrepareCopyRetry(); err != nil {
		return corebridge.SweepResponse{Success: false, Error: err.Error()}, err
	}
	m.mu.Lock()
	if rec := m.runtimeByID[migrationID]; rec != nil {
		rec.Status = migration.PhaseCopying
		rec.CompletedAt = nil
		rec.Error = ""
	}
	m.mu.Unlock()

	taskID := m.bgTaskMgr.StartTaskWithPath(migrationID, corebridge.BackgroundTaskTypeCopyRetry, "")
	go func() {
		_, runErr := mig.RunCopyRetry(cfg, opts)
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
		m.bgTaskMgr.CompleteTask(migrationID, taskID)
	}()

	return corebridge.SweepResponse{
		Success: true,
		Message: "Copy retry started",
	}, nil
}

func (m *Manager) ensureCopyResumeAllowed(migrationID string, taskType corebridge.BackgroundTaskType) error {
	runningTasks := m.bgTaskMgr.GetRunningTasks(migrationID)
	if len(runningTasks) > 0 {
		return fmt.Errorf("cannot resume: there are %d running background tasks", len(runningTasks))
	}
	if m.bgTaskMgr.HasRunningTask(migrationID, taskType) {
		return fmt.Errorf("copy resume is already running for this migration")
	}
	return nil
}

func (m *Manager) buildCopyPhaseOptions(config corebridge.SweepConfigRequest) migration.CopyPhaseOptions {
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
	return migration.CopyPhaseOptions{
		WorkerCount:  config.WorkerCount,
		MaxRetries:   maxRetries,
		LogAddress:   logAddress,
		LogLevel:     logLevel,
		SkipListener: skipListener,
	}
}

func (m *Manager) triggerDeleteStartResume(migrationID string) (corebridge.SweepResponse, error) {
	if err := m.ensureFSAdaptersRehydrated(migrationID); err != nil {
		return corebridge.SweepResponse{Success: false, Error: err.Error()}, err
	}
	mig, err := m.GetMigration(context.TODO(), migrationID)
	if err != nil {
		return corebridge.SweepResponse{Success: false, Error: err.Error()}, err
	}
	plan := m.rootsMgr.GetPlan(migrationID)
	if plan == nil || !plan.HasSource {
		msg := fmt.Sprintf("source root not configured for migration %s", migrationID)
		return corebridge.SweepResponse{Success: false, Error: msg}, fmt.Errorf("%s", msg)
	}
	cfg := m.buildTraversalConfig(corebridge.MigrationOptions{}, plan)
	m.mu.Lock()
	if rec := m.runtimeByID[migrationID]; rec != nil {
		rec.Status = migration.PhaseDeleting
		rec.CompletedAt = nil
		rec.Error = ""
	}
	m.mu.Unlock()
	go func() {
		_, runErr := mig.StartDelete(cfg)
		doneAt := time.Now().UTC()
		m.mu.Lock()
		if rec := m.runtimeByID[migrationID]; rec != nil {
			rec.CompletedAt = &doneAt
			if runErr != nil {
				rec.Status = corebridge.MigrationStatusFailed
				rec.Error = runErr.Error()
			} else {
				rec.Status = mig.Phase()
			}
		}
		m.mu.Unlock()
	}()
	return corebridge.SweepResponse{Success: true, Message: "Delete phase resume started"}, nil
}

func (m *Manager) triggerDeleteRetryResume(migrationID string, config corebridge.SweepConfigRequest) (corebridge.SweepResponse, error) {
	if err := m.ensureFSAdaptersRehydrated(migrationID); err != nil {
		return corebridge.SweepResponse{Success: false, Error: err.Error()}, err
	}
	mig, err := m.GetMigration(context.TODO(), migrationID)
	if err != nil {
		return corebridge.SweepResponse{Success: false, Error: err.Error()}, err
	}
	plan := m.rootsMgr.GetPlan(migrationID)
	if plan == nil || !plan.HasSource {
		msg := fmt.Sprintf("source root not configured for migration %s", migrationID)
		return corebridge.SweepResponse{Success: false, Error: msg}, fmt.Errorf("%s", msg)
	}
	cfg := m.buildTraversalConfig(corebridge.MigrationOptions{}, plan)
	opts := m.buildCopyPhaseOptions(config)
	if err := mig.PrepareDeleteRetry(); err != nil {
		return corebridge.SweepResponse{Success: false, Error: err.Error()}, err
	}
	m.mu.Lock()
	if rec := m.runtimeByID[migrationID]; rec != nil {
		rec.Status = migration.PhaseDeleting
		rec.CompletedAt = nil
		rec.Error = ""
	}
	m.mu.Unlock()
	go func() {
		_, runErr := mig.RunDeleteRetry(cfg, opts)
		doneAt := time.Now().UTC()
		m.mu.Lock()
		if rec := m.runtimeByID[migrationID]; rec != nil {
			rec.CompletedAt = &doneAt
			if runErr != nil {
				rec.Status = corebridge.MigrationStatusFailed
				rec.Error = runErr.Error()
			} else {
				rec.Status = mig.Phase()
			}
		}
		m.mu.Unlock()
	}()
	return corebridge.SweepResponse{Success: true, Message: "Delete retry started"}, nil
}
