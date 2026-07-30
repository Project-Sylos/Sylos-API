package manager

import (
	"context"
	"fmt"
	"time"

	"codeberg.org/Sylos/Migration-Engine/pkg/migration"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge"
)

type phaseResumeSetup struct {
	mig *migration.Migration
	cfg migration.Config
}

// ResumeMigration resumes a stopped or failed migration in the correct phase:
// traversal-suspended / awaiting-traversal-review → retry sweep;
// copy-suspended → StartCopy (full copy resume);
// awaiting-copy-review → copy retry (failed copy items);
// delete-suspended → StartDelete; awaiting-delete-review → delete retry.
//
// Idempotent: if the migration (or a resume background task) is already live, returns success
// with AlreadyRunning. If phase is still *-in-progress but nothing is running, normalizes to
// *-suspended then resumes.
func (m *Manager) ResumeMigration(ctx context.Context, migrationID string, config corebridge.SweepConfigRequest) (corebridge.SweepResponse, error) {
	mig, err := m.GetMigration(context.TODO(), migrationID)
	if err != nil {
		return corebridge.SweepResponse{Success: false, Error: err.Error()}, err
	}

	if resp, ok := m.resumeIfAlreadyRunning(migrationID, mig); ok {
		return resp, nil
	}
	if err := m.normalizeDeadInProgressForResume(migrationID, mig); err != nil {
		return corebridge.SweepResponse{Success: false, Error: err.Error()}, err
	}

	switch mig.Phase() {
	case migration.PhaseCopySuspended:
		return m.triggerCopyStartResume(migrationID)
	case migration.PhaseCopyReview:
		return m.triggerCopyRetryResume(migrationID, config)
	case migration.PhaseDeleteSuspended:
		return m.triggerDeleteStartResume(migrationID)
	case migration.PhaseDeleteReview:
		return m.triggerDeleteRetryResume(migrationID, config)
	default:
		return m.TriggerRetrySweep(ctx, migrationID, config)
	}
}

func (m *Manager) resumeIfAlreadyRunning(migrationID string, mig *migration.Migration) (corebridge.SweepResponse, bool) {
	if mig != nil && mig.IsLive() {
		return corebridge.SweepResponse{
			Success:        true,
			Message:        "Migration already running",
			AlreadyRunning: true,
		}, true
	}
	if len(m.bgTaskMgr.GetRunningTasks(migrationID)) > 0 {
		return corebridge.SweepResponse{
			Success:        true,
			Message:        "Migration resume already in progress",
			AlreadyRunning: true,
		}, true
	}
	return corebridge.SweepResponse{}, false
}

func (m *Manager) normalizeDeadInProgressForResume(migrationID string, mig *migration.Migration) error {
	if mig == nil {
		return nil
	}
	changed, err := mig.NormalizeDeadInProgressToSuspended()
	if err != nil {
		return err
	}
	if !changed {
		return nil
	}
	m.mu.Lock()
	if rec := m.runtimeByID[migrationID]; rec != nil {
		rec.Status = mig.Phase()
		rec.Error = ""
	}
	m.mu.Unlock()
	return nil
}

func (m *Manager) preparePhaseResume(migrationID string, requireDestination bool) (phaseResumeSetup, corebridge.SweepResponse, bool, error) {
	mig, err := m.GetMigration(context.TODO(), migrationID)
	if err != nil {
		return phaseResumeSetup{}, corebridge.SweepResponse{Success: false, Error: err.Error()}, false, err
	}
	if resp, ok := m.resumeIfAlreadyRunning(migrationID, mig); ok {
		return phaseResumeSetup{}, resp, true, nil
	}
	if err := m.ensureFSAdaptersRehydrated(migrationID); err != nil {
		return phaseResumeSetup{}, corebridge.SweepResponse{Success: false, Error: err.Error()}, false, err
	}
	mig, err = m.GetMigration(context.TODO(), migrationID)
	if err != nil {
		return phaseResumeSetup{}, corebridge.SweepResponse{Success: false, Error: err.Error()}, false, err
	}
	plan := m.rootsMgr.GetPlan(migrationID)
	if requireDestination {
		if plan == nil || !plan.HasSource || !plan.HasDestination {
			msg := fmt.Sprintf("roots not fully configured for migration %s", migrationID)
			return phaseResumeSetup{}, corebridge.SweepResponse{Success: false, Error: msg}, false, fmt.Errorf("%s", msg)
		}
	} else if plan == nil || !plan.HasSource {
		msg := fmt.Sprintf("source root not configured for migration %s", migrationID)
		return phaseResumeSetup{}, corebridge.SweepResponse{Success: false, Error: msg}, false, fmt.Errorf("%s", msg)
	}
	cfg := m.buildTraversalConfig(corebridge.MigrationOptions{}, plan)
	return phaseResumeSetup{mig: mig, cfg: cfg}, corebridge.SweepResponse{}, false, nil
}

func (m *Manager) setRuntimePhaseStatus(migrationID, status string) {
	m.mu.Lock()
	if rec := m.runtimeByID[migrationID]; rec != nil {
		rec.Status = status
		rec.CompletedAt = nil
		rec.Error = ""
	}
	m.mu.Unlock()
}

func (m *Manager) finishRuntimeAfterBackgroundPhase(migrationID string, mig *migration.Migration, runErr error) {
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
}

func (m *Manager) sweepConfigFields(config corebridge.SweepConfigRequest) (workerCount, maxRetries int, logAddress, logLevel string, skipListener bool) {
	workerCount = config.WorkerCount
	maxRetries = config.MaxRetries
	if maxRetries <= 0 {
		maxRetries = m.cfg.Runtime.DefaultMaxRetries
	}
	if maxRetries <= 0 {
		maxRetries = 3
	}
	logAddress = config.LogAddress
	if logAddress == "" {
		logAddress = m.cfg.Runtime.LogAddress
	}
	logLevel = config.LogLevel
	if logLevel == "" {
		logLevel = m.cfg.Runtime.LogLevel
	}
	if logLevel == "" {
		logLevel = "info"
	}
	skipListener = true
	if config.SkipListener != nil {
		skipListener = *config.SkipListener
	}
	return workerCount, maxRetries, logAddress, logLevel, skipListener
}

func (m *Manager) triggerCopyStartResume(migrationID string) (corebridge.SweepResponse, error) {
	setup, early, done, err := m.preparePhaseResume(migrationID, true)
	if err != nil || done {
		return early, err
	}
	m.setRuntimePhaseStatus(migrationID, migration.PhaseCopying)

	taskID := m.bgTaskMgr.StartTaskWithPath(migrationID, corebridge.BackgroundTaskTypeCopyResume, "")
	go func() {
		_, runErr := setup.mig.StartCopy(setup.cfg)
		m.finishRuntimeAfterBackgroundPhase(migrationID, setup.mig, runErr)
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
	setup, early, done, err := m.preparePhaseResume(migrationID, true)
	if err != nil || done {
		return early, err
	}
	opts := m.buildCopyPhaseOptions(config)
	if err := setup.mig.PreparePhase(migration.PreparePhaseCopyRetry); err != nil {
		return corebridge.SweepResponse{Success: false, Error: err.Error()}, err
	}
	m.setRuntimePhaseStatus(migrationID, migration.PhaseCopying)

	taskID := m.bgTaskMgr.StartTaskWithPath(migrationID, corebridge.BackgroundTaskTypeCopyRetry, "")
	go func() {
		_, runErr := setup.mig.RunCopyRetry(setup.cfg, opts)
		m.finishRuntimeAfterBackgroundPhase(migrationID, setup.mig, runErr)
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

func (m *Manager) buildCopyPhaseOptions(config corebridge.SweepConfigRequest) migration.CopyPhaseOptions {
	workerCount, maxRetries, logAddress, logLevel, skipListener := m.sweepConfigFields(config)
	return migration.CopyPhaseOptions{
		WorkerCount:  workerCount,
		MaxRetries:   maxRetries,
		LogAddress:   logAddress,
		LogLevel:     logLevel,
		SkipListener: skipListener,
	}
}

func (m *Manager) triggerDeleteStartResume(migrationID string) (corebridge.SweepResponse, error) {
	setup, early, done, err := m.preparePhaseResume(migrationID, false)
	if err != nil || done {
		return early, err
	}
	m.setRuntimePhaseStatus(migrationID, migration.PhaseDeleting)
	go func() {
		_, runErr := setup.mig.StartDelete(setup.cfg)
		m.finishRuntimeAfterBackgroundPhase(migrationID, setup.mig, runErr)
	}()
	return corebridge.SweepResponse{Success: true, Message: "Delete phase resume started"}, nil
}

func (m *Manager) triggerDeleteRetryResume(migrationID string, config corebridge.SweepConfigRequest) (corebridge.SweepResponse, error) {
	setup, early, done, err := m.preparePhaseResume(migrationID, false)
	if err != nil || done {
		return early, err
	}
	opts := m.buildCopyPhaseOptions(config)
	if err := setup.mig.PreparePhase(migration.PreparePhaseDeleteRetry); err != nil {
		return corebridge.SweepResponse{Success: false, Error: err.Error()}, err
	}
	m.setRuntimePhaseStatus(migrationID, migration.PhaseDeleting)
	go func() {
		_, runErr := setup.mig.RunDeleteRetry(setup.cfg, opts)
		m.finishRuntimeAfterBackgroundPhase(migrationID, setup.mig, runErr)
	}()
	return corebridge.SweepResponse{Success: true, Message: "Delete retry started"}, nil
}
