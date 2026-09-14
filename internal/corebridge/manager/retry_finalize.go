package manager

import (
	"context"
	"fmt"
	"time"

	"codeberg.org/Sylos/Migration-Engine/pkg/migration"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge"
)

// RetryFinalize retries seal stop / secondary indexes / checkpoint after *-finalize-failed.
func (m *Manager) RetryFinalize(ctx context.Context, migrationID string, config corebridge.SweepConfigRequest) (corebridge.SweepResponse, error) {
	mig, err := m.GetMigration(ctx, migrationID)
	if err != nil {
		return corebridge.SweepResponse{Success: false, Error: err.Error()}, err
	}
	if !migration.IsFinalizeFailedPhase(mig.Phase()) {
		msg := fmt.Sprintf("retry finalize requires *-finalize-failed phase, got %s", mig.Phase())
		return corebridge.SweepResponse{Success: false, Error: msg}, fmt.Errorf("%s", msg)
	}
	if resp, ok := m.resumeIfAlreadyRunning(migrationID, mig); ok {
		return resp, nil
	}

	opts := migration.FinalizeOverrides{
		Threads:       config.Threads,
		MemoryLimitGB: config.MemoryLimitGB,
	}

	m.mu.Lock()
	now := time.Now().UTC()
	if rec := m.runtimeByID[migrationID]; rec != nil {
		rec.StartedAt = now
		rec.CompletedAt = nil
		rec.Status = mig.Phase()
		rec.Error = ""
	} else {
		m.runtimeByID[migrationID] = &runtimeMigration{
			StartedAt: now,
			Status:    mig.Phase(),
		}
	}
	m.mu.Unlock()

	go func() {
		runErr := mig.RetryFinalize(opts)
		m.mu.Lock()
		defer m.mu.Unlock()
		if rec := m.runtimeByID[migrationID]; rec != nil {
			doneAt := time.Now().UTC()
			rec.CompletedAt = &doneAt
			applyRunResultToRuntime(rec, mig, runErr)
		}
	}()

	return corebridge.SweepResponse{
		Success: true,
		Message: "Finalize retry started",
	}, nil
}
