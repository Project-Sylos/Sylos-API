package manager

import (
	"context"

	"codeberg.org/Sylos/Sylos-API/internal/corebridge"
)

func (m *Manager) RetryAllFailed(ctx context.Context, migrationID string) (*corebridge.MarkRetryResponse, error) {
	prc, err := m.preparePathReviewContext(ctx, migrationID)
	if err != nil {
		if err == corebridge.ErrMigrationNotFound {
			return &corebridge.MarkRetryResponse{
				Success: false,
				Error:   "migration not found",
				Deltas:  map[string]int64{},
			}, err
		}
		return &corebridge.MarkRetryResponse{
			Success: false,
			Error:   err.Error(),
			Deltas:  map[string]int64{},
		}, err
	}
	mig, err := m.GetMigration(ctx, prc.MigrationID)
	if err != nil {
		return &corebridge.MarkRetryResponse{Success: false, Error: err.Error(), Deltas: map[string]int64{}}, err
	}
	res, err := mig.RetryAllFailed()
	if err != nil {
		return &corebridge.MarkRetryResponse{Success: false, Error: err.Error(), Deltas: map[string]int64{}}, err
	}
	if err := m.MarkPathReviewChanges(ctx, migrationID, true); err != nil {
		m.logger.Warn().Err(err).Str("migration_id", migrationID).Msg("failed to mark path review changes")
	}
	deltas := res.Deltas
	if deltas == nil {
		deltas = map[string]int64{}
	}
	return &corebridge.MarkRetryResponse{
		Success:       true,
		AffectedCount: res.AffectedCount,
		Deltas:        deltas,
	}, nil
}

func (m *Manager) MarkAllFailedAsExcluded(ctx context.Context, migrationID string) (*corebridge.ExclusionResponse, error) {
	mig, err := m.GetMigration(context.TODO(), migrationID)
	if err != nil {
		if err == corebridge.ErrMigrationNotFound {
			return &corebridge.ExclusionResponse{Success: false, Error: "migration not found", Deltas: map[string]int64{}}, err
		}
		return &corebridge.ExclusionResponse{Success: false, Error: err.Error(), Deltas: map[string]int64{}}, err
	}
	req := corebridge.ExclusionRequest{
		All: true,
		Filter: &struct {
			Status string `json:"status,omitempty"`
		}{Status: "failed"},
	}
	resp, err := corebridge.ExcludeNodes(mig, req)
	if err != nil {
		return resp, err
	}
	if resp.Success {
		_ = m.MarkPathReviewChanges(context.TODO(), migrationID, true)
	}
	return resp, nil
}

func (m *Manager) GetBackgroundTasks(ctx context.Context, migrationID string) ([]corebridge.BackgroundTask, error) {
	return m.bgTaskMgr.GetTasks(migrationID), nil
}

func (m *Manager) GetRunningBackgroundTasks(ctx context.Context, migrationID string) ([]corebridge.BackgroundTask, error) {
	return m.bgTaskMgr.GetRunningTasks(migrationID), nil
}

func (m *Manager) GetBackgroundTask(ctx context.Context, migrationID, taskID string) (*corebridge.BackgroundTask, error) {
	return m.bgTaskMgr.GetTask(migrationID, taskID)
}
