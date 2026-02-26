package manager

import (
	"context"
	"fmt"

	"codeberg.org/Sylos/Sylos-API/internal/corebridge"
)

func (m *Manager) RetryAllFailed(ctx context.Context, migrationID string) (*corebridge.MarkRetryResponse, error) {
	prc, err := m.preparePathReviewContext(ctx, migrationID)
	if err != nil {
		if err == corebridge.ErrMigrationNotFound {
			return &corebridge.MarkRetryResponse{
				Success: false,
				Error:   "migration not found",
			}, err
		}
		return &corebridge.MarkRetryResponse{
			Success: false,
			Error:   err.Error(),
		}, err
	}
	mig, err := m.engineMgr.GetMigration(prc.MigrationID)
	if err != nil {
		return &corebridge.MarkRetryResponse{Success: false, Error: err.Error()}, err
	}
	if mig == nil {
		return &corebridge.MarkRetryResponse{Success: false, Error: "migration not found"}, corebridge.ErrMigrationNotFound
	}
	if err := mig.RetryAllFailed(); err != nil {
		return &corebridge.MarkRetryResponse{Success: false, Error: err.Error()}, err
	}
	if err := m.markPathReviewChanges(migrationID, true); err != nil {
		m.logger.Warn().Err(err).Str("migration_id", migrationID).Msg("failed to mark path review changes")
	}

	return &corebridge.MarkRetryResponse{
		Success: true,
	}, nil
}

func (m *Manager) MarkAllFailedAsExcluded(ctx context.Context, migrationID string) (*corebridge.ExclusionResponse, error) {
	return m.ExcludeNodes(ctx, migrationID, corebridge.ExclusionRequest{
		All: true,
		Filter: &struct {
			Status string `json:"status,omitempty"`
		}{Status: "failed"},
	})
}

func (m *Manager) MarkNodesForRetryDiscovery(ctx context.Context, migrationID string, req corebridge.MarkRetryRequest) (*corebridge.MarkRetryResponse, error) {
	prc, err := m.preparePathReviewContext(ctx, migrationID)
	if err != nil {
		if err == corebridge.ErrMigrationNotFound {
			return &corebridge.MarkRetryResponse{
				Success: false,
				Error:   "migration not found",
			}, err
		}
		return &corebridge.MarkRetryResponse{
			Success: false,
			Error:   err.Error(),
		}, err
	}
	mig, err := m.engineMgr.GetMigration(prc.MigrationID)
	if err != nil {
		return &corebridge.MarkRetryResponse{Success: false, Error: err.Error()}, err
	}
	if mig == nil {
		return &corebridge.MarkRetryResponse{Success: false, Error: "migration not found"}, corebridge.ErrMigrationNotFound
	}

	for _, nodeID := range req.NodeIDs {
		if err := mig.MarkNodeForRetryDiscovery(nodeID); err != nil {
			m.logger.Warn().Err(err).Str("node_id", nodeID).Msg("failed to mark node for discovery retry, skipping")
			continue
		}
	}

	if err := m.markPathReviewChanges(migrationID, true); err != nil {
		m.logger.Warn().Err(err).Str("migration_id", migrationID).Msg("failed to mark path review changes")
	}

	return &corebridge.MarkRetryResponse{
		Success: true,
	}, nil
}

func (m *Manager) MarkNodesForRetryCopy(ctx context.Context, migrationID string, req corebridge.MarkRetryRequest) (*corebridge.MarkRetryResponse, error) {
	prc, err := m.preparePathReviewContext(ctx, migrationID)
	if err != nil {
		if err == corebridge.ErrMigrationNotFound {
			return &corebridge.MarkRetryResponse{
				Success: false,
				Error:   "migration not found",
			}, err
		}
		return &corebridge.MarkRetryResponse{
			Success: false,
			Error:   err.Error(),
		}, err
	}
	mig, err := m.engineMgr.GetMigration(prc.MigrationID)
	if err != nil {
		return &corebridge.MarkRetryResponse{Success: false, Error: err.Error()}, err
	}
	if mig == nil {
		return &corebridge.MarkRetryResponse{Success: false, Error: "migration not found"}, corebridge.ErrMigrationNotFound
	}

	for _, nodeID := range req.NodeIDs {
		if err := mig.MarkNodeForRetryCopy(nodeID); err != nil {
			m.logger.Warn().Err(err).Str("node_id", nodeID).Msg("failed to mark node for copy retry, skipping")
			continue
		}
	}

	if err := m.markPathReviewChanges(migrationID, true); err != nil {
		m.logger.Warn().Err(err).Str("migration_id", migrationID).Msg("failed to mark path review changes")
	}

	return &corebridge.MarkRetryResponse{
		Success: true,
	}, nil
}

func (m *Manager) UnmarkNodeForRetryDiscovery(ctx context.Context, migrationID string, nodeID string) (*corebridge.MarkRetryResponse, error) {
	prc, err := m.preparePathReviewContext(ctx, migrationID)
	if err != nil {
		if err == corebridge.ErrMigrationNotFound {
			return &corebridge.MarkRetryResponse{
				Success: false,
				Error:   "migration not found",
			}, err
		}
		return &corebridge.MarkRetryResponse{
			Success: false,
			Error:   err.Error(),
		}, err
	}
	mig, err := m.engineMgr.GetMigration(prc.MigrationID)
	if err != nil {
		return &corebridge.MarkRetryResponse{Success: false, Error: err.Error()}, err
	}
	if mig == nil {
		return &corebridge.MarkRetryResponse{Success: false, Error: "migration not found"}, corebridge.ErrMigrationNotFound
	}
	if err := mig.UnmarkNodeForRetryDiscovery(nodeID); err != nil {
		return &corebridge.MarkRetryResponse{
			Success: false,
			Error:   err.Error(),
		}, err
	}

	if err := m.markPathReviewChanges(migrationID, true); err != nil {
		m.logger.Warn().Err(err).Str("migration_id", migrationID).Msg("failed to mark path review changes")
	}

	return &corebridge.MarkRetryResponse{
		Success: true,
	}, nil
}

func (m *Manager) UnmarkNodeForRetryCopy(ctx context.Context, migrationID string, nodeID string) (*corebridge.MarkRetryResponse, error) {
	prc, err := m.preparePathReviewContext(ctx, migrationID)
	if err != nil {
		if err == corebridge.ErrMigrationNotFound {
			return &corebridge.MarkRetryResponse{
				Success: false,
				Error:   "migration not found",
			}, err
		}
		return &corebridge.MarkRetryResponse{
			Success: false,
			Error:   err.Error(),
		}, err
	}
	mig, err := m.engineMgr.GetMigration(prc.MigrationID)
	if err != nil {
		return &corebridge.MarkRetryResponse{Success: false, Error: err.Error()}, err
	}
	if mig == nil {
		return &corebridge.MarkRetryResponse{Success: false, Error: "migration not found"}, corebridge.ErrMigrationNotFound
	}
	if err := mig.UnmarkNodeForRetryCopy(nodeID); err != nil {
		return &corebridge.MarkRetryResponse{
			Success: false,
			Error:   err.Error(),
		}, err
	}

	if err := m.markPathReviewChanges(migrationID, true); err != nil {
		m.logger.Warn().Err(err).Str("migration_id", migrationID).Msg("failed to mark path review changes")
	}

	return &corebridge.MarkRetryResponse{
		Success: true,
	}, nil
}

func (m *Manager) GetPathReviewStats(ctx context.Context, migrationID string) (*corebridge.PathReviewStats, error) {
	mig, err := m.engineMgr.GetMigration(migrationID)
	if err != nil {
		if err == corebridge.ErrMigrationNotFound {
			return nil, err
		}
		return nil, err
	}
	if mig == nil {
		return nil, corebridge.ErrMigrationNotFound
	}
	summary, err := mig.GetTraversalSummary()
	if err != nil {
		return nil, fmt.Errorf("failed to get traversal summary: %w", err)
	}

	return &corebridge.PathReviewStats{
		TraversalStatusCounts: map[string]int{
			"src_pending": summary.SrcPending,
			"dst_pending": summary.DstPending,
			"src_failed":  summary.SrcFailed,
			"dst_failed":  summary.DstFailed,
		},
		CopyStatusCounts: map[string]int{},
		ExcludedCount:    summary.SrcExcluded + summary.DstExcluded,
		TotalFileSize:    corebridge.FileSizeStats{},
	}, nil
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
