package manager

import (
	"context"
	"fmt"

	"codeberg.org/Sylos/Sylos-API/internal/corebridge"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge/database"
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

	taskID := m.bgTaskMgr.StartTask(prc.MigrationID, corebridge.BackgroundTaskTypeRetryAll)
	go func() {
		defer m.bgTaskMgr.CompleteTask(prc.MigrationID, taskID)
		if err := database.RetryAllFailedDuckDB(context.Background(), m.logger, prc.DuckDBConn); err != nil {
			m.logger.Error().Err(err).Str("migration_id", prc.MigrationID).Msg("failed to retry all failed items")
			m.bgTaskMgr.FailTask(prc.MigrationID, taskID, err)
			return
		}

		if err := m.markPathReviewChanges(migrationID, true); err != nil {
			m.logger.Warn().Err(err).Str("migration_id", migrationID).Msg("failed to mark path review changes")
		}
	}()

	return &corebridge.MarkRetryResponse{
		Success: true,
	}, nil
}

func (m *Manager) MarkAllFailedAsExcluded(ctx context.Context, migrationID string) (*corebridge.ExclusionResponse, error) {
	prc, err := m.preparePathReviewContext(ctx, migrationID)
	if err != nil {
		if err == corebridge.ErrMigrationNotFound {
			return &corebridge.ExclusionResponse{
				Success: false,
				Error:   "migration not found",
			}, err
		}
		return &corebridge.ExclusionResponse{
			Success: false,
			Error:   err.Error(),
		}, err
	}

	taskID := m.bgTaskMgr.StartTask(prc.MigrationID, corebridge.BackgroundTaskTypeExclusionSweep)
	go func() {
		defer m.bgTaskMgr.CompleteTask(prc.MigrationID, taskID)
		if err := database.MarkAllFailedAsExcludedDuckDB(context.Background(), m.logger, prc.DuckDBConn); err != nil {
			m.logger.Error().Err(err).Str("migration_id", prc.MigrationID).Msg("failed to mark all failed items as excluded")
			m.bgTaskMgr.FailTask(prc.MigrationID, taskID, err)
			return
		}

		if err := m.markPathReviewChanges(migrationID, true); err != nil {
			m.logger.Warn().Err(err).Str("migration_id", migrationID).Msg("failed to mark path review changes")
		}
	}()

	return &corebridge.ExclusionResponse{
		Success: true,
	}, nil
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

	for _, nodeID := range req.NodeIDs {
		nodePath, err := m.findNodePathFromID(ctx, prc, nodeID)
		if err != nil {
			m.logger.Warn().Err(err).Str("node_id", nodeID).Msg("failed to find node path, skipping")
			continue
		}

		err = database.MarkNodeForRetryDuckDB(ctx, m.logger, prc.DuckDBConn, nodePath)
		if err != nil {
			m.logger.Warn().Err(err).Str("node_path", nodePath).Msg("failed to mark node for discovery retry, skipping")
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

	for _, nodeID := range req.NodeIDs {
		nodePath, err := m.findNodePathFromID(ctx, prc, nodeID)
		if err != nil {
			m.logger.Warn().Err(err).Str("node_id", nodeID).Msg("failed to find node path, skipping")
			continue
		}

		err = database.MarkNodeForRetryCopyDuckDB(ctx, m.logger, prc.DuckDBConn, nodePath)
		if err != nil {
			m.logger.Warn().Err(err).Str("node_path", nodePath).Msg("failed to mark node for copy retry, skipping")
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

	nodePath, err := m.findNodePathFromID(ctx, prc, nodeID)
	if err != nil {
		return &corebridge.MarkRetryResponse{
			Success: false,
			Error:   fmt.Sprintf("node not found: %v", err),
		}, err
	}

	err = database.UnmarkNodeForRetryDuckDB(ctx, m.logger, prc.DuckDBConn, nodePath)
	if err != nil {
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

	nodePath, err := m.findNodePathFromID(ctx, prc, nodeID)
	if err != nil {
		return &corebridge.MarkRetryResponse{
			Success: false,
			Error:   fmt.Sprintf("node not found: %v", err),
		}, err
	}

	err = database.UnmarkNodeForRetryCopyDuckDB(ctx, m.logger, prc.DuckDBConn, nodePath)
	if err != nil {
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
	prc, err := m.preparePathReviewContext(ctx, migrationID)
	if err != nil {
		if err == corebridge.ErrMigrationNotFound {
			return nil, err
		}
		return nil, err
	}

	stats, err := database.GetPathReviewStatsDuckDB(ctx, m.logger, prc.DuckDBConn)
	if err != nil {
		return nil, fmt.Errorf("failed to get path review stats: %w", err)
	}

	return &corebridge.PathReviewStats{
		TraversalStatusCounts: stats.TraversalStatusCounts,
		CopyStatusCounts:      stats.CopyStatusCounts,
		ExcludedCount:         stats.ExcludedCount,
		FoldersCount:          stats.FoldersCount,
		FilesCount:            stats.FilesCount,
		FoldersRatio:          stats.FoldersRatio,
		FilesRatio:            stats.FilesRatio,
		TotalFileSize: corebridge.FileSizeStats{
			Src: stats.TotalFileSize.Src,
			Dst: stats.TotalFileSize.Dst,
		},
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
