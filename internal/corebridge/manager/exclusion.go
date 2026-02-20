package manager

import (
	"context"
	"fmt"

	"codeberg.org/Sylos/Sylos-API/internal/corebridge"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge/database"
)

func (m *Manager) ExcludeNodes(ctx context.Context, migrationID string, req corebridge.ExclusionRequest) (*corebridge.ExclusionResponse, error) {
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

	if prc.ReviewPhase == "copy" {
		return &corebridge.ExclusionResponse{
			Success: false,
			Error:   "exclusion operations are not available in copy phase (exclusion only applies to traversal)",
		}, fmt.Errorf("exclusion operations are locked in copy phase")
	}

	if req.All {
		if req.Filter != nil && req.Filter.Status == "failed" {
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
				TaskID:  taskID,
			}, nil
		}
		pendingPaths, err := database.GetAllNodesByStatusDuckDB(ctx, m.logger, prc.DuckDBConn, "pending")
		if err != nil {
			return &corebridge.ExclusionResponse{
				Success: false,
				Error:   fmt.Sprintf("failed to get pending nodes: %v", err),
			}, err
		}
		failedPaths, err := database.GetAllNodesByStatusDuckDB(ctx, m.logger, prc.DuckDBConn, "failed")
		if err != nil {
			return &corebridge.ExclusionResponse{
				Success: false,
				Error:   fmt.Sprintf("failed to get failed nodes: %v", err),
			}, err
		}
		allPaths := make(map[string]bool)
		for _, p := range pendingPaths {
			allPaths[p] = true
		}
		for _, p := range failedPaths {
			allPaths[p] = true
		}
		req.NodeIDs = make([]string, 0, len(allPaths))
		for p := range allPaths {
			req.NodeIDs = append(req.NodeIDs, p)
		}
	}

	for _, nodeID := range req.NodeIDs {
		nodePath, err := m.findNodePathFromID(ctx, prc, nodeID)
		if err != nil {
			m.logger.Warn().Err(err).Str("node_id", nodeID).Msg("failed to find node path, skipping")
			continue
		}

		err = database.SetNodeExclusionDuckDB(ctx, m.logger, prc.DuckDBConn, nodePath, true)
		if err != nil {
			m.logger.Warn().Err(err).Str("node_path", nodePath).Msg("failed to set node exclusion, skipping")
			continue
		}

		taskID := m.bgTaskMgr.StartTaskWithPath(prc.MigrationID, corebridge.BackgroundTaskTypeExclusionPropagate, nodePath)
		go func(path string) {
			defer m.bgTaskMgr.CompleteTask(prc.MigrationID, taskID)
			if err := database.PropagateExclusionDuckDB(context.Background(), m.logger, prc.DuckDBConn, path, true); err != nil {
				m.logger.Error().Err(err).Str("migration_id", prc.MigrationID).Str("node_path", path).Msg("failed to propagate exclusion")
				m.bgTaskMgr.FailTask(prc.MigrationID, taskID, err)
			}
		}(nodePath)
	}

	if err := m.markPathReviewChanges(migrationID, true); err != nil {
		m.logger.Warn().Err(err).Str("migration_id", migrationID).Msg("failed to mark path review changes")
	}

	return &corebridge.ExclusionResponse{
		Success: true,
	}, nil
}

func (m *Manager) UnexcludeNodes(ctx context.Context, migrationID string, req corebridge.ExclusionRequest) (*corebridge.ExclusionResponse, error) {
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

	if prc.ReviewPhase == "copy" {
		return &corebridge.ExclusionResponse{
			Success: false,
			Error:   "exclusion operations are not available in copy phase (exclusion only applies to traversal)",
		}, fmt.Errorf("exclusion operations are locked in copy phase")
	}

	if req.All {
		explicitPaths, err := database.GetAllNodesByStatusDuckDB(ctx, m.logger, prc.DuckDBConn, "exclusion_explicit")
		if err != nil {
			return &corebridge.ExclusionResponse{
				Success: false,
				Error:   fmt.Sprintf("failed to get exclusion_explicit nodes: %v", err),
			}, err
		}
		inheritedPaths, err := database.GetAllNodesByStatusDuckDB(ctx, m.logger, prc.DuckDBConn, "exclusion_inherited")
		if err != nil {
			return &corebridge.ExclusionResponse{
				Success: false,
				Error:   fmt.Sprintf("failed to get exclusion_inherited nodes: %v", err),
			}, err
		}
		allPaths := make(map[string]bool)
		for _, p := range explicitPaths {
			allPaths[p] = true
		}
		for _, p := range inheritedPaths {
			allPaths[p] = true
		}
		req.NodeIDs = make([]string, 0, len(allPaths))
		for p := range allPaths {
			req.NodeIDs = append(req.NodeIDs, p)
		}
	}

	for _, nodeID := range req.NodeIDs {
		nodePath, err := m.findNodePathFromID(ctx, prc, nodeID)
		if err != nil {
			m.logger.Warn().Err(err).Str("node_id", nodeID).Msg("failed to find node path, skipping")
			continue
		}

		err = database.SetNodeExclusionDuckDB(ctx, m.logger, prc.DuckDBConn, nodePath, false)
		if err != nil {
			m.logger.Warn().Err(err).Str("node_path", nodePath).Msg("failed to set node unexclusion, skipping")
			continue
		}

		taskID := m.bgTaskMgr.StartTaskWithPath(prc.MigrationID, corebridge.BackgroundTaskTypeUnexclusionPropagate, nodePath)
		go func(path string) {
			defer m.bgTaskMgr.CompleteTask(prc.MigrationID, taskID)
			if err := database.PropagateExclusionDuckDB(context.Background(), m.logger, prc.DuckDBConn, path, false); err != nil {
				m.logger.Error().Err(err).Str("migration_id", prc.MigrationID).Str("node_path", path).Msg("failed to propagate unexclusion")
				m.bgTaskMgr.FailTask(prc.MigrationID, taskID, err)
			}
		}(nodePath)
	}

	if err := m.markPathReviewChanges(migrationID, true); err != nil {
		m.logger.Warn().Err(err).Str("migration_id", migrationID).Msg("failed to mark path review changes")
	}

	return &corebridge.ExclusionResponse{
		Success: true,
	}, nil
}
