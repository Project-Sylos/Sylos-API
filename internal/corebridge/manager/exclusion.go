package manager

import (
	"context"
	"fmt"

	"codeberg.org/Sylos/Migration-Engine/pkg/migration"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge"
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

	mig, err := m.engineMgr.GetMigration(prc.MigrationID)
	if err != nil {
		return &corebridge.ExclusionResponse{Success: false, Error: err.Error()}, err
	}
	if mig == nil {
		return &corebridge.ExclusionResponse{Success: false, Error: "migration not found"}, corebridge.ErrMigrationNotFound
	}

	if req.All {
		filter := migration.NodeQueryFilter{
			Queue:  "SRC",
			Limit:  1000,
			Offset: 0,
		}
		if req.Filter != nil && req.Filter.Status != "" {
			filter.Status = req.Filter.Status
		}
		updated, err := mig.BulkExcludeWithPropagation(filter, true)
		if err != nil {
			return &corebridge.ExclusionResponse{
				Success: false,
				Error:   err.Error(),
			}, err
		}
		_ = updated
		return &corebridge.ExclusionResponse{Success: true}, nil
	}

	for _, nodeID := range req.NodeIDs {
		if err := mig.SetNodeExcludedWithPropagation("SRC", nodeID, true); err != nil {
			_ = mig.SetNodeExcludedWithPropagation("DST", nodeID, true)
		}
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

	mig, err := m.engineMgr.GetMigration(prc.MigrationID)
	if err != nil {
		return &corebridge.ExclusionResponse{Success: false, Error: err.Error()}, err
	}
	if mig == nil {
		return &corebridge.ExclusionResponse{Success: false, Error: "migration not found"}, corebridge.ErrMigrationNotFound
	}

	if req.All {
		updated, err := mig.BulkExcludeWithPropagation(migration.NodeQueryFilter{
			Queue:    "SRC",
			Excluded: ptrBool(true),
			Limit:    1000,
		}, false)
		if err != nil {
			return &corebridge.ExclusionResponse{
				Success: false,
				Error:   err.Error(),
			}, err
		}
		_ = updated
		return &corebridge.ExclusionResponse{Success: true}, nil
	}

	for _, nodeID := range req.NodeIDs {
		if err := mig.SetNodeExcludedWithPropagation("SRC", nodeID, false); err != nil {
			_ = mig.SetNodeExcludedWithPropagation("DST", nodeID, false)
		}
	}

	if err := m.markPathReviewChanges(migrationID, true); err != nil {
		m.logger.Warn().Err(err).Str("migration_id", migrationID).Msg("failed to mark path review changes")
	}

	return &corebridge.ExclusionResponse{
		Success: true,
	}, nil
}

func ptrBool(v bool) *bool { return &v }
