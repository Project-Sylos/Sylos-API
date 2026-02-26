package manager

import (
	"context"
	"fmt"

	"codeberg.org/Sylos/Migration-Engine/pkg/migration"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge/metadata"
)

func (m *Manager) getMigrationPhase(migrationID string) (string, error) {
	mig, err := m.engineMgr.GetMigration(migrationID)
	if err != nil {
		return "unknown", err
	}
	if mig == nil {
		return "roots", nil
	}
	switch mig.Phase().String() {
	case "created":
		return "roots", nil
	case "traversing", "review":
		return "traversal", nil
	case "copying", "completed":
		return "copy", nil
	default:
		return "unknown", nil
	}
}

func (m *Manager) checkPhaseLock(migrationID string, operation string) error {
	switch operation {
	case "setRoot":
		plan := m.rootsMgr.GetPlan(migrationID)
		if plan != nil && plan.HasSource && plan.HasDestination {
			phase, err := m.getMigrationPhase(migrationID)
			if err != nil {
				return fmt.Errorf("failed to determine migration phase: %w", err)
			}
			if phase == "traversal" || phase == "copy" {
				return fmt.Errorf("root selection is locked: migration is in %s phase", phase)
			}
		}
		return nil
	case "startTraversal", "retrySweep", "exclude", "unexclude", "markRetry":
		phase, err := m.getMigrationPhase(migrationID)
		if err != nil {
			return fmt.Errorf("failed to determine migration phase: %w", err)
		}
		if operation == "exclude" || operation == "unexclude" {
			if phase == "copy" {
				return fmt.Errorf("exclusion operations are locked: migration is in copy phase (exclusion only applies to traversal)")
			}
		} else if phase == "copy" {
			return fmt.Errorf("traversal operations are locked: migration is in copy phase")
		}
	}

	return nil
}

func (m *Manager) markPathReviewChanges(migrationID string, hasChanges bool) error {
	metaMgr := metadata.NewManager(m.cfg.Runtime.DataDir)
	meta, err := metaMgr.GetMigrationMetadata(migrationID)
	if err != nil {
		meta = metadata.MigrationMetadata{
			ID:                   migrationID,
			Name:                 migrationID,
			HasPathReviewChanges: hasChanges,
		}
	} else {
		meta.HasPathReviewChanges = hasChanges
	}

	return metaMgr.UpdateMigrationMetadata(meta)
}

func (m *Manager) CheckPendingWork(_ context.Context, migrationID string) (corebridge.PendingWorkResponse, error) {
	mig, err := m.engineMgr.GetMigration(migrationID)
	if err != nil {
		return corebridge.PendingWorkResponse{}, err
	}
	if mig == nil {
		return corebridge.PendingWorkResponse{}, corebridge.ErrMigrationNotFound
	}

	srcPending, err := mig.QueryNodes(migration.NodeQueryFilter{
		Queue:  "SRC",
		Status: "pending",
		Limit:  1000,
	})
	if err != nil {
		return corebridge.PendingWorkResponse{}, fmt.Errorf("failed to query pending source nodes: %w", err)
	}
	dstPending, err := mig.QueryNodes(migration.NodeQueryFilter{
		Queue:  "DST",
		Status: "pending",
		Limit:  1000,
	})
	if err != nil {
		return corebridge.PendingWorkResponse{}, fmt.Errorf("failed to query pending destination nodes: %w", err)
	}
	retriesCount := len(srcPending) + len(dstPending)

	metaMgr := metadata.NewManager(m.cfg.Runtime.DataDir)
	meta, err := metaMgr.GetMigrationMetadata(migrationID)
	hasUnsavedChanges := false
	if err == nil {
		hasUnsavedChanges = meta.HasPathReviewChanges
	}

	return corebridge.PendingWorkResponse{
		HasPendingRetries:    retriesCount > 0,
		HasPathReviewChanges: hasUnsavedChanges,
		PendingRetriesCount:  retriesCount,
	}, nil
}
