package manager

import (
	"context"
	"fmt"

	"codeberg.org/Sylos/Migration-Engine/pkg/migration"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge/metadata"
)

// getMigrationPhase maps the engine phase to a coarse API phase for locking/UX.
// Engine phases: roots-set, filters-set, traversal-in-progress, awaiting-traversal-review, copy-in-progress, awaiting-copy-review.
func (m *Manager) getMigrationPhase(migrationID string) (string, error) {
	mig, err := m.GetMigration(context.TODO(), migrationID)
	if err != nil {
		return "unknown", err
	}
	p := mig.Phase()
	switch p {
	case migration.PhaseCreated, migration.PhaseFiltersSet:
		return "roots", nil
	case migration.PhaseTraversing, migration.PhaseTraversalReview:
		return "traversal", nil
	case migration.PhaseCopying, migration.PhaseCopyReview:
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

func (m *Manager) CheckPendingWork(ctx context.Context, migrationID string) (corebridge.PendingWorkResponse, error) {
	mig, err := m.GetMigration(ctx, migrationID)
	if err != nil {
		return corebridge.PendingWorkResponse{}, err
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
