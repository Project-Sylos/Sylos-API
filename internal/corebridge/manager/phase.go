package manager

import (
	"context"
	"fmt"
	"strings"

	"codeberg.org/Sylos/Migration-Engine/pkg/migration"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge/database"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge/metadata"
)

func (m *Manager) getMigrationPhase(migrationID string) (string, error) {
	metaMgr := metadata.NewManager(m.cfg.Runtime.DataDir)
	meta, err := metaMgr.GetMigrationMetadata(migrationID)
	if err != nil {
		return "roots", nil
	}

	if meta.ConfigPath == "" {
		return "roots", nil
	}

	yamlCfg, err := migration.LoadMigrationConfig(meta.ConfigPath)
	if err != nil {
		return "unknown", err
	}

	status := strings.TrimSpace(yamlCfg.State.Status)

	switch {
	case status == "" || status == "Roots-Set":
		return "roots", nil
	case strings.Contains(status, "Traversal") || status == "Awaiting-Path-Review" || status == "Filters-Set":
		return "traversal", nil
	case strings.Contains(status, "Copy") || status == "Awaiting-Copy-Review" || status == "Preparing-For-Copy":
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

func (m *Manager) CheckPendingWork(ctx context.Context, migrationID string) (corebridge.PendingWorkResponse, error) {
	prc, err := m.preparePathReviewContext(ctx, migrationID)
	if err != nil {
		if err == corebridge.ErrMigrationNotFound {
			return corebridge.PendingWorkResponse{}, err
		}
		return corebridge.PendingWorkResponse{
			HasPendingRetries:    false,
			HasPathReviewChanges: false,
			PendingRetriesCount:  0,
		}, nil
	}

	retriesCount, err := database.CountPendingRetriesDuckDB(ctx, m.logger, prc.DuckDBConn)
	if err != nil {
		return corebridge.PendingWorkResponse{}, fmt.Errorf("failed to count pending retries: %w", err)
	}

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
