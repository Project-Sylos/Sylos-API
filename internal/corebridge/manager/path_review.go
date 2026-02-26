package manager

import (
	"context"

	"codeberg.org/Sylos/Sylos-API/internal/corebridge"
)

// PathReviewContext holds the context needed for path review operations.
type PathReviewContext struct {
	MigrationID string
	ReviewPhase string
}

func (m *Manager) preparePathReviewContext(_ context.Context, migrationID string) (*PathReviewContext, error) {
	mig, err := m.engineMgr.GetMigration(migrationID)
	if err != nil {
		return nil, err
	}
	if mig == nil {
		return nil, corebridge.ErrMigrationNotFound
	}
	reviewPhase := "traversal"
	if mig.Phase().String() == "copying" || mig.Phase().String() == "completed" {
		reviewPhase = "copy"
	}

	return &PathReviewContext{
		MigrationID: migrationID,
		ReviewPhase: reviewPhase,
	}, nil
}
