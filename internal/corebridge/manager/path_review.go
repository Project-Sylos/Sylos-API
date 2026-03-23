package manager

import (
	"context"

	"codeberg.org/Sylos/Migration-Engine/pkg/migration"
)

// PathReviewContext holds the context needed for path review operations.
type PathReviewContext struct {
	MigrationID string
	ReviewPhase string
}

func (m *Manager) preparePathReviewContext(_ context.Context, migrationID string) (*PathReviewContext, error) {
	mig, err := m.GetMigration(context.TODO(), migrationID)
	if err != nil {
		return nil, err
	}
	reviewPhase := "traversal"
	if mig.Phase() == migration.PhaseCopying || mig.Phase() == migration.PhaseCopySuspended || mig.Phase() == migration.PhaseCopyReview {
		reviewPhase = "copy"
	}

	return &PathReviewContext{
		MigrationID: migrationID,
		ReviewPhase: reviewPhase,
	}, nil
}
