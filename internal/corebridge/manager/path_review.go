package manager

import (
	"context"
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
	if mig.Phase().String() == "copying" || mig.Phase().String() == "completed" {
		reviewPhase = "copy"
	}

	return &PathReviewContext{
		MigrationID: migrationID,
		ReviewPhase: reviewPhase,
	}, nil
}
