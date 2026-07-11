package manager

import (
	"context"
	"fmt"

	"codeberg.org/Sylos/Sylos-API/internal/corebridge"
)

// GetDeleteSummary returns source path and delete status counts for the confirmation modal.
func (m *Manager) GetDeleteSummary(ctx context.Context, migrationID string) (corebridge.DeleteSummaryResponse, error) {
	mig, err := m.GetMigration(ctx, migrationID)
	if err != nil {
		return corebridge.DeleteSummaryResponse{}, err
	}
	plan := m.rootsMgr.GetPlan(migrationID)
	sourceRoot := ""
	sourceHost := ""
	if plan != nil && plan.HasSource {
		sourceRoot = plan.SourceRoot.LocationPath
		sourceHost = plan.SourceDefinition.Name
		if sourceHost == "" {
			sourceHost = plan.SourceDefinition.ID
		}
	}
	summary, err := mig.GetDeleteSummary(sourceRoot)
	if err != nil {
		return corebridge.DeleteSummaryResponse{}, fmt.Errorf("delete summary: %w", err)
	}
	return corebridge.DeleteSummaryResponse{
		SourceRootPath: summary.SourceRootPath,
		SourceHost:     sourceHost,
		Pending:        summary.Pending,
		Failed:         summary.Failed,
		Deleted:        summary.Deleted,
	}, nil
}
