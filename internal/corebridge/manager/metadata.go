package manager

import (
	"context"
	"fmt"
	"time"

	"codeberg.org/Sylos/Sylos-API/internal/corebridge"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge/apidb"
)

func (m *Manager) GetMigrationMetadata(ctx context.Context, migrationID string) (corebridge.MigrationMetadata, error) {
	rec, err := m.getMigrationRecord(migrationID)
	if err != nil {
		return corebridge.MigrationMetadata{}, err
	}
	return corebridge.MigrationMetadata{
		ID:           rec.ID,
		Name:         rec.Name,
		DatabasePath: rec.DatabasePath,
		CreatedAt:    rec.CreatedAt,
	}, nil
}

func (m *Manager) UpdateMigrationName(ctx context.Context, migrationID, name string) error {
	rec, err := m.getMigrationRecord(migrationID)
	if err != nil {
		return err
	}
	rec.Name = name
	return m.upsertMigrationRecord(rec)
}

func (m *Manager) ListAllMigrations(ctx context.Context, req corebridge.ListMigrationsRequest) (corebridge.ListMigrationsResponse, error) {
	offset := req.Offset
	if offset < 0 {
		offset = 0
	}
	limit := req.Limit
	if limit <= 0 {
		limit = 100
	}
	if limit > 1000 {
		limit = 1000
	}

	if m.apiDB == nil {
		return corebridge.ListMigrationsResponse{}, fmt.Errorf("API database not configured")
	}
	allRecs, err := m.apiDB.ListMigrations()
	if err != nil {
		return corebridge.ListMigrationsResponse{}, fmt.Errorf("failed to load migration metadata: %w", err)
	}

	total := len(allRecs)
	statuses := make([]corebridge.Status, 0, total)

	for _, rec := range allRecs {
		status, err := m.GetMigrationStatus(ctx, rec.ID)
		if err != nil {
			status = corebridge.Status{
				Migration: corebridge.Migration{
					ID:            rec.ID,
					SourceID:      "",
					DestinationID: "",
					StartedAt:     rec.CreatedAt,
					Status:        "",
				},
				CompletedAt: nil,
				Error:       "",
				Result:      nil,
			}

			plan := m.rootsMgr.GetPlan(rec.ID)
			if plan != nil {
				if plan.HasSource {
					status.SourceID = plan.SourceDefinition.ID
				}
				if plan.HasDestination {
					status.DestinationID = plan.DestinationDefinition.ID
				}
				if plan.HasSource && plan.HasDestination {
					status.Status = "Roots-Set"
				} else if plan.HasSource || plan.HasDestination {
					status.Status = "Roots-Partial"
				}
			}
		}

		statuses = append(statuses, status)
	}

	sortStatusesByTime(statuses)

	hasMore := offset+limit < total
	end := offset + limit
	if end > total {
		end = total
	}

	var paginatedStatuses []corebridge.Status
	if offset < total {
		paginatedStatuses = statuses[offset:end]
	} else {
		paginatedStatuses = []corebridge.Status{}
	}

	return corebridge.ListMigrationsResponse{
		Migrations: paginatedStatuses,
		Total:      total,
		Offset:     offset,
		Limit:      limit,
		HasMore:    hasMore,
	}, nil
}

func sortStatusesByTime(statuses []corebridge.Status) {
	for i := 0; i < len(statuses); i++ {
		for j := i + 1; j < len(statuses); j++ {
			timeI := statuses[i].StartedAt
			timeJ := statuses[j].StartedAt
			if timeI.IsZero() {
				timeI = time.Time{}
			}
			if timeJ.IsZero() {
				timeJ = time.Time{}
			}
			if timeJ.After(timeI) {
				statuses[i], statuses[j] = statuses[j], statuses[i]
			}
		}
	}
}

func (m *Manager) APIDB() *apidb.DB {
	return m.apiDB
}
