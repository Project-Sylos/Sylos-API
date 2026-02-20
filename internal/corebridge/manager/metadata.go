package manager

import (
	"context"
	"fmt"
	"sort"
	"time"

	"codeberg.org/Sylos/Sylos-API/internal/corebridge"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge/metadata"
)

func (m *Manager) GetMigrationMetadata(ctx context.Context, migrationID string) (corebridge.MigrationMetadata, error) {
	metaMgr := metadata.NewManager(m.cfg.Runtime.DataDir)
	meta, err := metaMgr.GetMigrationMetadata(migrationID)
	if err != nil {
		return corebridge.MigrationMetadata{}, err
	}
	return convertMetadata(meta), nil
}

func (m *Manager) UpdateMigrationName(ctx context.Context, migrationID, name string) error {
	metaMgr := metadata.NewManager(m.cfg.Runtime.DataDir)
	meta, err := metaMgr.GetMigrationMetadata(migrationID)
	if err != nil {
		return err
	}
	meta.Name = name
	return metaMgr.UpdateMigrationMetadata(meta)
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

	metaMgr := metadata.NewManager(m.cfg.Runtime.DataDir)
	allMeta, err := metaMgr.LoadAllMetadata()
	if err != nil {
		return corebridge.ListMigrationsResponse{}, fmt.Errorf("failed to load migration metadata: %w", err)
	}

	total := len(allMeta.Migrations)
	statuses := make([]corebridge.Status, 0, total)

	for id, meta := range allMeta.Migrations {
		status, err := m.GetMigrationStatus(ctx, id)
		if err != nil {
			status = corebridge.Status{
				Migration: corebridge.Migration{
					ID:            meta.ID,
					SourceID:      "",
					DestinationID: "",
					StartedAt:     meta.CreatedAt,
					Status:        "",
				},
				CompletedAt: nil,
				Error:       "",
				Result:      nil,
			}

			plan := m.rootsMgr.GetPlan(id)
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

	sort.Slice(statuses, func(i, j int) bool {
		timeI := statuses[i].StartedAt
		timeJ := statuses[j].StartedAt
		if timeI.IsZero() {
			timeI = time.Time{}
		}
		if timeJ.IsZero() {
			timeJ = time.Time{}
		}
		return timeI.After(timeJ)
	})

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
