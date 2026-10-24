package manager

import (
	"context"
	"fmt"
	"strings"
	"time"

	"codeberg.org/Sylos/Sylos-API/internal/corebridge"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge/apidb"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge/roots"
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

	// Listing must never open a per-migration ops DB. A Badger open takes an
	// exclusive directory lock and replays the store, so opening every
	// migration just to render the resume list is slow and leaves all of
	// those DBs locked open. Migrations already loaded in this process can
	// still afford a full status read; cold ones are summarized from the
	// registry record and their last known phase. Opening a single
	// migration by ID still opens its DB on demand.
	loaded := make(map[string]bool)
	if m.engineMgr != nil {
		if summaries, err := m.engineMgr.ListMigrations(""); err == nil {
			for _, s := range summaries {
				loaded[s.ID] = true
			}
		}
	}
	m.mu.RLock()
	for id := range m.runtimeByID {
		loaded[id] = true
	}
	m.mu.RUnlock()

	total := len(allRecs)
	statuses := make([]corebridge.Status, 0, total)

	for _, rec := range allRecs {
		var status corebridge.Status
		if loaded[rec.ID] {
			status, _ = m.GetMigrationStatus(ctx, rec.ID)
		}
		if status.ID == "" {
			status = m.coldMigrationStatus(rec)
		}

		// Prefer a human-friendly registry name when status has none / only the ID.
		status.Name = preferMigrationDisplayName(status.Name, rec.Name, rec.ID)
		// Resume list "Created" uses StartedAt; keep registry creation time, not last-run start.
		if !rec.CreatedAt.IsZero() {
			status.StartedAt = rec.CreatedAt
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

// coldMigrationStatus summarizes a migration that is not loaded in this
// process, using only the registry record and in-memory root plan. It must
// not touch the migration's ops DB.
func (m *Manager) coldMigrationStatus(rec apidb.MigrationRecord) corebridge.Status {
	status := corebridge.Status{
		Migration: corebridge.Migration{
			ID:        rec.ID,
			Name:      rec.Name,
			StartedAt: rec.CreatedAt,
			Status:    rec.Phase,
		},
	}

	var plan *roots.RootPlan
	if m.rootsMgr != nil {
		plan = m.rootsMgr.GetPlan(rec.ID)
	}
	if plan != nil && plan.HasSource {
		status.SourceID = plan.SourceDefinition.ID
	}
	if plan != nil && plan.HasDestination {
		status.DestinationID = plan.DestinationDefinition.ID
	}
	if status.Status == "" && plan != nil && plan.HasSource && plan.HasDestination {
		status.Status = "Roots-Set"
	} else if status.Status == "" && plan != nil && (plan.HasSource || plan.HasDestination) {
		status.Status = "Roots-Partial"
	}
	if source, destination, ok := m.cachedMigrationRoots(rec.ID); ok {
		status.SourceRoot = source
		status.DestinationRoot = destination
	} else {
		status.SourceRoot = rootInfoFromSnapshot(rec.SourceRoot)
		status.DestinationRoot = rootInfoFromSnapshot(rec.DestinationRoot)
	}
	if status.SourceRoot != nil {
		status.SourceID = status.SourceRoot.ServiceID
	}
	if status.DestinationRoot != nil {
		status.DestinationID = status.DestinationRoot.ServiceID
	}
	return status
}

// persistLastKnownPhase records the latest observed lifecycle phase on the
// registry record so migration lists can show it without opening the
// migration's ops DB. Writes only when the phase actually changed.
func (m *Manager) persistLastKnownPhase(migrationID, phase string) {
	m.persistMigrationListHints(migrationID, phase, nil, nil)
}

// persistMigrationListHints stores phase and/or root snapshots on the registry
// so cold lists keep sidebar glyphs after restart. Nil roots leave existing
// snapshots unchanged; non-nil replaces that side.
func (m *Manager) persistMigrationListHints(
	migrationID, phase string,
	source, destination *corebridge.RootInfo,
) {
	if m.apiDB == nil {
		return
	}
	rec, err := m.apiDB.GetMigration(migrationID)
	if err != nil {
		return
	}
	changed := false
	if phase != "" && rec.Phase != phase {
		rec.Phase = phase
		changed = true
	}
	if source != nil {
		next := rootSnapshot(source)
		if !rootSnapshotsEqual(rec.SourceRoot, next) {
			rec.SourceRoot = next
			changed = true
		}
	}
	if destination != nil {
		next := rootSnapshot(destination)
		if !rootSnapshotsEqual(rec.DestinationRoot, next) {
			rec.DestinationRoot = next
			changed = true
		}
	}
	if !changed {
		return
	}
	_ = m.apiDB.UpsertMigration(rec)
}

func rootSnapshot(info *corebridge.RootInfo) *apidb.MigrationRootSnapshot {
	if info == nil {
		return nil
	}
	return &apidb.MigrationRootSnapshot{
		ServiceID:    info.ServiceID,
		ServiceName:  info.ServiceName,
		ServiceType:  info.ServiceType,
		ConnectionID: info.ConnectionID,
		Name:         info.Name,
		LocationPath: info.LocationPath,
		NativePath:   info.NativePath,
		Type:         info.Type,
		ParentID:     info.ParentID,
	}
}

func rootInfoFromSnapshot(s *apidb.MigrationRootSnapshot) *corebridge.RootInfo {
	if s == nil {
		return nil
	}
	return &corebridge.RootInfo{
		ServiceID:    s.ServiceID,
		ServiceName:  s.ServiceName,
		ServiceType:  s.ServiceType,
		ConnectionID: s.ConnectionID,
		Name:         s.Name,
		LocationPath: s.LocationPath,
		NativePath:   s.NativePath,
		Type:         s.Type,
		ParentID:     s.ParentID,
	}
}

func rootSnapshotsEqual(a, b *apidb.MigrationRootSnapshot) bool {
	if a == nil || b == nil {
		return a == b
	}
	return *a == *b
}

// preferMigrationDisplayName picks a user-facing name for list cards.
// Prefer a non-empty status/engine name, then registry name, else the migration ID.
func preferMigrationDisplayName(statusName, registryName, migrationID string) string {
	statusName = strings.TrimSpace(statusName)
	registryName = strings.TrimSpace(registryName)
	if statusName != "" && statusName != migrationID {
		return statusName
	}
	if registryName != "" && registryName != migrationID {
		return registryName
	}
	if statusName != "" {
		return statusName
	}
	if registryName != "" {
		return registryName
	}
	return migrationID
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
