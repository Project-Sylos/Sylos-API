package manager

import (
	"codeberg.org/Sylos/Migration-Engine/pkg/migration"
	"codeberg.org/Sylos/Migration-Engine/pkg/queue"
)

// SyncPathCheckProviders copies source/destination provider IDs and path-check profile from the
// root plan onto the migration so destination-name checks can be skipped or targeted correctly.
func (m *Manager) SyncPathCheckProviders(migrationID string, mig *migration.Migration) {
	if m == nil || mig == nil || migrationID == "" {
		return
	}
	plan := m.rootsMgr.GetPlan(migrationID)
	if plan == nil {
		return
	}
	src := migrationService(plan.SourceDefinition, plan.SourceAdapter, plan.SourceRoot, plan.SourceConnectionID)
	dst := migrationService(plan.DestinationDefinition, plan.DestinationAdapter, plan.DestinationRoot, plan.DestinationConnectionID)
	profile := plan.PathCheckTarget
	if profile == "" {
		// Default: off when same service type, auto (dst rules) when different.
		if queue.ResolvePathCheckTarget(src.ProviderID, dst.ProviderID, "auto") == "" {
			profile = "none"
		} else {
			profile = "auto"
		}
	}
	mig.SetPathCheckProviders(src.ProviderID, dst.ProviderID, profile)
}

// SetPathCheckTarget stores the user-selected path-check profile on the root plan.
func (m *Manager) SetPathCheckTarget(migrationID, target string) {
	if m == nil {
		return
	}
	m.rootsMgr.SetPathCheckTarget(migrationID, target)
}

// PathCheckTarget returns the stored path-check profile for a migration (may be empty).
func (m *Manager) PathCheckTarget(migrationID string) string {
	if m == nil || migrationID == "" {
		return ""
	}
	plan := m.rootsMgr.GetPlan(migrationID)
	if plan == nil {
		return ""
	}
	return plan.PathCheckTarget
}
