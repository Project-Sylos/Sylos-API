package manager

import (
	"context"

	"codeberg.org/Sylos/Migration-Engine/pkg/migration"
	"codeberg.org/Sylos/Migration-Engine/pkg/queue/gpl"
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
		if gpl.ResolvePathCheckTarget(src.ProviderID, dst.ProviderID, "auto") == "" {
			profile = "none"
		} else {
			profile = "auto"
		}
	}
	mig.SetPathCheckProviders(src.ProviderID, dst.ProviderID, profile)
	mig.SetWindowsCompat(plan.WindowsCompat)
}

// SetPathCheckTarget stores the user-selected path-check profile on the root plan.
func (m *Manager) SetPathCheckTarget(migrationID, target string) {
	if m == nil {
		return
	}
	m.rootsMgr.SetPathCheckTarget(migrationID, target)
}

// SetWindowsCompat stores the Windows-compat opt-in on the root plan and migration.
func (m *Manager) SetWindowsCompat(migrationID string, enabled bool) {
	if m == nil {
		return
	}
	m.rootsMgr.SetWindowsCompat(migrationID, enabled)
	if mig, err := m.GetMigration(context.Background(), migrationID); err == nil && mig != nil {
		mig.SetWindowsCompat(enabled)
	}
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

// WindowsCompat returns the stored Windows-compat flag for a migration.
func (m *Manager) WindowsCompat(migrationID string) bool {
	if m == nil || migrationID == "" {
		return false
	}
	plan := m.rootsMgr.GetPlan(migrationID)
	if plan == nil {
		return false
	}
	return plan.WindowsCompat
}

// PathCheckTargetView is the path-check profile state for a migration.
type PathCheckTargetView struct {
	PathCheckTarget   string
	PathChecksEnabled bool
	WindowsCompat     bool
}

func (m *Manager) PathCheckTargetView(ctx context.Context, migrationID string) (PathCheckTargetView, error) {
	mig, err := m.PathMigration(ctx, migrationID)
	if err != nil {
		return PathCheckTargetView{}, err
	}
	profile := m.PathCheckTarget(migrationID)
	if profile == "" {
		profile = mig.PathCheckProfile()
	}
	return PathCheckTargetView{
		PathCheckTarget:   profile,
		PathChecksEnabled: mig.PathChecksEnabled(),
		WindowsCompat:     m.WindowsCompat(migrationID) || mig.WindowsCompat(),
	}, nil
}

func (m *Manager) UpdatePathCheckTarget(ctx context.Context, migrationID, target string, windowsCompat *bool) (PathCheckTargetView, error) {
	m.SetPathCheckTarget(migrationID, target)
	if windowsCompat != nil {
		m.SetWindowsCompat(migrationID, *windowsCompat)
	}
	mig, err := m.PathMigration(ctx, migrationID)
	if err != nil {
		return PathCheckTargetView{}, err
	}
	return PathCheckTargetView{
		PathCheckTarget:   mig.PathCheckProfile(),
		PathChecksEnabled: mig.PathChecksEnabled(),
		WindowsCompat:     m.WindowsCompat(migrationID) || mig.WindowsCompat(),
	}, nil
}
