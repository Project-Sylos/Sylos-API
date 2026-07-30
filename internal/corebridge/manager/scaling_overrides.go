package manager

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"codeberg.org/Sylos/Migration-Engine/pkg/scaling/profile"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge/apidb"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge/roots"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge/services"
)

const (
	persistTargetProvider = "provider"
	persistTargetSFTPHost = "sftp_host"
)

// ScalingOverrideEntry is a grouped override for one scope key.
type ScalingOverrideEntry struct {
	Scope     string         `json:"scope"`
	ScopeKey  string         `json:"scopeKey"`
	Modes     map[string]int `json:"modes"`
	UpdatedAt time.Time      `json:"updatedAt"`
}

// MigrationScalingView is the live MaxWorkers overlays for a migration.
type MigrationScalingView struct {
	MigrationID string         `json:"migrationId"`
	Modes       map[string]int `json:"modes"`
}

// SetMigrationScalingRequest applies live overrides and optionally persists them.
type SetMigrationScalingRequest struct {
	Modes         map[string]int `json:"modes"`
	Persist       bool           `json:"persist"`
	PersistTarget string         `json:"persistTarget,omitempty"`
	PersistKey    string         `json:"persistKey,omitempty"`
}

// SaveScalingOverridesRequest is the body for PUT /scaling/overrides/...
type SaveScalingOverridesRequest struct {
	Modes map[string]int `json:"modes"`
}

func validateWorkerCapModes(modes map[string]int) (map[string]int, error) {
	if len(modes) == 0 {
		return map[string]int{}, nil
	}
	out := make(map[string]int, len(modes))
	for mode, maxWorkers := range modes {
		mode = strings.TrimSpace(mode)
		if !profile.ValidWorkerCapMode(mode) {
			return nil, fmt.Errorf("invalid worker cap mode %q", mode)
		}
		if maxWorkers < 1 || maxWorkers > profile.AbsoluteMaxWorkers {
			return nil, fmt.Errorf("max workers for %s must be between 1 and %d", mode, profile.AbsoluteMaxWorkers)
		}
		out[mode] = maxWorkers
	}
	return out, nil
}

// ListScalingDefaults returns shipped DefaultWorkerCaps for known providers plus sftp.
func (m *Manager) ListScalingDefaults() map[string]profile.ProviderWorkerCaps {
	ids := profile.KnownProviderIDs()
	seen := make(map[string]bool, len(ids)+4)
	out := make(map[string]profile.ProviderWorkerCaps, len(ids)+4)
	for _, id := range ids {
		seen[id] = true
		out[id] = profile.DefaultWorkerCaps(id)
	}
	for _, id := range []string{"local", "spectra", "generic", "sftp"} {
		if seen[id] {
			continue
		}
		seen[id] = true
		out[id] = profile.DefaultWorkerCaps(id)
	}
	return out
}

// ListScalingOverrides returns persisted overrides grouped by scope key.
func (m *Manager) ListScalingOverrides() ([]ScalingOverrideEntry, error) {
	if m.apiDB == nil {
		return nil, fmt.Errorf("api database unavailable")
	}
	rows, err := m.apiDB.ListAll()
	if err != nil {
		return nil, err
	}
	return groupScalingOverrideRows(rows), nil
}

// SaveScalingOverrides upserts MaxWorkers modes for a scope key; empty modes clears.
// scope is apidb.ScopeProvider or apidb.ScopeSFTPHost.
func (m *Manager) SaveScalingOverrides(scope, key string, modes map[string]int) error {
	if m.apiDB == nil {
		return fmt.Errorf("api database unavailable")
	}
	scope = strings.TrimSpace(scope)
	key = strings.TrimSpace(key)
	if scope != apidb.ScopeProvider && scope != apidb.ScopeSFTPHost {
		return fmt.Errorf("scope must be %q or %q", apidb.ScopeProvider, apidb.ScopeSFTPHost)
	}
	if key == "" {
		return fmt.Errorf("scope key is required")
	}
	cleaned, err := validateWorkerCapModes(modes)
	if err != nil {
		return err
	}
	return m.apiDB.UpsertModes(scope, key, cleaned)
}

// DeleteScalingOverrides clears all modes for a scope key.
func (m *Manager) DeleteScalingOverrides(scope, key string) error {
	if m.apiDB == nil {
		return fmt.Errorf("api database unavailable")
	}
	scope = strings.TrimSpace(scope)
	key = strings.TrimSpace(key)
	if scope != apidb.ScopeProvider && scope != apidb.ScopeSFTPHost {
		return fmt.Errorf("scope must be %q or %q", apidb.ScopeProvider, apidb.ScopeSFTPHost)
	}
	if key == "" {
		return fmt.Errorf("scope key is required")
	}
	return m.apiDB.DeleteScope(scope, key)
}

// ResolveWorkerCapOverrides merges src/dst provider or sftp_host overrides per mode.
// For each side, sftp_host overrides win when a host id is provided; otherwise provider.
// When any side sets a mode, the effective value is the min of the set values.
func (m *Manager) ResolveWorkerCapOverrides(srcProvider, dstProvider, srcSftpHostID, dstSftpHostID string) profile.WorkerCapOverrides {
	if m == nil || m.apiDB == nil {
		return profile.WorkerCapOverrides{}
	}
	rows, err := m.apiDB.ListAll()
	if err != nil {
		return profile.WorkerCapOverrides{}
	}
	src := sideOverrideCaps(rows, srcProvider, srcSftpHostID)
	dst := sideOverrideCaps(rows, dstProvider, dstSftpHostID)
	out := profile.WorkerCapOverrides{Caps: map[profile.WorkerCapMode]int{}}
	for _, mode := range profile.AllWorkerCapModes {
		var vals []int
		if v := src.MaxFor(mode); v > 0 {
			vals = append(vals, v)
		}
		if v := dst.MaxFor(mode); v > 0 {
			vals = append(vals, v)
		}
		if len(vals) == 0 {
			continue
		}
		min := vals[0]
		for _, v := range vals[1:] {
			if v < min {
				min = v
			}
		}
		out.Caps[mode] = min
	}
	if len(out.Caps) == 0 {
		return profile.WorkerCapOverrides{}
	}
	return out
}

func sideOverrideCaps(rows []apidb.ScalingOverrideRow, providerID, sftpHostID string) profile.WorkerCapOverrides {
	sftpHostID = strings.TrimSpace(sftpHostID)
	providerID = strings.TrimSpace(providerID)
	caps := make(map[profile.WorkerCapMode]int)
	if sftpHostID != "" {
		fillCapsFromRows(caps, rows, apidb.ScopeSFTPHost, sftpHostID)
	} else if providerID != "" {
		fillCapsFromRows(caps, rows, apidb.ScopeProvider, providerID)
	}
	if len(caps) == 0 {
		return profile.WorkerCapOverrides{}
	}
	return profile.WorkerCapOverrides{Caps: caps}
}

func fillCapsFromRows(caps map[profile.WorkerCapMode]int, rows []apidb.ScalingOverrideRow, scope, key string) {
	for _, row := range rows {
		if row.Scope != scope || row.ScopeKey != key {
			continue
		}
		if !profile.ValidWorkerCapMode(row.Mode) || row.MaxWorkers <= 0 {
			continue
		}
		caps[profile.WorkerCapMode(row.Mode)] = row.MaxWorkers
	}
}

// GetMigrationScaling returns the session MaxWorkers overlays for a migration.
func (m *Manager) GetMigrationScaling(ctx context.Context, migrationID string) (MigrationScalingView, error) {
	mig, err := m.GetMigration(ctx, migrationID)
	if err != nil {
		return MigrationScalingView{}, err
	}
	return MigrationScalingView{
		MigrationID: migrationID,
		Modes:       workerCapModesMap(mig.WorkerCapOverrides()),
	}, nil
}

// SetMigrationScaling applies live overrides and optionally persists to api_db.
func (m *Manager) SetMigrationScaling(ctx context.Context, migrationID string, req SetMigrationScalingRequest) (MigrationScalingView, error) {
	mig, err := m.GetMigration(ctx, migrationID)
	if err != nil {
		return MigrationScalingView{}, err
	}
	cleaned, err := validateWorkerCapModes(req.Modes)
	if err != nil {
		return MigrationScalingView{}, err
	}
	overrides := modesToWorkerCapOverrides(cleaned)
	mig.SetWorkerCapOverrides(overrides)

	if req.Persist {
		if err := m.persistMigrationScaling(req.PersistTarget, req.PersistKey, cleaned); err != nil {
			return MigrationScalingView{}, err
		}
	}
	return MigrationScalingView{
		MigrationID: migrationID,
		Modes:       cleaned,
	}, nil
}

func (m *Manager) persistMigrationScaling(target, key string, modes map[string]int) error {
	target = strings.TrimSpace(strings.ToLower(target))
	key = strings.TrimSpace(key)
	if key == "" {
		return fmt.Errorf("persistKey is required when persist is true")
	}
	switch target {
	case persistTargetProvider:
		return m.SaveScalingOverrides(apidb.ScopeProvider, key, modes)
	case persistTargetSFTPHost:
		return m.SaveScalingOverrides(apidb.ScopeSFTPHost, key, modes)
	default:
		return fmt.Errorf("persistTarget must be %q or %q", persistTargetProvider, persistTargetSFTPHost)
	}
}

// resolveOverridesForPlan loads persisted MaxWorkers overlays for the plan's providers.
// SFTP saved-host ids are not currently stored on RootPlan / connection records, so
// start-time resolve uses provider-level overrides only (empty host ids).
func (m *Manager) resolveOverridesForPlan(plan *roots.RootPlan) profile.WorkerCapOverrides {
	if plan == nil {
		return profile.WorkerCapOverrides{}
	}
	srcProvider := providerIDFromDefinition(plan.SourceDefinition)
	dstProvider := providerIDFromDefinition(plan.DestinationDefinition)
	return m.ResolveWorkerCapOverrides(srcProvider, dstProvider, "", "")
}

func providerIDFromDefinition(def services.ServiceDefinition) string {
	switch def.Type {
	case services.ServiceTypeCloud:
		// Includes SFTP (cloud provider_id "sftp").
		return services.CloudProviderID(def)
	case services.ServiceTypeLocal:
		return string(services.ServiceTypeLocal)
	case services.ServiceTypeSpectra:
		return string(services.ServiceTypeSpectra)
	default:
		return services.CloudProviderID(def)
	}
}

func groupScalingOverrideRows(rows []apidb.ScalingOverrideRow) []ScalingOverrideEntry {
	type key struct{ scope, scopeKey string }
	grouped := make(map[key]*ScalingOverrideEntry)
	for _, row := range rows {
		k := key{scope: row.Scope, scopeKey: row.ScopeKey}
		entry, ok := grouped[k]
		if !ok {
			entry = &ScalingOverrideEntry{
				Scope:     row.Scope,
				ScopeKey:  row.ScopeKey,
				Modes:     make(map[string]int),
				UpdatedAt: row.UpdatedAt,
			}
			grouped[k] = entry
		}
		entry.Modes[row.Mode] = row.MaxWorkers
		if row.UpdatedAt.After(entry.UpdatedAt) {
			entry.UpdatedAt = row.UpdatedAt
		}
	}
	out := make([]ScalingOverrideEntry, 0, len(grouped))
	for _, entry := range grouped {
		out = append(out, *entry)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Scope != out[j].Scope {
			return out[i].Scope < out[j].Scope
		}
		return out[i].ScopeKey < out[j].ScopeKey
	})
	return out
}

func workerCapModesMap(o profile.WorkerCapOverrides) map[string]int {
	if len(o.Caps) == 0 {
		return map[string]int{}
	}
	out := make(map[string]int, len(o.Caps))
	for mode, maxWorkers := range o.Caps {
		out[string(mode)] = maxWorkers
	}
	return out
}

func modesToWorkerCapOverrides(modes map[string]int) profile.WorkerCapOverrides {
	if len(modes) == 0 {
		return profile.WorkerCapOverrides{}
	}
	caps := make(map[profile.WorkerCapMode]int, len(modes))
	for mode, maxWorkers := range modes {
		caps[profile.WorkerCapMode(mode)] = maxWorkers
	}
	return profile.WorkerCapOverrides{Caps: caps}
}
