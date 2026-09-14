package roots

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"codeberg.org/Sylos/Migration-Engine/pkg/migration"
)

// RootChildPlan is one child under a prepared parent (depth-1 or nested sparse forest).
type RootChildPlan struct {
	ID          string          `json:"id"`
	Name        string          `json:"name"`
	Type        string          `json:"type"` // folder | file
	Size        int64           `json:"size,omitempty"`
	MTime       string          `json:"mtime,omitempty"`
	Excluded    bool            `json:"excluded,omitempty"` // source only
	DstOnly     bool            `json:"dstOnly,omitempty"`  // destination only
	Children    []RootChildPlan `json:"children,omitempty"`
	IncludeOnly []string        `json:"includeOnly,omitempty"`
}

// PersistedPreparation is written under the migration data dir so restart keeps the review.
type PersistedPreparation struct {
	SourcePrepared bool            `json:"sourcePrepared,omitempty"`
	DestPrepared   bool            `json:"destPrepared,omitempty"`
	SourceChildren []RootChildPlan `json:"sourceChildren,omitempty"`
	DestChildren   []RootChildPlan `json:"destChildren,omitempty"`
}

// NormalizeRootChildren keeps named entries; recurses into Children for sparse source trees.
// ExcludedIds that are not among children are ignored (uncle/aunt / off-path marks).
func NormalizeRootChildren(children []RootChildPlan, excludedIDs []string, allowExclude bool) ([]RootChildPlan, error) {
	excluded := make(map[string]struct{}, len(excludedIDs))
	for _, id := range excludedIDs {
		id = strings.TrimSpace(id)
		if id == "" {
			continue
		}
		excluded[id] = struct{}{}
	}

	var normalizeLevel func([]RootChildPlan) ([]RootChildPlan, int, error)
	normalizeLevel = func(level []RootChildPlan) ([]RootChildPlan, int, error) {
		out := make([]RootChildPlan, 0, len(level))
		included := 0
		for _, c := range level {
			if c.Name == "" {
				continue
			}
			id := strings.TrimSpace(c.ID)
			child := RootChildPlan{
				ID:          id,
				Name:        c.Name,
				Type:        strings.TrimSpace(c.Type),
				Size:        c.Size,
				MTime:       strings.TrimSpace(c.MTime),
				IncludeOnly: append([]string(nil), c.IncludeOnly...),
			}
			if child.Type == "" {
				child.Type = "folder"
			}
			if allowExclude {
				if c.Excluded {
					child.Excluded = true
				}
				if id != "" {
					if _, ok := excluded[id]; ok {
						child.Excluded = true
					}
				}
				if !child.Excluded {
					included++
				}
			} else {
				child.DstOnly = c.DstOnly
			}
			if len(c.Children) > 0 {
				nested, _, err := normalizeLevel(c.Children)
				if err != nil {
					return nil, 0, err
				}
				child.Children = nested
			}
			out = append(out, child)
		}
		return out, included, nil
	}

	out, included, err := normalizeLevel(children)
	if err != nil {
		return nil, err
	}
	if allowExclude && len(out) > 0 && included == 0 {
		return nil, fmt.Errorf("source root review requires at least one included child")
	}
	return out, nil
}

func (p PersistedPreparation) ToEngine() migration.RootPreparation {
	return migration.RootPreparation{
		SourcePrepared: p.SourcePrepared,
		DestPrepared:   p.DestPrepared,
		SourceChildren: toEngineChildren(p.SourceChildren),
		DestChildren:   toEngineChildren(p.DestChildren),
	}
}

func toEngineChildren(in []RootChildPlan) []migration.RootChildSeed {
	if len(in) == 0 {
		return nil
	}
	out := make([]migration.RootChildSeed, 0, len(in))
	for _, c := range in {
		out = append(out, migration.RootChildSeed{
			ServiceID:   c.ID,
			Name:        c.Name,
			Type:        c.Type,
			Size:        c.Size,
			MTime:       c.MTime,
			Excluded:    c.Excluded,
			DstOnly:     c.DstOnly,
			Children:    toEngineChildren(c.Children),
			IncludeOnly: append([]string(nil), c.IncludeOnly...),
		})
	}
	return out
}

func (m *Manager) savePreparationLocked(migrationID string, plan *RootPlan) error {
	if m.dataDir == "" || migrationID == "" || plan == nil {
		return nil
	}
	payload := PersistedPreparation{
		SourcePrepared: plan.SourceRootPrepared,
		DestPrepared:   plan.DestinationRootPrepared,
		SourceChildren: plan.SourceChildren,
		DestChildren:   plan.DestinationChildren,
	}
	raw, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		return err
	}
	dir := filepath.Join(m.dataDir, migrationID)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(m.dataDir, migrationID, "root_preparation.json"), raw, 0o644)
}

func (m *Manager) loadPreparationInto(migrationID string, plan *RootPlan) {
	if m.dataDir == "" || migrationID == "" || plan == nil {
		return
	}
	raw, err := os.ReadFile(filepath.Join(m.dataDir, migrationID, "root_preparation.json"))
	if err != nil {
		return
	}
	var payload PersistedPreparation
	if err := json.Unmarshal(raw, &payload); err != nil {
		return
	}
	plan.SourceRootPrepared = payload.SourcePrepared
	plan.DestinationRootPrepared = payload.DestPrepared
	plan.SourceChildren = payload.SourceChildren
	plan.DestinationChildren = payload.DestChildren
}

// PreparationFor returns the engine prep snapshot for Start/AddRoots (loads disk if needed).
func (m *Manager) PreparationFor(migrationID string) migration.RootPreparation {
	m.mu.Lock()
	defer m.mu.Unlock()
	plan := m.plans[migrationID]
	if plan == nil {
		plan = &RootPlan{}
		m.plans[migrationID] = plan
		m.loadPreparationInto(migrationID, plan)
	} else if !plan.SourceRootPrepared && !plan.DestinationRootPrepared &&
		len(plan.SourceChildren) == 0 && len(plan.DestinationChildren) == 0 {
		m.loadPreparationInto(migrationID, plan)
	}
	return PersistedPreparation{
		SourcePrepared: plan.SourceRootPrepared,
		DestPrepared:   plan.DestinationRootPrepared,
		SourceChildren: plan.SourceChildren,
		DestChildren:   plan.DestinationChildren,
	}.ToEngine()
}

// PersistedPreparationFor returns the full UI/API prep payload (for browse reconfirm).
func (m *Manager) PersistedPreparationFor(migrationID string) PersistedPreparation {
	m.mu.Lock()
	defer m.mu.Unlock()
	plan := m.plans[migrationID]
	if plan == nil {
		plan = &RootPlan{}
		m.plans[migrationID] = plan
		m.loadPreparationInto(migrationID, plan)
	} else if !plan.SourceRootPrepared && !plan.DestinationRootPrepared &&
		len(plan.SourceChildren) == 0 && len(plan.DestinationChildren) == 0 {
		m.loadPreparationInto(migrationID, plan)
	}
	return PersistedPreparation{
		SourcePrepared: plan.SourceRootPrepared,
		DestPrepared:   plan.DestinationRootPrepared,
		SourceChildren: plan.SourceChildren,
		DestChildren:   plan.DestinationChildren,
	}
}

// PreparationSummary returns UI-facing prepared flags (loads disk if needed).
func (m *Manager) PreparationSummary(migrationID string) (sourcePrepared, destPrepared bool) {
	prep := m.PersistedPreparationFor(migrationID)
	return prep.SourcePrepared, prep.DestPrepared
}

// SourceChildrenNames returns display names of included source root children
// (for destination name-match / greying). Excluded source children are omitted.
func (m *Manager) SourceChildrenNames(migrationID string) []string {
	prep := m.PersistedPreparationFor(migrationID)
	out := make([]string, 0, len(prep.SourceChildren))
	for _, c := range prep.SourceChildren {
		if c.Name == "" || c.Excluded {
			continue
		}
		out = append(out, strings.ToLower(c.Name))
	}
	return out
}
