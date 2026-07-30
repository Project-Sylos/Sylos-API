package roots

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"codeberg.org/Sylos/Migration-Engine/pkg/migration"
)

// RootChildPlan is one immediate child under the confirmed migration root.
type RootChildPlan struct {
	ID       string `json:"id"`
	Name     string `json:"name"`
	Type     string `json:"type"` // folder | file
	Size     int64  `json:"size,omitempty"`
	MTime    string `json:"mtime,omitempty"`
	Excluded bool   `json:"excluded,omitempty"` // source only
	DstOnly  bool   `json:"dstOnly,omitempty"`  // destination only
}

// PersistedPreparation is written under the migration data dir so restart keeps the review.
type PersistedPreparation struct {
	SourcePrepared bool            `json:"sourcePrepared,omitempty"`
	DestPrepared   bool            `json:"destPrepared,omitempty"`
	SourceChildren []RootChildPlan `json:"sourceChildren,omitempty"`
	DestChildren   []RootChildPlan `json:"destChildren,omitempty"`
}

func preparationPath(dataDir, migrationID string) string {
	return filepath.Join(dataDir, migrationID, "root_preparation.json")
}

// NormalizeRootChildren keeps only entries with a name (immediate child review payload).
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

	out := make([]RootChildPlan, 0, len(children))
	included := 0
	for _, c := range children {
		name := strings.TrimSpace(c.Name)
		if name == "" {
			continue
		}
		id := strings.TrimSpace(c.ID)
		child := RootChildPlan{
			ID:    id,
			Name:  name,
			Type:  strings.TrimSpace(c.Type),
			Size:  c.Size,
			MTime: strings.TrimSpace(c.MTime),
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
		out = append(out, child)
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
			ServiceID: c.ID,
			Name:      c.Name,
			Type:      c.Type,
			Size:      c.Size,
			MTime:     c.MTime,
			Excluded:  c.Excluded,
			DstOnly:   c.DstOnly,
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
	return os.WriteFile(preparationPath(m.dataDir, migrationID), raw, 0o644)
}

func (m *Manager) loadPreparationInto(migrationID string, plan *RootPlan) {
	if m.dataDir == "" || migrationID == "" || plan == nil {
		return
	}
	raw, err := os.ReadFile(preparationPath(m.dataDir, migrationID))
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

// PreparationSummary returns UI-facing prepared flags (loads disk if needed).
func (m *Manager) PreparationSummary(migrationID string) (sourcePrepared, destPrepared bool) {
	prep := m.PreparationFor(migrationID)
	return prep.SourcePrepared, prep.DestPrepared
}

// SourceChildrenNames returns lowercase display names of reviewed source root children.
func (m *Manager) SourceChildrenNames(migrationID string) []string {
	m.mu.RLock()
	plan := m.plans[migrationID]
	var kids []RootChildPlan
	if plan != nil {
		kids = plan.SourceChildren
	}
	m.mu.RUnlock()
	if len(kids) == 0 {
		prep := m.PreparationFor(migrationID)
		out := make([]string, 0, len(prep.SourceChildren))
		for _, c := range prep.SourceChildren {
			if strings.TrimSpace(c.Name) != "" {
				out = append(out, c.Name)
			}
		}
		return out
	}
	out := make([]string, 0, len(kids))
	for _, c := range kids {
		if strings.TrimSpace(c.Name) != "" {
			out = append(out, c.Name)
		}
	}
	return out
}
