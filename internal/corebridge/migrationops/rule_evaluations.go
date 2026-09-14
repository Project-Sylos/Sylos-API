package migrationops

import (
	"fmt"
	"strings"

	enginedb "codeberg.org/Sylos/Migration-Engine/pkg/db"
	"codeberg.org/Sylos/Migration-Engine/pkg/migration"
	"codeberg.org/Sylos/Migration-Engine/pkg/opsdb"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge"
)

type ruleExclusionRow struct {
	NodeID          string
	RuleID          string
	Label           string
	EvalPhase       string
	ExclusionSource string
	ApplicationID   string
}

// EnrichPathNodesWithRuleExclusions attaches filter exclusion provenance from the ops store.
func EnrichPathNodesWithRuleExclusions(mig *migration.Migration, resp *corebridge.ListChildrenDiffsResponse) error {
	if mig == nil || mig.DB == nil || resp == nil || len(resp.Items) == 0 {
		return nil
	}
	nodeIDs := collectSrcNodeIDs(resp.Items)
	if len(nodeIDs) == 0 {
		return nil
	}
	rows, err := queryFilterExclusionProvenance(mig.DB, nodeIDs)
	if err != nil {
		return err
	}
	if len(rows) == 0 {
		return nil
	}
	byNode := make(map[string]ruleExclusionRow, len(rows))
	for _, row := range rows {
		byNode[row.NodeID] = row
	}
	for path := range resp.Items {
		nodes := resp.Items[path]
		if nodes.Src == nil || nodes.Src.Id == "" {
			continue
		}
		row, ok := byNode[nodes.Src.Id]
		if !ok {
			continue
		}
		nodes.Src.RuleExclusion = &corebridge.RuleExclusion{
			RuleID:          row.RuleID,
			Label:           row.Label,
			EvalPhase:       row.EvalPhase,
			ExclusionSource: row.ExclusionSource,
			ApplicationID:   row.ApplicationID,
		}
		resp.Items[path] = nodes
	}
	return nil
}

func collectSrcNodeIDs(items map[string]corebridge.PathNodes) []string {
	seen := make(map[string]struct{})
	var out []string
	for _, nodes := range items {
		if nodes.Src == nil {
			continue
		}
		id := strings.TrimSpace(nodes.Src.Id)
		if id == "" {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, id)
	}
	return out
}

func queryFilterExclusionProvenance(db *enginedb.DB, nodeIDs []string) ([]ruleExclusionRow, error) {
	if db == nil || db.Ops() == nil {
		return nil, fmt.Errorf("migration ops store required")
	}
	stMap, err := db.Ops().BatchGetStatus(opsdb.SideSRC, nodeIDs)
	if err != nil {
		return nil, fmt.Errorf("batch status for exclusion provenance: %w", err)
	}
	var out []ruleExclusionRow
	for _, id := range nodeIDs {
		st := stMap[id]
		cs := strings.TrimSpace(st.CopyStatus)
		if cs != enginedb.CopyStatusExcludedExplicit && cs != enginedb.CopyStatusExcludedInherited {
			continue
		}
		src := strings.TrimSpace(st.ExclusionSource)
		if src == "" || src == enginedb.ExclusionSourceManual {
			continue
		}
		row := ruleExclusionRow{
			NodeID:          id,
			RuleID:          strings.TrimSpace(st.DeterminingRuleID),
			ExclusionSource: src,
			ApplicationID:   src,
			EvalPhase:       "apply",
		}
		if rec, err := db.Ops().GetFilterApplication(src); err == nil && strings.TrimSpace(rec.CriteriaJSON) != "" {
			row.Label = "Filter application"
			if row.RuleID != "" {
				row.Label = "Filter: " + row.RuleID
			}
		} else if row.RuleID != "" {
			row.Label = "Filter: " + row.RuleID
		} else {
			row.Label = "Filter exclusion"
		}
		out = append(out, row)
	}
	return out, nil
}
