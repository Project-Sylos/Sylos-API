package migrationops

import (
	"errors"
	"sort"

	"codeberg.org/Sylos/Migration-Engine/pkg/migration"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge"
)

type pathReviewOutcome struct {
	success          bool
	errMsg           string
	affectedCount    int64
	unchangedCount   int64
	deltas           map[string]int64
	unchangedReasons []corebridge.BulkReasonGroup
}

func emptyDeltas() map[string]int64 {
	return map[string]int64{}
}

func nilMigrationOutcome() pathReviewOutcome {
	return pathReviewOutcome{errMsg: "migration is nil", deltas: emptyDeltas()}
}

type nodeBatchItem struct {
	nodeID  string
	changed bool
	reason  string
}

func runPathReviewBatch(
	nodeIDs []string,
	perNode func(nodeID string) (migration.PathReviewActionResult, error),
	noopReason string,
) (merged migration.PathReviewActionResult, items []nodeBatchItem, err error) {
	if noopReason == "" {
		noopReason = "no change"
	}
	items = make([]nodeBatchItem, 0, len(nodeIDs))
	for _, nodeID := range nodeIDs {
		res, nodeErr := perNode(nodeID)
		if nodeErr != nil {
			if errors.Is(nodeErr, migration.ErrReviewOpBusy) {
				return merged, items, nodeErr
			}
			items = append(items, nodeBatchItem{
				nodeID: nodeID,
				reason: nodeErr.Error(),
			})
			continue
		}
		if res.AffectedCount == 0 {
			items = append(items, nodeBatchItem{
				nodeID: nodeID,
				reason: noopReason,
			})
			continue
		}
		items = append(items, nodeBatchItem{nodeID: nodeID, changed: true})
		merged = mergePathReviewResults(merged, res)
	}
	return merged, items, nil
}

func groupUnchangedReasons(items []nodeBatchItem) []corebridge.BulkReasonGroup {
	counts := map[string]int{}
	for _, item := range items {
		if item.changed || item.reason == "" {
			continue
		}
		counts[item.reason]++
	}
	if len(counts) == 0 {
		return nil
	}
	out := make([]corebridge.BulkReasonGroup, 0, len(counts))
	for reason, count := range counts {
		out = append(out, corebridge.BulkReasonGroup{Reason: reason, Count: count})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Count == out[j].Count {
			return out[i].Reason < out[j].Reason
		}
		return out[i].Count > out[j].Count
	})
	return out
}

func outcomeFromItems(merged migration.PathReviewActionResult, items []nodeBatchItem) pathReviewOutcome {
	changed := int64(0)
	unchanged := int64(0)
	for _, item := range items {
		if item.changed {
			changed++
		} else {
			unchanged++
		}
	}
	aff, deltas := pathReviewResultToResponse(merged)
	if aff == 0 {
		aff = changed
	}
	out := pathReviewOutcome{
		success:          changed > 0 || (changed == 0 && unchanged == 0),
		affectedCount:    aff,
		unchangedCount:   unchanged,
		deltas:           deltas,
		unchangedReasons: groupUnchangedReasons(items),
	}
	if changed == 0 && unchanged > 0 {
		out.success = false
		if len(out.unchangedReasons) > 0 {
			out.errMsg = out.unchangedReasons[0].Reason
		} else {
			out.errMsg = "no items changed"
		}
	}
	return out
}

func outcomeFromSingle(res migration.PathReviewActionResult, err error) pathReviewOutcome {
	if err != nil {
		return pathReviewOutcome{deltas: emptyDeltas(), errMsg: err.Error()}
	}
	aff, deltas := pathReviewResultToResponse(res)
	success := aff > 0
	out := pathReviewOutcome{success: success, affectedCount: aff, deltas: deltas}
	if !success {
		out.errMsg = "no items changed"
	}
	return out
}

func exclusionFromOutcome(o pathReviewOutcome) *corebridge.ExclusionResponse {
	return &corebridge.ExclusionResponse{
		Success:          o.success,
		Error:            o.errMsg,
		AffectedCount:    o.affectedCount,
		UnchangedCount:   o.unchangedCount,
		UnchangedReasons: o.unchangedReasons,
		Deltas:           o.deltas,
	}
}

func markRetryFromOutcome(o pathReviewOutcome) *corebridge.MarkRetryResponse {
	return &corebridge.MarkRetryResponse{
		Success:          o.success,
		Error:            o.errMsg,
		AffectedCount:    o.affectedCount,
		UnchangedCount:   o.unchangedCount,
		UnchangedReasons: o.unchangedReasons,
		Deltas:           o.deltas,
	}
}
