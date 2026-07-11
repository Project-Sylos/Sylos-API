package corebridge

import (
	"codeberg.org/Sylos/Migration-Engine/pkg/migration"
)

type pathReviewOutcome struct {
	success       bool
	errMsg        string
	affectedCount int64
	deltas        map[string]int64
}

func emptyDeltas() map[string]int64 {
	return map[string]int64{}
}

func nilMigrationOutcome() pathReviewOutcome {
	return pathReviewOutcome{errMsg: "migration is nil", deltas: emptyDeltas()}
}

func runPathReviewBatch(
	nodeIDs []string,
	perNode func(nodeID string) (migration.PathReviewActionResult, error),
) (merged migration.PathReviewActionResult, err error) {
	for _, nodeID := range nodeIDs {
		res, err := perNode(nodeID)
		if err != nil {
			return merged, err
		}
		merged = mergePathReviewResults(merged, res)
	}
	return merged, nil
}

func outcomeFromBatch(merged migration.PathReviewActionResult, err error, partialOnErr bool) pathReviewOutcome {
	if err != nil {
		out := pathReviewOutcome{deltas: emptyDeltas(), errMsg: err.Error()}
		if partialOnErr {
			out.affectedCount, out.deltas = pathReviewResultToResponse(merged)
		}
		return out
	}
	aff, deltas := pathReviewResultToResponse(merged)
	return pathReviewOutcome{success: true, affectedCount: aff, deltas: deltas}
}

func outcomeFromSingle(res migration.PathReviewActionResult, err error) pathReviewOutcome {
	if err != nil {
		return pathReviewOutcome{deltas: emptyDeltas(), errMsg: err.Error()}
	}
	aff, deltas := pathReviewResultToResponse(res)
	return pathReviewOutcome{success: true, affectedCount: aff, deltas: deltas}
}

func exclusionFromOutcome(o pathReviewOutcome) *ExclusionResponse {
	return &ExclusionResponse{
		Success:       o.success,
		Error:         o.errMsg,
		AffectedCount: o.affectedCount,
		Deltas:        o.deltas,
	}
}

func markRetryFromOutcome(o pathReviewOutcome) *MarkRetryResponse {
	return &MarkRetryResponse{
		Success:       o.success,
		Error:         o.errMsg,
		AffectedCount: o.affectedCount,
		Deltas:        o.deltas,
	}
}
