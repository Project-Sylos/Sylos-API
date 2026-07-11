package corebridge

import (
	"fmt"
	"strings"
	"time"

	"codeberg.org/Sylos/Migration-Engine/pkg/convert"
	"codeberg.org/Sylos/Migration-Engine/pkg/migration"
)

// InspectMigrationStatus calls the engine's DB-backed inspection and returns the result.
func InspectMigrationStatus(mig *migration.Migration) (migration.MigrationStatus, error) {
	if mig == nil || mig.DB == nil {
		return migration.MigrationStatus{}, fmt.Errorf("migration or database is nil")
	}
	return migration.InspectMigrationStatus(mig.DB)
}

// PathReviewStatsFromMigration returns phase-aware review stats from the engine (cache-backed, no DB on API side).
func PathReviewStatsFromMigration(mig *migration.Migration) (*PathReviewStats, error) {
	if mig == nil {
		return nil, fmt.Errorf("migration is nil")
	}
	stats := mig.GetPathReviewStats()
	return &PathReviewStats{
		PendingCount:        stats.PendingCount,
		FailedCount:         stats.FailedCount,
		ExcludedCount:       stats.ExcludedCount,
		PendingRetriesCount: stats.PendingRetriesCount,
		SuccessfulCount:     stats.SuccessfulCount,
		FoldersCount:        stats.FoldersCount,
		FilesCount:          stats.FilesCount,
		FoldersRatio:        stats.FoldersRatio,
		FilesRatio:          stats.FilesRatio,
		TotalFileSize: FileSizeStats{
			Src: stats.TotalFileSize.Src,
			Dst: stats.TotalFileSize.Dst,
		},
	}, nil
}

// resolveSort extracts sort field and direction from an optional SortOption, defaulting to ascending.
func resolveSort(sort *SortOption) (sortBy, sortDirection string) {
	sortDirection = "asc"
	if sort != nil {
		sortBy = sort.Field
		if sort.Direction != "" {
			sortDirection = strings.ToLower(sort.Direction)
		}
	}
	return sortBy, sortDirection
}

// diffListResponse builds the paginated API response from engine diff items.
func diffListResponse(items []migration.DiffItem, offset, limit, total int) ListChildrenDiffsResponse {
	out := make(map[string]PathNodes, len(items))
	order := make([]string, 0, len(items))
	for _, item := range items {
		out[item.Path] = diffItemToPathNodes(item)
		order = append(order, item.Path)
	}
	return ListChildrenDiffsResponse{
		Items:     out,
		ItemOrder: order,
		Pagination: PaginationInfo{
			Offset:  offset,
			Limit:   limit,
			Total:   total,
			HasMore: offset+limit < total,
		},
	}
}

func diffItemToPathNodes(item migration.DiffItem) PathNodes {
	pathNodes := PathNodes{}
	if !item.MissingOnSource {
		pathNodes.Src = &PathNodeItem{
			Queue:           "SRC",
			Id:              item.SrcNodeID,
			Name:            item.Name,
			LocationPath:    item.Path,
			DepthLevel:      item.Depth,
			Type:            item.Type,
			Size:            item.Size,
			TraversalStatus: item.SrcTraversalStatus,
			CopyStatus:      item.CopyStatus,
			DeleteStatus:    item.DeleteStatus,
			FailureLogID:    item.SrcFailureLogID,
			FailureMessage:  item.SrcFailureMessage,
		}
	}
	if !item.MissingOnDest {
		pathNodes.Dst = &PathNodeItem{
			Queue:           "DST",
			Id:              item.DstNodeID,
			Name:            item.Name,
			LocationPath:    item.Path,
			DepthLevel:      item.Depth,
			Type:            item.Type,
			Size:            item.Size,
			TraversalStatus: item.DstTraversalStatus,
			FailureLogID:    item.DstFailureLogID,
			FailureMessage:  item.DstFailureMessage,
		}
	}
	return pathNodes
}

// ListChildrenDiffs calls the engine and converts the result to API response.
func ListChildrenDiffs(mig *migration.Migration, req ListChildrenDiffsRequest) (ListChildrenDiffsResponse, error) {
	if mig == nil {
		return ListChildrenDiffsResponse{}, fmt.Errorf("migration is nil")
	}
	sortBy, sortDirection := resolveSort(req.Sort)
	result, err := mig.ListChildrenDiffs(migration.ListChildrenDiffsRequest{
		Path:          req.Path,
		Limit:         req.Limit,
		Offset:        req.Offset,
		SortBy:        sortBy,
		SortDirection: sortDirection,
		FoldersOnly:   req.FoldersOnly,
	})
	if err != nil {
		return ListChildrenDiffsResponse{}, fmt.Errorf("failed to list children diffs: %w", err)
	}
	return diffListResponse(result.Items, result.Offset, result.Limit, result.Total), nil
}

// GetChildrenDiffsStats calls the engine and converts to API response.
func GetChildrenDiffsStats(mig *migration.Migration, path string, foldersOnly bool) (DiffsStatsResponse, error) {
	if mig == nil {
		return DiffsStatsResponse{}, fmt.Errorf("migration is nil")
	}
	stats, err := mig.GetChildrenDiffsStats(path, foldersOnly)
	if err != nil {
		return DiffsStatsResponse{}, fmt.Errorf("failed to get diffs stats: %w", err)
	}
	folders := stats.Folders
	files := stats.Files
	if foldersOnly {
		files = 0
	}
	return DiffsStatsResponse{
		Total:        stats.Total,
		TotalFolders: folders,
		TotalFiles:   files,
	}, nil
}

func enginePathReviewConditions(req SearchRequest) []migration.PathReviewSearchCondition {
	if len(req.Conditions) == 0 {
		return nil
	}
	out := make([]migration.PathReviewSearchCondition, 0, len(req.Conditions))
	for _, c := range req.Conditions {
		out = append(out, migration.PathReviewSearchCondition{
			Field:    c.Field,
			Operator: c.Operator,
			Value:    c.Value,
		})
	}
	return out
}

// engineSearchRequest builds the engine search request shared by search and search-stats calls.
func engineSearchRequest(req SearchRequest, offset, limit int) migration.SearchRequest {
	sortBy, sortDirection := resolveSort(req.Sort)
	return migration.SearchRequest{
		Path:             "",
		Limit:            limit,
		Offset:           offset,
		SortBy:           sortBy,
		SortDirection:    sortDirection,
		Conditions:       enginePathReviewConditions(req),
		StatusSearchType: req.StatusSearchType,
	}
}

// SearchPathReviewItems calls the engine and converts to API response.
func SearchPathReviewItems(mig *migration.Migration, req SearchRequest, offset, limit int) (ListChildrenDiffsResponse, error) {
	if mig == nil {
		return ListChildrenDiffsResponse{}, fmt.Errorf("migration is nil")
	}
	result, err := mig.SearchPathReviewItems(engineSearchRequest(req, offset, limit))
	if err != nil {
		return ListChildrenDiffsResponse{}, fmt.Errorf("failed to search path review items: %w", err)
	}
	return diffListResponse(result.Items, result.Offset, result.Limit, result.Total), nil
}

// GetSearchStats calls the engine and converts to API response.
func GetSearchStats(mig *migration.Migration, req SearchRequest) (DiffsStatsResponse, error) {
	if mig == nil {
		return DiffsStatsResponse{}, fmt.Errorf("migration is nil")
	}
	stats, err := mig.GetSearchStats(engineSearchRequest(req, 0, 10000))
	if err != nil {
		return DiffsStatsResponse{}, fmt.Errorf("failed to get search stats: %w", err)
	}
	return DiffsStatsResponse{
		Total:        stats.Total,
		TotalFolders: stats.Folders,
		TotalFiles:   stats.Files,
	}, nil
}

// commonQueueState fills the state fields shared by traversal and copy queues.
func commonQueueState(m *ExternalQueueMetrics, name string, q map[string]any) {
	m.Name = name
	m.Round = convert.ToNumber[int](q["round"])
	m.Pending = convert.ToNumber[int](q["pending"])
	m.InProgress = convert.ToNumber[int](q["in_progress"])
	m.Workers = convert.ToNumber[int](q["workers"])
	m.TotalPending = convert.ToNumber[int](q["total_pending"])
	m.TotalFailed = convert.ToNumber[int](q["total_failed"])
	m.PossibleStall = convert.ToBool(q["possible_stall"])
}

func traversalQueueMetrics(name string, q map[string]any) *ExternalQueueMetrics {
	m := &ExternalQueueMetrics{
		FilesDiscoveredTotal:     convert.ToNumber[int64](q["files_discovered_total"]),
		FoldersDiscoveredTotal:   convert.ToNumber[int64](q["folders_discovered_total"]),
		DiscoveryRateItemsPerSec: convert.ToNumber[float64](q["discovery_rate_items_per_sec"]),
		TotalDiscovered:          convert.ToNumber[int64](q["total_discovered"]),
	}
	commonQueueState(m, name, q)
	return m
}

func copyQueueMetrics(q map[string]any) *ExternalQueueMetrics {
	m := &ExternalQueueMetrics{
		Folders:        convert.ToNumber[int64](q["folders"]),
		Files:          convert.ToNumber[int64](q["files"]),
		Total:          convert.ToNumber[int64](q["total"]),
		Bytes:          convert.ToNumber[int64](q["bytes"]),
		ItemsPerSecond: convert.ToNumber[float64](q["items_per_second"]),
		BytesPerSecond: convert.ToNumber[float64](q["bytes_per_second"]),
	}
	commonQueueState(m, "copy", q)
	return m
}

func deleteQueueMetrics(q map[string]any) *ExternalQueueMetrics {
	m := &ExternalQueueMetrics{
		Folders:        convert.ToNumber[int64](q["folders"]),
		Files:          convert.ToNumber[int64](q["files"]),
		Total:          convert.ToNumber[int64](q["total"]),
		ItemsPerSecond: convert.ToNumber[float64](q["items_per_second"]),
	}
	commonQueueState(m, "delete", q)
	return m
}

// QueueMetricsFromMigration calls the engine and converts to API response.
func QueueMetricsFromMigration(mig *migration.Migration) (*QueueMetricsResponse, error) {
	if mig == nil {
		return nil, fmt.Errorf("migration is nil")
	}
	metrics, err := mig.GetQueueMetrics()
	if err != nil {
		return nil, fmt.Errorf("failed to query queue metrics: %w", err)
	}
	resp := &QueueMetricsResponse{Success: true, PossibleStall: mig.PossibleStall()}
	if q, ok := metrics.Queues["src-traversal"]; ok {
		resp.SrcTraversal = traversalQueueMetrics("src-traversal", q)
	}
	if q, ok := metrics.Queues["dst-traversal"]; ok {
		resp.DstTraversal = traversalQueueMetrics("dst-traversal", q)
	}
	if q, ok := metrics.Queues["copy"]; ok {
		resp.Copy = copyQueueMetrics(q)
	}
	if q, ok := metrics.Queues["delete"]; ok {
		resp.Delete = deleteQueueMetrics(q)
	} else if q, ok := metrics.Queues["delete-traversal"]; ok {
		// Legacy key from before delete queue used its own stats key.
		resp.Delete = deleteQueueMetrics(q)
	}
	return resp, nil
}

// GetLogsFromMigration calls the engine and converts to API response.
func GetLogsFromMigration(mig *migration.Migration, _ GetLogsRequest) (*GetLogsResponse, error) {
	if mig == nil {
		return nil, fmt.Errorf("migration is nil")
	}
	logs, err := mig.GetLogs(1000, true)
	if err != nil {
		return nil, fmt.Errorf("failed to get logs: %w", err)
	}
	out := make(map[string][]LogEntry)
	for level, entries := range logs.ByLevel {
		for i, entry := range entries {
			out[level] = append(out[level], LogEntry{
				ID:    fmt.Sprintf("%d", i+1),
				Level: level,
				Data: map[string]any{
					"message":   entry.Message,
					"timestamp": entry.Timestamp.Format(time.RFC3339),
				},
			})
		}
	}
	return &GetLogsResponse{Success: true, Logs: out}, nil
}

// pathReviewResultToResponse copies engine PathReviewActionResult into API response fields. Returns empty map for nil Deltas.
func pathReviewResultToResponse(res migration.PathReviewActionResult) (affectedCount int64, deltas map[string]int64) {
	affectedCount = res.AffectedCount
	if res.Deltas == nil {
		deltas = make(map[string]int64)
	} else {
		deltas = res.Deltas
	}
	return affectedCount, deltas
}

// mergePathReviewResults merges two PathReviewActionResult (adds AffectedCount, sums Deltas per key).
func mergePathReviewResults(a, b migration.PathReviewActionResult) migration.PathReviewActionResult {
	out := migration.PathReviewActionResult{
		AffectedCount: a.AffectedCount + b.AffectedCount,
		Deltas:        make(map[string]int64),
	}
	for k, v := range a.Deltas {
		out.Deltas[k] += v
	}
	for k, v := range b.Deltas {
		out.Deltas[k] += v
	}
	return out
}

// isCopyPhaseFamily is true when exclusion must be blocked (copy or delete phase families).
func isCopyPhaseFamily(phase string) bool {
	switch phase {
	case migration.PhaseCopying, migration.PhaseCopySuspended, migration.PhaseCopyReview,
		migration.PhaseDeleting, migration.PhaseDeleteSuspended, migration.PhaseDeleteReview:
		return true
	default:
		return false
	}
}

// SetNodesExcluded runs exclusion (excluded=true) or unexclusion (excluded=false) on the engine.
// Caller must call MarkPathReviewChanges after success if needed. Returns error if migration is in
// copy phase. Response includes affectedCount and deltas for UI to update local stats.
func SetNodesExcluded(mig *migration.Migration, req ExclusionRequest, excluded bool) (*ExclusionResponse, error) {
	if mig == nil {
		return exclusionFromOutcome(nilMigrationOutcome()), nil
	}
	if isCopyPhaseFamily(mig.Phase()) {
		resp, err := exclusionFromOutcome(pathReviewOutcome{
			errMsg: "exclusion operations are not available in copy phase (exclusion only applies to traversal)",
			deltas: emptyDeltas(),
		}), fmt.Errorf("exclusion operations are locked in copy phase")
		return resp, err
	}
	if req.All {
		filter := migration.NodeQueryFilter{
			Queue:  "SRC",
			Limit:  1000,
			Offset: 0,
		}
		if excluded {
			if req.Filter != nil && req.Filter.Status != "" {
				filter.Status = req.Filter.Status
			}
		} else {
			filter.Excluded = ptrBool(true)
		}
		res, err := mig.BulkExcludeWithPropagation(filter, excluded)
		resp := exclusionFromOutcome(outcomeFromSingle(res, err))
		return resp, err
	}
	merged, err := runPathReviewBatch(req.NodeIDs, func(nodeID string) (migration.PathReviewActionResult, error) {
		r1, err := mig.SetNodeExcludedWithPropagation("SRC", nodeID, excluded)
		if err != nil {
			return migration.PathReviewActionResult{}, err
		}
		r2, err := mig.SetNodeExcludedWithPropagation("DST", nodeID, excluded)
		if err != nil {
			return migration.PathReviewActionResult{}, err
		}
		return mergePathReviewResults(r1, r2), nil
	})
	resp := exclusionFromOutcome(outcomeFromBatch(merged, err, true))
	return resp, err
}

func ptrBool(v bool) *bool { return &v }

// MarkNodesForRetry calls the engine for discovery or copy retry marking.
// Caller should call MarkPathReviewChanges after success.
func MarkNodesForRetry(mig *migration.Migration, kind RetryKind, req MarkRetryRequest) (*MarkRetryResponse, error) {
	if mig == nil {
		return markRetryFromOutcome(nilMigrationOutcome()), nil
	}
	merged, err := runPathReviewBatch(req.NodeIDs, func(nodeID string) (migration.PathReviewActionResult, error) {
		return retryNode(mig, kind, nodeID, true)
	})
	resp := markRetryFromOutcome(outcomeFromBatch(merged, err, true))
	return resp, err
}

// UnmarkNodeForRetry calls the engine for discovery or copy retry unmarking.
// Caller should call MarkPathReviewChanges after success.
func UnmarkNodeForRetry(mig *migration.Migration, kind RetryKind, nodeID string) (*MarkRetryResponse, error) {
	if mig == nil {
		return markRetryFromOutcome(nilMigrationOutcome()), nil
	}
	res, err := retryNode(mig, kind, nodeID, false)
	resp := markRetryFromOutcome(outcomeFromSingle(res, err))
	return resp, err
}

// retryNode dispatches to the engine's mark/unmark retry method for the given kind.
func retryNode(mig *migration.Migration, kind RetryKind, nodeID string, mark bool) (migration.PathReviewActionResult, error) {
	switch {
	case mark && kind == RetryKindCopy:
		return mig.MarkNodeForRetryCopy(nodeID)
	case mark && kind == RetryKindDelete:
		return mig.MarkNodeForRetryDelete(nodeID)
	case mark:
		return mig.MarkNodeForRetryDiscovery(nodeID)
	case kind == RetryKindCopy:
		return mig.UnmarkNodeForRetryCopy(nodeID)
	case kind == RetryKindDelete:
		return mig.UnmarkNodeForRetryDelete(nodeID)
	default:
		return mig.UnmarkNodeForRetryDiscovery(nodeID)
	}
}

// PrepareSourceCleanup aligns delete_status with selected SRC nodes before source removal.
func PrepareSourceCleanup(mig *migration.Migration, req PrepareSourceCleanupRequest) (*MarkRetryResponse, error) {
	if mig == nil {
		return markRetryFromOutcome(nilMigrationOutcome()), nil
	}
	res, err := mig.PrepareSourceCleanup(req.NodeIDs, req.DeselectedNodeIDs)
	resp := markRetryFromOutcome(outcomeFromSingle(res, err))
	return resp, err
}

// SkipNodeDelete opts a node out of source removal during cleanup planning.
func SkipNodeDelete(mig *migration.Migration, nodeID string) (*MarkRetryResponse, error) {
	if mig == nil {
		return markRetryFromOutcome(nilMigrationOutcome()), nil
	}
	res, err := mig.SkipNodeDelete(nodeID)
	resp := markRetryFromOutcome(outcomeFromSingle(res, err))
	return resp, err
}

// UnskipNodeDelete re-includes a node in source removal during cleanup planning.
func UnskipNodeDelete(mig *migration.Migration, nodeID string) (*MarkRetryResponse, error) {
	if mig == nil {
		return markRetryFromOutcome(nilMigrationOutcome()), nil
	}
	res, err := mig.UnskipNodeDelete(nodeID)
	resp := markRetryFromOutcome(outcomeFromSingle(res, err))
	return resp, err
}
