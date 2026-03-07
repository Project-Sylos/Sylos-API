package corebridge

import (
	"fmt"
	"strings"
	"time"

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
			CopyStatus:      item.CopyStatus,
		}
	}
	return pathNodes
}

// ListChildrenDiffs calls the engine and converts the result to API response.
func ListChildrenDiffs(mig *migration.Migration, req ListChildrenDiffsRequest) (ListChildrenDiffsResponse, error) {
	if mig == nil {
		return ListChildrenDiffsResponse{}, fmt.Errorf("migration is nil")
	}
	sortBy := ""
	sortDirection := "asc"
	if req.Sort != nil {
		sortBy = req.Sort.Field
		if req.Sort.Direction != "" {
			sortDirection = strings.ToLower(req.Sort.Direction)
		}
	}
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
	items := make(map[string]PathNodes)
	for _, item := range result.Items {
		items[item.Path] = diffItemToPathNodes(item)
	}
	hasMore := result.Offset+result.Limit < result.Total
	return ListChildrenDiffsResponse{
		Items: items,
		Pagination: PaginationInfo{
			Offset:  result.Offset,
			Limit:   result.Limit,
			Total:   result.Total,
			HasMore: hasMore,
		},
	}, nil
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

// SearchPathReviewItems calls the engine and converts to API response.
func SearchPathReviewItems(mig *migration.Migration, req SearchRequest, offset, limit int) (ListChildrenDiffsResponse, error) {
	if mig == nil {
		return ListChildrenDiffsResponse{}, fmt.Errorf("migration is nil")
	}
	query := ""
	for _, cond := range req.Conditions {
		if cond.Field == "path" || cond.Field == "name" {
			query = fmt.Sprintf("%v", cond.Value)
			break
		}
	}
	sortBy := ""
	sortDirection := "asc"
	if req.Sort != nil {
		sortBy = req.Sort.Field
		if req.Sort.Direction != "" {
			sortDirection = strings.ToLower(req.Sort.Direction)
		}
	}
	result, err := mig.SearchPathReviewItems(migration.SearchRequest{
		Query:         query,
		Path:          "",
		Limit:         limit,
		Offset:        offset,
		SortBy:        sortBy,
		SortDirection: sortDirection,
	})
	if err != nil {
		return ListChildrenDiffsResponse{}, fmt.Errorf("failed to search path review items: %w", err)
	}
	items := make(map[string]PathNodes)
	for _, item := range result.Items {
		items[item.Path] = diffItemToPathNodes(item)
	}
	return ListChildrenDiffsResponse{
		Items: items,
		Pagination: PaginationInfo{
			Offset:  result.Offset,
			Limit:   result.Limit,
			Total:   result.Total,
			HasMore: result.Offset+result.Limit < result.Total,
		},
	}, nil
}

// GetSearchStats calls the engine and converts to API response.
func GetSearchStats(mig *migration.Migration, req SearchRequest) (DiffsStatsResponse, error) {
	if mig == nil {
		return DiffsStatsResponse{}, fmt.Errorf("migration is nil")
	}
	query := ""
	for _, cond := range req.Conditions {
		if cond.Field == "path" || cond.Field == "name" {
			query = fmt.Sprintf("%v", cond.Value)
			break
		}
	}
	sortBy := ""
	sortDirection := "asc"
	if req.Sort != nil {
		sortBy = req.Sort.Field
		if req.Sort.Direction != "" {
			sortDirection = strings.ToLower(req.Sort.Direction)
		}
	}
	engineReq := migration.SearchRequest{
		Query:         query,
		Path:          "",
		Limit:         10000,
		Offset:        0,
		SortBy:        sortBy,
		SortDirection: sortDirection,
		FoldersOnly:   false,
		Status:        req.StatusSearchType,
	}
	stats, err := mig.GetSearchStats(engineReq)
	if err != nil {
		return DiffsStatsResponse{}, fmt.Errorf("failed to get search stats: %w", err)
	}
	return DiffsStatsResponse{
		Total:        stats.Total,
		TotalFolders: stats.Folders,
		TotalFiles:   stats.Files,
	}, nil
}

func asInt(v any) int {
	switch t := v.(type) {
	case int:
		return t
	case int32:
		return int(t)
	case int64:
		return int(t)
	case float64:
		return int(t)
	default:
		return 0
	}
}

func asInt64(v any) int64 {
	switch t := v.(type) {
	case int:
		return int64(t)
	case int32:
		return int64(t)
	case int64:
		return t
	case float64:
		return int64(t)
	default:
		return 0
	}
}

func asFloat64(v any) float64 {
	switch t := v.(type) {
	case float64:
		return t
	case float32:
		return float64(t)
	case int:
		return float64(t)
	case int64:
		return float64(t)
	default:
		return 0
	}
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
	resp := &QueueMetricsResponse{Success: true}
	if q, ok := metrics.Queues["src-traversal"]; ok {
		resp.SrcTraversal = &ExternalQueueMetrics{
			Name:                     "src-traversal",
			FilesDiscoveredTotal:     asInt64(q["files_discovered_total"]),
			FoldersDiscoveredTotal:   asInt64(q["folders_discovered_total"]),
			DiscoveryRateItemsPerSec: asFloat64(q["discovery_rate_items_per_sec"]),
			TotalDiscovered:          asInt64(q["total_discovered"]),
			Round:                    asInt(q["round"]),
			Pending:                  asInt(q["pending"]),
			InProgress:               asInt(q["in_progress"]),
			Workers:                  asInt(q["workers"]),
			TotalPending:             asInt(q["total_pending"]),
			TotalFailed:              asInt(q["total_failed"]),
		}
	}
	if q, ok := metrics.Queues["dst-traversal"]; ok {
		resp.DstTraversal = &ExternalQueueMetrics{
			Name:                     "dst-traversal",
			FilesDiscoveredTotal:     asInt64(q["files_discovered_total"]),
			FoldersDiscoveredTotal:   asInt64(q["folders_discovered_total"]),
			DiscoveryRateItemsPerSec: asFloat64(q["discovery_rate_items_per_sec"]),
			TotalDiscovered:          asInt64(q["total_discovered"]),
			Round:                    asInt(q["round"]),
			Pending:                  asInt(q["pending"]),
			InProgress:               asInt(q["in_progress"]),
			Workers:                  asInt(q["workers"]),
			TotalPending:             asInt(q["total_pending"]),
			TotalFailed:              asInt(q["total_failed"]),
		}
	}
	if q, ok := metrics.Queues["copy"]; ok {
		resp.Copy = &ExternalQueueMetrics{
			Name:           "copy",
			Folders:        asInt64(q["folders"]),
			Files:          asInt64(q["files"]),
			Total:          asInt64(q["total"]),
			Bytes:          asInt64(q["bytes"]),
			ItemsPerSecond: asFloat64(q["items_per_second"]),
			BytesPerSecond: asFloat64(q["bytes_per_second"]),
			Round:          asInt(q["round"]),
			Pending:        asInt(q["pending"]),
			InProgress:     asInt(q["in_progress"]),
			Workers:        asInt(q["workers"]),
			TotalPending:   asInt(q["total_pending"]),
			TotalFailed:    asInt(q["total_failed"]),
		}
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

// ExcludeNodes runs exclusion on the engine. Caller must call MarkPathReviewChanges after success if needed.
// Returns error if migration is in copy phase. Response includes affectedCount and deltas for UI to update local stats.
func ExcludeNodes(mig *migration.Migration, req ExclusionRequest) (*ExclusionResponse, error) {
	if mig == nil {
		return &ExclusionResponse{Success: false, Error: "migration is nil", Deltas: map[string]int64{}}, nil
	}
	if mig.Phase() == migration.PhaseCopying || mig.Phase() == migration.PhaseCompleted {
		return &ExclusionResponse{
			Success: false,
			Error:   "exclusion operations are not available in copy phase (exclusion only applies to traversal)",
			Deltas:  map[string]int64{},
		}, fmt.Errorf("exclusion operations are locked in copy phase")
	}
	if req.All {
		filter := migration.NodeQueryFilter{
			Queue:  "SRC",
			Limit:  1000,
			Offset: 0,
		}
		if req.Filter != nil && req.Filter.Status != "" {
			filter.Status = req.Filter.Status
		}
		res, err := mig.BulkExcludeWithPropagation(filter, true)
		if err != nil {
			return &ExclusionResponse{Success: false, Error: err.Error(), Deltas: map[string]int64{}}, err
		}
		aff, deltas := pathReviewResultToResponse(res)
		return &ExclusionResponse{Success: true, AffectedCount: aff, Deltas: deltas}, nil
	}
	var merged migration.PathReviewActionResult
	for _, nodeID := range req.NodeIDs {
		r1, err := mig.SetNodeExcludedWithPropagation("SRC", nodeID, true)
		if err != nil {
			aff, deltas := pathReviewResultToResponse(merged)
			return &ExclusionResponse{Success: false, Error: err.Error(), AffectedCount: aff, Deltas: deltas}, err
		}
		r2, err := mig.SetNodeExcludedWithPropagation("DST", nodeID, true)
		if err != nil {
			aff, deltas := pathReviewResultToResponse(merged)
			return &ExclusionResponse{Success: false, Error: err.Error(), AffectedCount: aff, Deltas: deltas}, err
		}
		merged = mergePathReviewResults(mergePathReviewResults(merged, r1), r2)
	}
	aff, deltas := pathReviewResultToResponse(merged)
	return &ExclusionResponse{Success: true, AffectedCount: aff, Deltas: deltas}, nil
}

// UnexcludeNodes runs unexclude on the engine. Caller must call MarkPathReviewChanges after success if needed.
func UnexcludeNodes(mig *migration.Migration, req ExclusionRequest) (*ExclusionResponse, error) {
	if mig == nil {
		return &ExclusionResponse{Success: false, Error: "migration is nil", Deltas: map[string]int64{}}, nil
	}
	if mig.Phase() == migration.PhaseCopying || mig.Phase() == migration.PhaseCompleted {
		return &ExclusionResponse{
			Success: false,
			Error:   "exclusion operations are not available in copy phase (exclusion only applies to traversal)",
			Deltas:  map[string]int64{},
		}, fmt.Errorf("exclusion operations are locked in copy phase")
	}
	if req.All {
		res, err := mig.BulkExcludeWithPropagation(migration.NodeQueryFilter{
			Queue:    "SRC",
			Excluded: ptrBool(true),
			Limit:    1000,
		}, false)
		if err != nil {
			return &ExclusionResponse{Success: false, Error: err.Error(), Deltas: map[string]int64{}}, err
		}
		aff, deltas := pathReviewResultToResponse(res)
		return &ExclusionResponse{Success: true, AffectedCount: aff, Deltas: deltas}, nil
	}
	var merged migration.PathReviewActionResult
	for _, nodeID := range req.NodeIDs {
		r1, err := mig.SetNodeExcludedWithPropagation("SRC", nodeID, false)
		if err != nil {
			aff, deltas := pathReviewResultToResponse(merged)
			return &ExclusionResponse{Success: false, Error: err.Error(), AffectedCount: aff, Deltas: deltas}, err
		}
		r2, err := mig.SetNodeExcludedWithPropagation("DST", nodeID, false)
		if err != nil {
			aff, deltas := pathReviewResultToResponse(merged)
			return &ExclusionResponse{Success: false, Error: err.Error(), AffectedCount: aff, Deltas: deltas}, err
		}
		merged = mergePathReviewResults(mergePathReviewResults(merged, r1), r2)
	}
	aff, deltas := pathReviewResultToResponse(merged)
	return &ExclusionResponse{Success: true, AffectedCount: aff, Deltas: deltas}, nil
}

func ptrBool(v bool) *bool { return &v }

// MarkNodesForRetryDiscovery calls the engine. Caller should call MarkPathReviewChanges after success.
func MarkNodesForRetryDiscovery(mig *migration.Migration, req MarkRetryRequest) (*MarkRetryResponse, error) {
	if mig == nil {
		return &MarkRetryResponse{Success: false, Error: "migration is nil", Deltas: map[string]int64{}}, nil
	}
	var merged migration.PathReviewActionResult
	for _, nodeID := range req.NodeIDs {
		res, err := mig.MarkNodeForRetryDiscovery(nodeID)
		if err != nil {
			aff, deltas := pathReviewResultToResponse(merged)
			return &MarkRetryResponse{Success: false, Error: err.Error(), AffectedCount: aff, Deltas: deltas}, err
		}
		merged = mergePathReviewResults(merged, res)
	}
	aff, deltas := pathReviewResultToResponse(merged)
	return &MarkRetryResponse{Success: true, AffectedCount: aff, Deltas: deltas}, nil
}

// MarkNodesForRetryCopy calls the engine. Caller should call MarkPathReviewChanges after success.
func MarkNodesForRetryCopy(mig *migration.Migration, req MarkRetryRequest) (*MarkRetryResponse, error) {
	if mig == nil {
		return &MarkRetryResponse{Success: false, Error: "migration is nil", Deltas: map[string]int64{}}, nil
	}
	var merged migration.PathReviewActionResult
	for _, nodeID := range req.NodeIDs {
		res, err := mig.MarkNodeForRetryCopy(nodeID)
		if err != nil {
			aff, deltas := pathReviewResultToResponse(merged)
			return &MarkRetryResponse{Success: false, Error: err.Error(), AffectedCount: aff, Deltas: deltas}, err
		}
		merged = mergePathReviewResults(merged, res)
	}
	aff, deltas := pathReviewResultToResponse(merged)
	return &MarkRetryResponse{Success: true, AffectedCount: aff, Deltas: deltas}, nil
}

// UnmarkNodeForRetryDiscovery calls the engine. Caller should call MarkPathReviewChanges after success.
func UnmarkNodeForRetryDiscovery(mig *migration.Migration, nodeID string) (*MarkRetryResponse, error) {
	if mig == nil {
		return &MarkRetryResponse{Success: false, Error: "migration is nil", Deltas: map[string]int64{}}, nil
	}
	res, err := mig.UnmarkNodeForRetryDiscovery(nodeID)
	if err != nil {
		return &MarkRetryResponse{Success: false, Error: err.Error(), Deltas: map[string]int64{}}, err
	}
	aff, deltas := pathReviewResultToResponse(res)
	return &MarkRetryResponse{Success: true, AffectedCount: aff, Deltas: deltas}, nil
}

// UnmarkNodeForRetryCopy calls the engine. Caller should call MarkPathReviewChanges after success.
func UnmarkNodeForRetryCopy(mig *migration.Migration, nodeID string) (*MarkRetryResponse, error) {
	if mig == nil {
		return &MarkRetryResponse{Success: false, Error: "migration is nil", Deltas: map[string]int64{}}, nil
	}
	res, err := mig.UnmarkNodeForRetryCopy(nodeID)
	if err != nil {
		return &MarkRetryResponse{Success: false, Error: err.Error(), Deltas: map[string]int64{}}, err
	}
	aff, deltas := pathReviewResultToResponse(res)
	return &MarkRetryResponse{Success: true, AffectedCount: aff, Deltas: deltas}, nil
}
