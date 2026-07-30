package migrationops

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"codeberg.org/Sylos/Migration-Engine/pkg/convert"
	"codeberg.org/Sylos/Migration-Engine/pkg/migration"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge"
)

// ErrSearchRequiresFilter is returned when search/count is called with no narrowing predicates.
var ErrSearchRequiresFilter = errors.New("search requires at least one filter")

// SearchRequestHasFilter reports whether req carries any narrowing search predicate.
// Mirrors Migration-Engine ReviewFilterHasSearchPredicate after condition mapping:
// statusSearchType / sort alone do not count; IncludeDestinationOnly=false does.
func SearchRequestHasFilter(req corebridge.SearchRequest) bool {
	for _, c := range req.Conditions {
		field := strings.ToLower(strings.TrimSpace(c.Field))
		switch field {
		case "path", "name":
			if s, ok := searchConditionString(c.Value); ok && strings.TrimSpace(s) != "" {
				return true
			}
		case "type":
			if s, ok := searchConditionString(c.Value); ok && strings.TrimSpace(s) != "" {
				return true
			}
		case "traversalstatus", "copystatus", "deletestatus":
			if s, ok := searchConditionString(c.Value); ok && strings.TrimSpace(s) != "" {
				return true
			}
		case "pathissuestatus", "pathissuefilter", "compatibilitystatus",
			"pathissuecategory", "compatibilitycategory":
			if s, ok := searchConditionString(c.Value); ok && strings.TrimSpace(s) != "" {
				return true
			}
		case "depth", "size":
			if searchConditionHasNumber(c.Value) {
				return true
			}
		}
	}
	if req.IncludeDestinationOnly != nil && !*req.IncludeDestinationOnly {
		return true
	}
	return false
}

func searchConditionString(v any) (string, bool) {
	if v == nil {
		return "", false
	}
	switch t := v.(type) {
	case string:
		return t, true
	case float64:
		return strconv.FormatInt(int64(t), 10), true
	case int:
		return strconv.Itoa(t), true
	case int64:
		return strconv.FormatInt(t, 10), true
	default:
		return fmt.Sprintf("%v", t), true
	}
}

func searchConditionHasNumber(v any) bool {
	switch t := v.(type) {
	case int, int64, float64:
		return true
	case string:
		_, err := strconv.ParseFloat(strings.TrimSpace(t), 64)
		return err == nil
	default:
		return false
	}
}

// InspectMigrationStatus calls the engine's DB-backed inspection and returns the result.
func InspectMigrationStatus(mig *migration.Migration) (migration.MigrationStatus, error) {
	if mig == nil || mig.DB == nil {
		return migration.MigrationStatus{}, fmt.Errorf("migration or database is nil")
	}
	return migration.InspectMigrationStatus(mig.DB)
}

// PathReviewStatsFromMigration returns review stats projected for the requested UI view.
func PathReviewStatsFromMigration(mig *migration.Migration, view string) (*corebridge.PathReviewStats, error) {
	if mig == nil {
		return nil, fmt.Errorf("migration is nil")
	}
	stats := mig.GetPathReviewStatsForView(view)
	return &corebridge.PathReviewStats{
		PendingCount:        stats.PendingCount,
		FailedCount:         stats.FailedCount,
		ExcludedCount:       stats.ExcludedCount,
		PendingRetriesCount: stats.PendingRetriesCount,
		SuccessfulCount:     stats.SuccessfulCount,
		FoldersCount:        stats.FoldersCount,
		FilesCount:          stats.FilesCount,
		FoldersRatio:        stats.FoldersRatio,
		FilesRatio:          stats.FilesRatio,
		TotalFileSize: corebridge.FileSizeStats{
			Src:      stats.TotalFileSize.Src,
			Dst:      stats.TotalFileSize.Dst,
			Selected: stats.TotalFileSize.Selected,
		},
	}, nil
}

// resolveSort extracts sort field and direction from an optional SortOption, defaulting to ascending.
func resolveSort(sort *corebridge.SortOption) (sortBy, sortDirection string) {
	sortDirection = "asc"
	if sort != nil {
		sortBy = sort.Field
		if sort.Direction != "" {
			sortDirection = strings.ToLower(sort.Direction)
		}
	}
	return sortBy, sortDirection
}

// diffListResponse builds the paginated API response from engine diff items (counted tree path).
func diffListResponse(items []migration.DiffItem, offset, limit, total int) corebridge.ListChildrenDiffsResponse {
	out := make(map[string]corebridge.PathNodes, len(items))
	order := make([]string, 0, len(items))
	for _, item := range items {
		out[item.Path] = diffItemToPathNodes(item)
		order = append(order, item.Path)
	}
	return corebridge.ListChildrenDiffsResponse{
		Items:     out,
		ItemOrder: order,
		Pagination: corebridge.PaginationInfo{
			Offset:  offset,
			Limit:   limit,
			Total:   &total,
			HasMore: offset+limit < total,
		},
	}
}

// searchListResponse builds the paginated search response; HasMore comes from the engine and Total may be nil/omitted.
func searchListResponse(result migration.SearchResult) corebridge.ListChildrenDiffsResponse {
	out := make(map[string]corebridge.PathNodes, len(result.Items))
	order := make([]string, 0, len(result.Items))
	for _, item := range result.Items {
		out[item.Path] = diffItemToPathNodes(item)
		order = append(order, item.Path)
	}
	return corebridge.ListChildrenDiffsResponse{
		Items:     out,
		ItemOrder: order,
		Pagination: corebridge.PaginationInfo{
			Offset:  result.Offset,
			Limit:   result.Limit,
			Total:   result.Total,
			HasMore: result.HasMore,
		},
	}
}

func diffItemToPathNodes(item migration.DiffItem) corebridge.PathNodes {
	pathNodes := corebridge.PathNodes{
		ResolvedDstName: strings.TrimSpace(item.ResolvedDstName),
	}
	if !item.MissingOnSource {
		pathNodes.Src = &corebridge.PathNodeItem{
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
		dstName := item.Name
		dstPath := item.Path
		if pathNodes.ResolvedDstName != "" {
			dstName = pathNodes.ResolvedDstName
			dstPath = joinParentPath(item.Path, pathNodes.ResolvedDstName)
		}
		pathNodes.Dst = &corebridge.PathNodeItem{
			Queue:           "DST",
			Id:              item.DstNodeID,
			Name:            dstName,
			LocationPath:    dstPath,
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

// joinParentPath replaces the basename of srcPath with newBase.
func joinParentPath(srcPath, newBase string) string {
	newBase = strings.TrimSpace(newBase)
	if newBase == "" {
		return srcPath
	}
	srcPath = strings.TrimSpace(srcPath)
	if srcPath == "" || srcPath == "/" {
		return "/" + newBase
	}
	i := strings.LastIndex(srcPath, "/")
	if i < 0 {
		return "/" + newBase
	}
	if i == 0 {
		return "/" + newBase
	}
	return srcPath[:i] + "/" + newBase
}

// ListChildrenDiffs calls the engine and converts the result to API response.
func ListChildrenDiffs(mig *migration.Migration, req corebridge.ListChildrenDiffsRequest) (corebridge.ListChildrenDiffsResponse, error) {
	if mig == nil {
		return corebridge.ListChildrenDiffsResponse{}, fmt.Errorf("migration is nil")
	}
	sortBy, sortDirection := resolveSort(req.Sort)
	result, err := mig.ListChildrenDiffs(migration.ListChildrenDiffsRequest{
		Path:                   req.Path,
		Limit:                  req.Limit,
		Offset:                 req.Offset,
		SortBy:                 sortBy,
		SortDirection:          sortDirection,
		FoldersOnly:            req.FoldersOnly,
		IncludeDestinationOnly: req.IncludeDestinationOnly,
	})
	if err != nil {
		return corebridge.ListChildrenDiffsResponse{}, fmt.Errorf("failed to list children diffs: %w", err)
	}
	return diffListResponse(result.Items, result.Offset, result.Limit, result.Total), nil
}

// GetChildrenDiffsStats calls the engine and converts to API response.
func GetChildrenDiffsStats(mig *migration.Migration, path string, foldersOnly bool, includeDestinationOnly *bool) (corebridge.DiffsStatsResponse, error) {
	if mig == nil {
		return corebridge.DiffsStatsResponse{}, fmt.Errorf("migration is nil")
	}
	stats, err := mig.GetChildrenDiffsStats(path, foldersOnly, includeDestinationOnly)
	if err != nil {
		return corebridge.DiffsStatsResponse{}, fmt.Errorf("failed to get diffs stats: %w", err)
	}
	folders := stats.Folders
	files := stats.Files
	if foldersOnly {
		files = 0
	}
	return corebridge.DiffsStatsResponse{
		Total:        stats.Total,
		TotalFolders: folders,
		TotalFiles:   files,
	}, nil
}

func enginePathReviewConditions(req corebridge.SearchRequest) []migration.PathReviewSearchCondition {
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
func engineSearchRequest(req corebridge.SearchRequest, offset, limit int) migration.SearchRequest {
	sortBy, sortDirection := resolveSort(req.Sort)
	return migration.SearchRequest{
		Path:                   "",
		Limit:                  limit,
		Offset:                 offset,
		SortBy:                 sortBy,
		SortDirection:          sortDirection,
		Conditions:             enginePathReviewConditions(req),
		StatusSearchType:       req.StatusSearchType,
		IncludeDestinationOnly: req.IncludeDestinationOnly,
	}
}

// SearchPathReviewItems calls the engine and converts to API response.
// Caller should reject zero-filter requests via SearchRequestHasFilter before calling.
func SearchPathReviewItems(mig *migration.Migration, req corebridge.SearchRequest, offset, limit int) (corebridge.ListChildrenDiffsResponse, error) {
	if mig == nil {
		return corebridge.ListChildrenDiffsResponse{}, fmt.Errorf("migration is nil")
	}
	if !SearchRequestHasFilter(req) {
		return corebridge.ListChildrenDiffsResponse{}, ErrSearchRequiresFilter
	}
	result, err := mig.SearchPathReviewItems(engineSearchRequest(req, offset, limit))
	if err != nil {
		if errors.Is(err, migration.ErrSearchRequiresFilter) {
			return corebridge.ListChildrenDiffsResponse{}, ErrSearchRequiresFilter
		}
		return corebridge.ListChildrenDiffsResponse{}, fmt.Errorf("failed to search path review items: %w", err)
	}
	return searchListResponse(result), nil
}

// GetSearchStats calls the engine and converts to API response (exact total/folder/file counts).
// Caller should reject zero-filter requests via SearchRequestHasFilter before calling.
func GetSearchStats(mig *migration.Migration, req corebridge.SearchRequest) (corebridge.DiffsStatsResponse, error) {
	if mig == nil {
		return corebridge.DiffsStatsResponse{}, fmt.Errorf("migration is nil")
	}
	if !SearchRequestHasFilter(req) {
		return corebridge.DiffsStatsResponse{}, ErrSearchRequiresFilter
	}
	stats, err := mig.GetSearchStats(engineSearchRequest(req, 0, 10000))
	if err != nil {
		return corebridge.DiffsStatsResponse{}, fmt.Errorf("failed to get search stats: %w", err)
	}
	return corebridge.DiffsStatsResponse{
		Total:        stats.Total,
		TotalFolders: stats.Folders,
		TotalFiles:   stats.Files,
	}, nil
}

// commonQueueState fills the state fields shared by traversal and copy queues.
func commonQueueState(m *corebridge.ExternalQueueMetrics, name string, q map[string]any) {
	m.Name = name
	m.Round = convert.ToNumber[int](q["round"])
	m.Pending = convert.ToNumber[int](q["pending"])
	m.InProgress = convert.ToNumber[int](q["in_progress"])
	m.Workers = convert.ToNumber[int](q["workers"])
	m.TotalPending = convert.ToNumber[int](q["total_pending"])
	m.TotalFailed = convert.ToNumber[int](q["total_failed"])
	m.PossibleStall = convert.ToBool(q["possible_stall"])
	m.State = convert.ToString(q["state"])
	m.RoundExpected = convert.ToNumber[int](q["round_expected"])
	m.RoundCompleted = convert.ToNumber[int](q["round_completed"])
	m.CopyPass = convert.ToNumber[int](q["copy_pass"])
	m.RateLimitedUntilSrc = convert.ToString(q["rate_limited_until_src"])
	m.RateLimitedUntilDst = convert.ToString(q["rate_limited_until_dst"])
	m.RateLimitedRemainingMsSrc = convert.ToNumber[int64](q["rate_limited_remaining_ms_src"])
	m.RateLimitedRemainingMsDst = convert.ToNumber[int64](q["rate_limited_remaining_ms_dst"])
	m.InterOpDelayMs = convert.ToNumber[int64](q["inter_op_delay_ms"])
}

func traversalQueueMetrics(name string, q map[string]any) *corebridge.ExternalQueueMetrics {
	m := &corebridge.ExternalQueueMetrics{
		FilesDiscoveredTotal:     convert.ToNumber[int64](q["files_discovered_total"]),
		FoldersDiscoveredTotal:   convert.ToNumber[int64](q["folders_discovered_total"]),
		DiscoveryRateItemsPerSec: convert.ToNumber[float64](q["discovery_rate_items_per_sec"]),
		TotalDiscovered:          convert.ToNumber[int64](q["total_discovered"]),
	}
	commonQueueState(m, name, q)
	return m
}

func phaseQueueMetrics(name string, q map[string]any) *corebridge.ExternalQueueMetrics {
	m := &corebridge.ExternalQueueMetrics{
		Folders:              convert.ToNumber[int64](q["folders"]),
		Files:                convert.ToNumber[int64](q["files"]),
		Total:                convert.ToNumber[int64](q["total"]),
		Bytes:                convert.ToNumber[int64](q["bytes"]),
		ItemsPerSecond:       convert.ToNumber[float64](q["items_per_second"]),
		BytesPerSecond:       convert.ToNumber[float64](q["bytes_per_second"]),
		FoldersExpected:      convert.ToNumber[int64](q["folders_expected"]),
		FilesExpected:        convert.ToNumber[int64](q["files_expected"]),
		TotalExpected:        convert.ToNumber[int64](q["total_expected"]),
		ItemsCompleted:       convert.ToNumber[int64](q["items_completed"]),
		ItemsTotal:           convert.ToNumber[int64](q["items_total"]),
		ItemsProgressPercent: convert.ToNumber[float64](q["items_progress_percent"]),
		ItemsFailedPercent:   convert.ToNumber[float64](q["items_failed_percent"]),
		BytesTotal:           convert.ToNumber[int64](q["bytes_total"]),
		BytesFailed:          convert.ToNumber[int64](q["bytes_failed"]),
		BytesProgressPercent: convert.ToNumber[float64](q["bytes_progress_percent"]),
		BytesFailedPercent:   convert.ToNumber[float64](q["bytes_failed_percent"]),
		ProgressPercent:      convert.ToNumber[float64](q["progress_percent"]),
	}
	commonQueueState(m, name, q)
	m.EtaBasis = convert.ToString(q["eta_basis"])
	if _, ok := q["eta_seconds"]; ok {
		v := convert.ToNumber[float64](q["eta_seconds"])
		m.EtaSeconds = &v
	}
	return m
}

// QueueMetricsFromMigration calls the engine and converts to API response.
func QueueMetricsFromMigration(mig *migration.Migration) (*corebridge.QueueMetricsResponse, error) {
	if mig == nil {
		return nil, fmt.Errorf("migration is nil")
	}
	metrics, err := mig.GetQueueMetrics()
	if err != nil {
		return nil, fmt.Errorf("failed to query queue metrics: %w", err)
	}
	resp := &corebridge.QueueMetricsResponse{Success: true, PossibleStall: mig.PossibleStall()}
	if q, ok := metrics.Queues["src-traversal"]; ok {
		resp.SrcTraversal = traversalQueueMetrics("src-traversal", q)
	}
	if q, ok := metrics.Queues["dst-traversal"]; ok {
		resp.DstTraversal = traversalQueueMetrics("dst-traversal", q)
	}
	if q, ok := metrics.Queues["copy"]; ok {
		resp.Copy = phaseQueueMetrics("copy", q)
	}
	if q, ok := metrics.Queues["delete"]; ok {
		resp.Delete = phaseQueueMetrics("delete", q)
	} else if q, ok := metrics.Queues["delete-traversal"]; ok {
		// Legacy key from before delete queue used its own stats key.
		resp.Delete = phaseQueueMetrics("delete", q)
	}
	return resp, nil
}

// GetLogsFromMigration calls the engine and converts to API response.
func GetLogsFromMigration(mig *migration.Migration, _ corebridge.GetLogsRequest) (*corebridge.GetLogsResponse, error) {
	if mig == nil {
		return nil, fmt.Errorf("migration is nil")
	}
	logs, err := mig.GetLogs(1000, true)
	if err != nil {
		return nil, fmt.Errorf("failed to get logs: %w", err)
	}
	out := make(map[string][]corebridge.LogEntry)
	for level, entries := range logs.ByLevel {
		for _, entry := range entries {
			id := entry.ID
			if id == "" {
				// Stable fallback so UI polling can dedupe across snapshots.
				id = fmt.Sprintf("%s|%s|%s", level, entry.Timestamp.Format(time.RFC3339Nano), entry.Message)
			}
			out[level] = append(out[level], corebridge.LogEntry{
				ID:    id,
				Level: level,
				Data: map[string]any{
					"message":   entry.Message,
					"timestamp": entry.Timestamp.Format(time.RFC3339Nano),
				},
			})
		}
	}
	return &corebridge.GetLogsResponse{Success: true, Logs: out}, nil
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
func SetNodesExcluded(mig *migration.Migration, req corebridge.ExclusionRequest, excluded bool) (*corebridge.ExclusionResponse, error) {
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
		// Exclude/unexclude is SRC-only; a second DST call would re-mutate the same SRC id.
		return mig.SetNodeExcludedWithPropagation("SRC", nodeID, excluded)
	})
	resp := exclusionFromOutcome(outcomeFromBatch(merged, err, true))
	return resp, err
}

func ptrBool(v bool) *bool { return &v }

// MarkNodesForRetry calls the engine for discovery or copy retry marking.
// Caller should call MarkPathReviewChanges after success.
func MarkNodesForRetry(mig *migration.Migration, kind RetryKind, req corebridge.MarkRetryRequest) (*corebridge.MarkRetryResponse, error) {
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
func UnmarkNodeForRetry(mig *migration.Migration, kind RetryKind, nodeID string) (*corebridge.MarkRetryResponse, error) {
	if mig == nil {
		return markRetryFromOutcome(nilMigrationOutcome()), nil
	}
	res, err := retryNode(mig, kind, nodeID, false)
	resp := markRetryFromOutcome(outcomeFromSingle(res, err))
	return resp, err
}

// retryNode dispatches to the engine's mark/unmark retry method for the given kind.
func retryNode(mig *migration.Migration, kind RetryKind, nodeID string, mark bool) (migration.PathReviewActionResult, error) {
	engineKind := migration.RetryMutationDiscovery
	switch kind {
	case RetryKindCopy:
		engineKind = migration.RetryMutationCopy
	case RetryKindDelete:
		engineKind = migration.RetryMutationDelete
	}
	return mig.SetNodeRetryMark(engineKind, nodeID, mark)
}

// PrepareSourceCleanup aligns delete_status with selected SRC nodes before source removal.
func PrepareSourceCleanup(mig *migration.Migration, req corebridge.PrepareSourceCleanupRequest) (*corebridge.MarkRetryResponse, error) {
	if mig == nil {
		return markRetryFromOutcome(nilMigrationOutcome()), nil
	}
	res, err := mig.PrepareSourceCleanup(req.NodeIDs, req.DeselectedNodeIDs)
	resp := markRetryFromOutcome(outcomeFromSingle(res, err))
	return resp, err
}

// SkipNodeDelete opts a node out of source removal during cleanup planning.
func SkipNodeDelete(mig *migration.Migration, nodeID string) (*corebridge.MarkRetryResponse, error) {
	if mig == nil {
		return markRetryFromOutcome(nilMigrationOutcome()), nil
	}
	res, err := mig.SkipNodeDelete(nodeID)
	resp := markRetryFromOutcome(outcomeFromSingle(res, err))
	return resp, err
}

// UnskipNodeDelete re-includes a node in source removal during cleanup planning.
func UnskipNodeDelete(mig *migration.Migration, nodeID string) (*corebridge.MarkRetryResponse, error) {
	if mig == nil {
		return markRetryFromOutcome(nilMigrationOutcome()), nil
	}
	res, err := mig.UnskipNodeDelete(nodeID)
	resp := markRetryFromOutcome(outcomeFromSingle(res, err))
	return resp, err
}
