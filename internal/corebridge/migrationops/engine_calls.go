package migrationops

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"codeberg.org/Sylos/Migration-Engine/pkg/convert"
	"codeberg.org/Sylos/Migration-Engine/pkg/db/stats"
	"codeberg.org/Sylos/Migration-Engine/pkg/migration"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge"
	"github.com/google/uuid"
)

// ApplyDisableQueryTimeout toggles the 2m review query deadline on the migration DB handle.
func ApplyDisableQueryTimeout(mig *migration.Migration, disable bool) {
	if mig == nil || mig.DB == nil {
		return
	}
	mig.DB.SetDisableQueryTimeout(disable)
}

// ErrSearchRequiresFilter is returned when search/count is called with no narrowing predicates.
var ErrSearchRequiresFilter = errors.New("search requires at least one filter")

// SearchRequestHasFilter reports whether req carries any narrowing search predicate.
// Mirrors Migration-Engine ReviewFilterHasSearchPredicate after condition mapping:
// statusSearchType / sort alone do not count; IncludeDestinationOnly=false does.
func SearchRequestHasFilter(req corebridge.SearchRequest) bool {
	if under := strings.TrimSpace(req.UnderPath); under != "" && under != "/" {
		return true
	}
	if req.Ruleset != nil && len(req.Ruleset.RootGroup.Children) > 0 {
		return true
	}
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

// diffListResponse builds the paginated API response from engine diff items.
func diffListResponse(items []migration.DiffItem, offset, limit int, total *int, hasMore bool) corebridge.ListChildrenDiffsResponse {
	out := make(map[string]corebridge.PathNodes, len(items))
	order := make([]string, 0, len(items))
	for _, item := range items {
		out[item.Path] = diffItemToPathNodes(item)
		order = append(order, item.Path)
	}
	if total != nil {
		hasMore = offset+limit < *total
	}
	return corebridge.ListChildrenDiffsResponse{
		Items:     out,
		ItemOrder: order,
		Pagination: corebridge.PaginationInfo{
			Offset:  offset,
			Limit:   limit,
			Total:   total,
			HasMore: hasMore,
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
	displayPath := strings.TrimSpace(item.DisplayPath)
	dstDisplay := strings.TrimSpace(item.DstDisplayPath)
	if dstDisplay == "" {
		dstDisplay = displayPath
		if pathNodes.ResolvedDstName != "" && displayPath != "" {
			// Fallback: swap leaf to resolved DST basename when DST compose is unavailable.
			dstDisplay = joinParentPath(displayPath, pathNodes.ResolvedDstName)
		}
	}
	if !item.MissingOnSource {
		pathNodes.Src = &corebridge.PathNodeItem{
			Queue:           "SRC",
			Id:              item.SrcNodeID,
			Name:            item.Name,
			LocationPath:    item.Path,
			DisplayPath:     displayPath,
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
		if pathNodes.ResolvedDstName != "" {
			dstName = pathNodes.ResolvedDstName
		}
		pathNodes.Dst = &corebridge.PathNodeItem{
			Queue:           "DST",
			Id:              item.DstNodeID,
			Name:            dstName,
			LocationPath:    item.Path, // id_path for parent_path nav (same chain as SRC when mapped)
			DisplayPath:     dstDisplay,
			DepthLevel:      item.Depth,
			Type:            item.Type,
			Size:            dstReviewSize(item),
			TraversalStatus: item.DstTraversalStatus,
			FailureLogID:    item.DstFailureLogID,
			FailureMessage:  item.DstFailureMessage,
		}
	}
	return pathNodes
}

func dstReviewSize(item migration.DiffItem) int64 {
	if item.HasDstSize {
		return item.DstSize
	}
	return item.Size
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
		AfterPath:              req.AfterPath,
	})
	if err != nil {
		return corebridge.ListChildrenDiffsResponse{}, fmt.Errorf("failed to list children diffs: %w", err)
	}
	resp := diffListResponse(result.Items, result.Offset, result.Limit, result.Total, result.HasMore)
	if enrichErr := EnrichPathNodesWithRuleExclusions(mig, &resp); enrichErr != nil {
		return corebridge.ListChildrenDiffsResponse{}, fmt.Errorf("failed to enrich rule exclusions: %w", enrichErr)
	}
	return resp, nil
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
		UnderPath:              strings.TrimSpace(req.UnderPath),
		Limit:                  limit,
		Offset:                 offset,
		SortBy:                 sortBy,
		SortDirection:          sortDirection,
		Conditions:             enginePathReviewConditions(req),
		StatusSearchType:       req.StatusSearchType,
		IncludeDestinationOnly: req.IncludeDestinationOnly,
		Ruleset:                req.Ruleset,
		AfterPath:              strings.TrimSpace(req.AfterPath),
		AfterID:                strings.TrimSpace(req.AfterID),
	}
}

// SearchPathReviewItems calls the engine and converts to API response.
// Caller should reject zero-filter requests via SearchRequestHasFilter before calling.
func SearchPathReviewItems(ctx context.Context, mig *migration.Migration, req corebridge.SearchRequest, offset, limit int) (corebridge.ListChildrenDiffsResponse, error) {
	if mig == nil {
		return corebridge.ListChildrenDiffsResponse{}, fmt.Errorf("migration is nil")
	}
	if !SearchRequestHasFilter(req) {
		return corebridge.ListChildrenDiffsResponse{}, ErrSearchRequiresFilter
	}
	result, err := mig.SearchPathReviewItems(ctx, engineSearchRequest(req, offset, limit))
	if err != nil {
		if errors.Is(err, migration.ErrSearchRequiresFilter) {
			return corebridge.ListChildrenDiffsResponse{}, ErrSearchRequiresFilter
		}
		return corebridge.ListChildrenDiffsResponse{}, fmt.Errorf("failed to search path review items: %w", err)
	}
	resp := searchListResponse(result)
	if enrichErr := EnrichPathNodesWithRuleExclusions(mig, &resp); enrichErr != nil {
		return corebridge.ListChildrenDiffsResponse{}, fmt.Errorf("failed to enrich rule exclusions: %w", enrichErr)
	}
	return resp, nil
}

// GetSearchStats calls the engine and converts to API response (total/folder/file counts; Truncated when deadline cut short).
// Caller should reject zero-filter requests via SearchRequestHasFilter before calling.
func GetSearchStats(ctx context.Context, mig *migration.Migration, req corebridge.SearchRequest) (corebridge.DiffsStatsResponse, error) {
	if mig == nil {
		return corebridge.DiffsStatsResponse{}, fmt.Errorf("migration is nil")
	}
	if !SearchRequestHasFilter(req) {
		return corebridge.DiffsStatsResponse{}, ErrSearchRequiresFilter
	}
	stats, err := mig.GetSearchStats(ctx, engineSearchRequest(req, 0, 10000))
	if err != nil {
		return corebridge.DiffsStatsResponse{}, fmt.Errorf("failed to get search stats: %w", err)
	}
	return corebridge.DiffsStatsResponse{
		Total:        stats.Total,
		TotalFolders: stats.Folders,
		TotalFiles:   stats.Files,
		Truncated:    stats.Truncated,
	}, nil
}

// ReviewOpsStatus reports in-flight path-review writers for UI staleness banners.
func ReviewOpsStatus(mig *migration.Migration) corebridge.ReviewOpsResponse {
	if mig == nil {
		return corebridge.ReviewOpsResponse{}
	}
	return corebridge.ReviewOpsResponse{
		BulkMutationInProgress: mig.BulkMutationInProgress(),
	}
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
	itemsPerSec := convert.ToNumber[float64](q["items_per_second"])
	if itemsPerSec == 0 {
		itemsPerSec = convert.ToNumber[float64](q["discovery_rate_items_per_sec"])
	}
	m := &corebridge.ExternalQueueMetrics{
		FilesDiscoveredTotal:     convert.ToNumber[int64](q["files_discovered_total"]),
		FoldersDiscoveredTotal:   convert.ToNumber[int64](q["folders_discovered_total"]),
		DiscoveryRateItemsPerSec: convert.ToNumber[float64](q["discovery_rate_items_per_sec"]),
		TotalDiscovered:          convert.ToNumber[int64](q["total_discovered"]),
		ItemsPerSecond:           itemsPerSec,
		BytesPerSecond:           convert.ToNumber[float64](q["bytes_per_second"]),
	}
	commonQueueState(m, name, q)
	return m
}

func phaseQueueMetrics(name string, q map[string]any) *corebridge.ExternalQueueMetrics {
	m := &corebridge.ExternalQueueMetrics{
		Folders:                   convert.ToNumber[int64](q["folders"]),
		Files:                     convert.ToNumber[int64](q["files"]),
		Total:                     convert.ToNumber[int64](q["total"]),
		Bytes:                     convert.ToNumber[int64](q["bytes"]),
		ItemsPerSecond:            convert.ToNumber[float64](q["items_per_second"]),
		BytesPerSecond:            convert.ToNumber[float64](q["bytes_per_second"]),
		FoldersAlreadyExists:      convert.ToNumber[int64](q["folders_already_exists"]),
		FilesAlreadyExists:        convert.ToNumber[int64](q["files_already_exists"]),
		BytesAlreadyExists:        convert.ToNumber[int64](q["bytes_already_exists"]),
		FoldersFailed:             convert.ToNumber[int64](q["folders_failed"]),
		FilesFailed:               convert.ToNumber[int64](q["files_failed"]),
		FoldersExpected:           convert.ToNumber[int64](q["folders_expected"]),
		FilesExpected:             convert.ToNumber[int64](q["files_expected"]),
		TotalExpected:             convert.ToNumber[int64](q["total_expected"]),
		ItemsCompleted:            convert.ToNumber[int64](q["items_completed"]),
		ItemsTotal:                convert.ToNumber[int64](q["items_total"]),
		ItemsProgressPercent:      convert.ToNumber[float64](q["items_progress_percent"]),
		ItemsOkPercent:            convert.ToNumber[float64](q["items_ok_percent"]),
		ItemsAlreadyExistsPercent: convert.ToNumber[float64](q["items_already_exists_percent"]),
		ItemsFailedPercent:        convert.ToNumber[float64](q["items_failed_percent"]),
		BytesTotal:                convert.ToNumber[int64](q["bytes_total"]),
		BytesFailed:               convert.ToNumber[int64](q["bytes_failed"]),
		BytesProgressPercent:      convert.ToNumber[float64](q["bytes_progress_percent"]),
		BytesOkPercent:            convert.ToNumber[float64](q["bytes_ok_percent"]),
		BytesAlreadyExistsPercent: convert.ToNumber[float64](q["bytes_already_exists_percent"]),
		BytesFailedPercent:        convert.ToNumber[float64](q["bytes_failed_percent"]),
		ProgressPercent:           convert.ToNumber[float64](q["progress_percent"]),
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
	}
	if fold := sizeFoldMetrics(mig); fold != nil {
		resp.SizeFold = fold
	}
	return resp, nil
}

func sizeFoldMetrics(mig *migration.Migration) *corebridge.ExternalQueueMetrics {
	if mig == nil || mig.DB == nil {
		return nil
	}
	phase := ""
	switch mig.Phase() {
	case migration.PhaseTraversalFinalizing, migration.PhaseTraversalFinalizeFailed:
		phase = "trav"
	case migration.PhaseCopyFinalizing, migration.PhaseCopyFinalizeFailed:
		phase = "copy"
	default:
		return nil
	}
	raw, err := stats.GetLatestQueueStats(mig.DB, "size-fold", phase)
	if err != nil || len(raw) == 0 {
		return nil
	}
	var q map[string]any
	if err := json.Unmarshal(raw, &q); err != nil {
		return nil
	}
	m := phaseQueueMetrics("size-fold", q)
	m.ItemsPerSecond = convert.ToNumber[float64](q["folders_per_sec"])
	if m.ItemsPerSecond == 0 {
		m.ItemsPerSecond = convert.ToNumber[float64](q["items_per_second"])
	}
	return m
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

// excludeOperationsLocked is true while copy/delete workers are running; review-phase exclude is allowed.
func excludeOperationsLocked(phase string) bool {
	switch phase {
	case migration.PhaseCopying, migration.PhaseCopySuspended,
		migration.PhaseDeleting, migration.PhaseDeleteSuspended:
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
	if excludeOperationsLocked(mig.Phase()) {
		resp, err := exclusionFromOutcome(pathReviewOutcome{
			errMsg: "exclusion operations are not available while copy or delete is in progress",
			deltas: emptyDeltas(),
		}), fmt.Errorf("exclusion operations are locked during active copy or delete")
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
	merged, items, err := runPathReviewBatch(req.NodeIDs, func(nodeID string) (migration.PathReviewActionResult, error) {
		// Exclude/unexclude is SRC-only; a second DST call would re-mutate the same SRC id.
		return mig.SetNodeExcludedWithPropagation("SRC", nodeID, excluded)
	}, excludeNoopReason(excluded))
	if err != nil {
		return exclusionFromOutcome(outcomeFromSingle(migration.PathReviewActionResult{}, err)), err
	}
	resp := exclusionFromOutcome(outcomeFromItems(merged, items))
	return resp, nil
}

func excludeNoopReason(excluded bool) string {
	if excluded {
		return "already excluded or not pending"
	}
	return "already included"
}

func ptrBool(v bool) *bool { return &v }

// ExcludeBySearch applies path-review search criteria as a bulk exclusion over
// discovered SRC nodes. Flat conditions and optional ruleset are ANDed the same
// way as POST .../search.
func ExcludeBySearch(mig *migration.Migration, req corebridge.SearchRequest, except []string) (*corebridge.ExclusionResponse, error) {
	if mig == nil {
		return exclusionFromOutcome(nilMigrationOutcome()), nil
	}
	if excludeOperationsLocked(mig.Phase()) {
		resp, err := exclusionFromOutcome(pathReviewOutcome{
			errMsg: "exclusion operations are not available while copy or delete is in progress",
			deltas: emptyDeltas(),
		}), fmt.Errorf("exclusion operations are locked during active copy or delete")
		return resp, err
	}
	appID := uuid.New().String()
	engineReq := engineSearchRequest(req, 0, 0)
	res, err := mig.ApplySearchExclusion(engineReq, except, appID)
	resp := exclusionFromOutcome(outcomeFromSingle(res, err))
	return resp, err
}

// UnexcludeBySearch restores pending for excluded SRC nodes matching search criteria.
func UnexcludeBySearch(mig *migration.Migration, req corebridge.SearchRequest, except []string) (*corebridge.ExclusionResponse, error) {
	if mig == nil {
		return exclusionFromOutcome(nilMigrationOutcome()), nil
	}
	if excludeOperationsLocked(mig.Phase()) {
		resp, err := exclusionFromOutcome(pathReviewOutcome{
			errMsg: "exclusion operations are not available while copy or delete is in progress",
			deltas: emptyDeltas(),
		}), fmt.Errorf("exclusion operations are locked during active copy or delete")
		return resp, err
	}
	engineReq := engineSearchRequest(req, 0, 0)
	res, err := mig.ApplySearchUnexclusion(engineReq, except)
	resp := exclusionFromOutcome(outcomeFromSingle(res, err))
	return resp, err
}

// MarkNodesForRetry calls the engine for discovery or copy retry marking.
// Caller should call MarkPathReviewChanges after success.
func MarkNodesForRetry(mig *migration.Migration, kind RetryKind, req corebridge.MarkRetryRequest) (*corebridge.MarkRetryResponse, error) {
	if mig == nil {
		return markRetryFromOutcome(nilMigrationOutcome()), nil
	}
	if req.MarkAsFailed {
		return UnmarkNodesForRetry(mig, kind, req)
	}
	merged, items, err := runPathReviewBatch(req.NodeIDs, func(nodeID string) (migration.PathReviewActionResult, error) {
		return retryNode(mig, kind, nodeID, true)
	}, "not eligible for retry")
	if err != nil {
		return markRetryFromOutcome(outcomeFromSingle(migration.PathReviewActionResult{}, err)), err
	}
	resp := markRetryFromOutcome(outcomeFromItems(merged, items))
	return resp, nil
}

// UnmarkNodesForRetry clears retry (marks failed) for many nodes.
func UnmarkNodesForRetry(mig *migration.Migration, kind RetryKind, req corebridge.MarkRetryRequest) (*corebridge.MarkRetryResponse, error) {
	if mig == nil {
		return markRetryFromOutcome(nilMigrationOutcome()), nil
	}
	merged, items, err := runPathReviewBatch(req.NodeIDs, func(nodeID string) (migration.PathReviewActionResult, error) {
		return retryNode(mig, kind, nodeID, false)
	}, "not eligible to mark failed")
	if err != nil {
		return markRetryFromOutcome(outcomeFromSingle(migration.PathReviewActionResult{}, err)), err
	}
	resp := markRetryFromOutcome(outcomeFromItems(merged, items))
	return resp, nil
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
