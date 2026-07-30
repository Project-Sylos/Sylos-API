package corebridge

import "fmt"

// ListChildrenDiffsRequest represents a request to list children diffs from migration database
type ListChildrenDiffsRequest struct {
	MigrationID string
	Path        string      // Optional, defaults to "/"
	Offset      int         // Pagination offset (default: 0); ignored when using keyset (AfterPath set)
	Limit       int         // Pagination limit (default: 100, max: 1000)
	AfterPath   string      // Keyset cursor: return children with path > AfterPath (empty = first page)
	FoldersOnly bool        // If true, only return folders and apply limit to folders only
	Sort        *SortOption `json:"sort,omitempty"` // Sort options (field and direction); keyset uses path order
	// IncludeDestinationOnly when false hides destination-only rows. Nil means include (legacy default).
	IncludeDestinationOnly *bool `json:"includeDestinationOnly,omitempty"`
}

// PathNodeItem represents a single node (from either SRC or DST) with its metadata
type PathNodeItem struct {
	Queue           string `json:"queue"` // "SRC" or "DST"
	Id              string `json:"id"`
	ParentId        string `json:"parentId,omitempty"`
	ParentPath      string `json:"parentPath,omitempty"`
	Name            string `json:"name"`
	LocationPath    string `json:"locationPath"`
	LastUpdated     string `json:"lastUpdated,omitempty"`
	DepthLevel      int    `json:"depthLevel"`
	Type            string `json:"type"`
	Size            int64  `json:"size,omitempty"`
	TraversalStatus string `json:"traversalStatus"`
	CopyStatus      string `json:"copyStatus,omitempty"`
	DeleteStatus    string `json:"deleteStatus,omitempty"`
	FailureLogID    string `json:"failureLogId,omitempty"`
	FailureMessage  string `json:"failureMessage,omitempty"`
}

// PathNodes represents the src and dst nodes for a given path
type PathNodes struct {
	Src *PathNodeItem `json:"src,omitempty"`
	Dst *PathNodeItem `json:"dst,omitempty"`
	// ResolvedDstName is the accepted/committed destination basename when it differs from the source name.
	ResolvedDstName string `json:"resolvedDstName,omitempty"`
}

type FileSizeStats struct {
	Src      int64 `json:"src"`
	Dst      int64 `json:"dst"`
	Selected int64 `json:"selected"`
}

// PrepareSourceCleanupRequest selects SRC nodes to remove from source after copy.
// Pass nodeIds to keep pending, or deselectedNodeIds to skip those (default: all successful copies are selected).
type PrepareSourceCleanupRequest struct {
	NodeIDs           []string `json:"nodeIds,omitempty"`
	DeselectedNodeIDs []string `json:"deselectedNodeIds,omitempty"`
}

// PathReviewStats is the phase-aware path review stats from the engine (GetPathReviewStatsForView). Returned as JSON for GET /migrations/{id}/stats.
type PathReviewStats struct {
	PendingCount        int           `json:"pendingCount"`
	FailedCount         int           `json:"failedCount"`
	ExcludedCount       int           `json:"excludedCount"`
	PendingRetriesCount int           `json:"pendingRetriesCount"`
	SuccessfulCount     int           `json:"successfulCount"`
	FoldersCount        int           `json:"foldersCount"`
	FilesCount          int           `json:"filesCount"`
	FoldersRatio        float64       `json:"foldersRatio"`
	FilesRatio          float64       `json:"filesRatio"`
	TotalFileSize       FileSizeStats `json:"totalFileSize"`
}

// ListChildrenDiffsResponse wraps the diff result with pagination metadata
type ListChildrenDiffsResponse struct {
	Items      map[string]PathNodes `json:"items"`               // path -> {src?: {...}, dst?: {...}}
	ItemOrder  []string             `json:"itemOrder,omitempty"` // paths in engine/SQL order; use when present — JSON object keys are sorted by path, not by sort
	Pagination PaginationInfo       `json:"pagination"`
}

// DiffsStatsResponse is returned by the separate diffs stats endpoint (total and folder/file counts for a path).
type DiffsStatsResponse struct {
	Total        int `json:"total"`
	TotalFolders int `json:"totalFolders"`
	TotalFiles   int `json:"totalFiles"`
}

// ExclusionRequest represents a request to exclude/unexclude nodes
type ExclusionRequest struct {
	NodeIDs []string `json:"nodeIDs,omitempty"` // Array of node IDs to exclude/unexclude
	All     bool     `json:"all,omitempty"`     // If true, mark all matching items
	Filter  *struct {
		Status string `json:"status,omitempty"` // Optional status filter (e.g., "failed")
	} `json:"filter,omitempty"`
}

// ExclusionResponse represents the response from exclude/unexclude operations.
// AffectedCount and Deltas come from the engine's PathReviewActionResult so the UI can update local stats without refetching.
// Delta keys (traversal only): traversalPending, traversalFailed, excluded. Apply to the phase's pending/failed/excluded counts.
type ExclusionResponse struct {
	Success       bool             `json:"success"`
	Error         string           `json:"error,omitempty"`
	TaskID        string           `json:"taskID,omitempty"` // Background task ID for 'all' operations
	AffectedCount int64            `json:"affectedCount"`
	Deltas        map[string]int64 `json:"deltas"` // Engine keys: traversalPending, traversalFailed, excluded; only keys that changed are present
}

// SweepConfigRequest represents the configuration for exclusion or retry sweeps
type SweepConfigRequest struct {
	WorkerCount        int    `json:"workerCount,omitempty"`
	MaxRetries         int    `json:"maxRetries,omitempty"`
	MaxKnownDepth      int    `json:"maxKnownDepth,omitempty"`
	LogAddress         string `json:"logAddress,omitempty"`
	LogLevel           string `json:"logLevel,omitempty"`
	SkipListener       *bool  `json:"skipListener,omitempty"`
	StartupDelaySec    int    `json:"startupDelaySeconds,omitempty"`
	ProgressTickMillis int    `json:"progressTickMillis,omitempty"`
}

// SweepResponse represents the response from triggering a sweep
type SweepResponse struct {
	Success        bool   `json:"success"`
	Message        string `json:"message,omitempty"`
	Error          string `json:"error,omitempty"`
	AlreadyRunning bool   `json:"alreadyRunning,omitempty"`
}

// PendingWorkResponse represents the response for checking pending work
type PendingWorkResponse struct {
	HasPendingRetries    bool `json:"hasPendingRetries"`    // True if count > 0
	HasPathReviewChanges bool `json:"hasPathReviewChanges"` // True if user made changes (exclusions/retries) since last sweep completion
	PendingRetriesCount  int  `json:"pendingRetriesCount"`  // Number of items marked as "pending" in status-lookup buckets (actual retry count)
}

// MarkRetryRequest represents a request to mark nodes for retry
type MarkRetryRequest struct {
	NodeIDs      []string `json:"nodeIDs,omitempty"`      // Array of node IDs to mark for retry
	All          bool     `json:"all,omitempty"`          // If true, mark all failed items
	MarkAsFailed bool     `json:"markAsFailed,omitempty"` // If true, mark nodes as failed instead of retry
}

// MarkRetryResponse represents the response from marking/unmarking a node for retry.
// AffectedCount and Deltas come from the engine's PathReviewActionResult so the UI can update local stats without refetching.
// Delta keys depend on action:
//   - Discovery retry (mark/unmark for retry discovery): traversalPending, traversalFailed, pendingRetries; plus folders, files, excluded, sizeDst when DST descendants removed.
//   - Copy retry (mark/unmark for retry copy): copyPending, copyFailed.
//   - Retry all failed: traversalFailed, traversalPending.
//
// UI should apply traversal keys to traversal review counters and copy keys to copy review counters.
type MarkRetryResponse struct {
	Success       bool             `json:"success"`
	Error         string           `json:"error,omitempty"`
	TaskID        string           `json:"taskID,omitempty"` // Background task ID for 'all' operations
	AffectedCount int64            `json:"affectedCount"`
	Deltas        map[string]int64 `json:"deltas"` // Engine keys above; only keys that changed are present
}

// SearchCondition represents a single search condition
type SearchCondition struct {
	Field    string `json:"field"`              // Field to search: "name", "path", "type", "depth", "size", "traversalStatus", "copyStatus"
	Operator string `json:"operator,omitempty"` // Operator: "equals", "contains", "gt", "gte", "lt", "lte" (ignored for "name" and "path" - always uses contains)
	Value    any    `json:"value"`              // Value to compare against
}

// SearchRequest represents a request to search path review items.
// At least one narrowing filter is required (see migrationops.SearchRequestHasFilter); empty global search is rejected.
type SearchRequest struct {
	Conditions       []SearchCondition `json:"conditions,omitempty"`       // Search conditions
	Sort             *SortOption       `json:"sort,omitempty"`             // Sort options (field and direction)
	StatusSearchType string            `json:"statusSearchType,omitempty"` // Which status type(s) to search by: "traversal", "copy", or "both" (default: "both")
	// IncludeDestinationOnly when false hides destination-only rows. Nil means include (legacy default).
	// Explicitly false counts as a search filter (matches engine ExcludeDestinationOnly).
	IncludeDestinationOnly *bool `json:"includeDestinationOnly,omitempty"`
}

// SortOption represents sorting options for search results
type SortOption struct {
	Field     string `json:"field"`               // Field to sort by: "name", "path", "depth", "size", "type", "traversalStatus", etc.
	Direction string `json:"direction,omitempty"` // Sort direction: "asc" or "desc" (default: "asc")
}

// Path issue API error codes (user-facing titles elsewhere; codes stay stable for clients).
const (
	ErrCodePathIssuesRemaining = "PATH_ISSUES_REMAINING"
	ErrCodePathValidation      = "PATH_VALIDATION_FAILED"
)

// PathIssue is one destination-name finding for review.
type PathIssue struct {
	NodeID       string             `json:"nodeId"`
	Path         string             `json:"path"`
	Name         string             `json:"name"`
	ProposedPath string             `json:"proposedPath"`
	Status       string             `json:"status"`
	Category     string             `json:"category,omitempty"`
	Summary      string             `json:"summary,omitempty"`
	EventTime    int64              `json:"eventTime,omitempty"`
	Messages     []PathIssueMessage `json:"messages,omitempty"`
	// Ignored is true when the warning is dismissed (original name kept).
	Ignored bool `json:"ignored,omitempty"`
}

// PathIssueMessage is a short user-facing validation finding.
type PathIssueMessage struct {
	Category string `json:"category,omitempty"`
	Message  string `json:"message"`
	// Detail is a GPL structured attribute (e.g. InvalidChar: forbidden runes found).
	Detail  string `json:"detail,omitempty"`
	DocsURL string `json:"docsURL,omitempty"`
}

// ValidatePathProposalRequest is the body for dry-run destination name checks.
type ValidatePathProposalRequest struct {
	NodeID       string `json:"nodeId"`
	ProposedPath string `json:"proposedPath"`
}

// ValidatePathProposalResponse is the dry-run result with friendly messages.
type ValidatePathProposalResponse struct {
	Valid             bool               `json:"valid"`
	Messages          []PathIssueMessage `json:"messages,omitempty"`
	PathChecksEnabled bool               `json:"pathChecksEnabled"`
}

// RemapPathRequest is the body for manual destination rename.
type RemapPathRequest struct {
	ProposedPath        string `json:"proposedPath"`
	ForceSkipValidation bool   `json:"forceSkipValidation,omitempty"`
}

// AcceptPathRequest is the body for accepting a suggested destination name.
type AcceptPathRequest struct {
	ProposedPath string `json:"proposedPath,omitempty"`
}

// PathIssuesListResponse is GET /path-issues.
type PathIssuesListResponse struct {
	Issues            []PathIssue `json:"issues"`
	Count             int         `json:"count"`
	PathChecksEnabled bool        `json:"pathChecksEnabled"`
}

// PathIssuesMutationResponse is returned after accept / remap / accept-all / ignore.
type PathIssuesMutationResponse struct {
	Success           bool        `json:"success"`
	Accepted          int         `json:"accepted,omitempty"`
	Ignored           int         `json:"ignored,omitempty"`
	Issues            []PathIssue `json:"issues,omitempty"`
	Count             int         `json:"count"`
	Message           string      `json:"message,omitempty"`
	PathChecksEnabled bool        `json:"pathChecksEnabled"`
}

// PathIssuesRemainingError is returned when Start Copy is blocked by active path issues.
type PathIssuesRemainingError struct {
	Count  int
	Issues []PathIssue
}

func (e *PathIssuesRemainingError) Error() string {
	if e == nil {
		return "some destination names still need attention"
	}
	return fmt.Sprintf("some destination names still need attention (%d remaining)", e.Count)
}
