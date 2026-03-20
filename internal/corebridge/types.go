package corebridge

import (
	"context"
	"errors"
	"time"

	"codeberg.org/Sylos/Migration-Engine/pkg/migration"
	fstypes "codeberg.org/Sylos/Sylos-FS/pkg/types"
)

var (
	ErrMigrationNotFound    = errors.New("migration not found")
	ErrServiceNotFound      = errors.New("service not found")
	ErrDatabaseNotAvailable = errors.New("database not available")
)

type ServiceType string

const (
	ServiceTypeLocal   ServiceType = "local"
	ServiceTypeSpectra ServiceType = "spectra"
)

const (
	RootRoleSource      = "source"
	RootRoleDestination = "destination"
)

type Source struct {
	ID          string            `json:"id"`
	DisplayName string            `json:"displayName"`
	Type        ServiceType       `json:"type"`
	Metadata    map[string]string `json:"metadata,omitempty"`
}

type ListChildrenRequest struct {
	ServiceID   string
	Identifier  string
	Role        string // "source" or "destination" - used to map "spectra" to the correct world
	Offset      int    // Pagination offset (default: 0)
	Limit       int    // Pagination limit (default: 100, max: 1000)
	FoldersOnly bool   // If true, only return folders and apply limit to folders only
}

// ListChildrenResponse wraps the list result with pagination metadata
type ListChildrenResponse struct {
	Folders    []fstypes.Folder `json:"folders"`
	Files      []fstypes.File   `json:"files"`
	Pagination PaginationInfo   `json:"pagination"`
}

// PaginationInfo provides pagination metadata
type PaginationInfo struct {
	Offset       int    `json:"offset"`               // Current offset
	Limit        int    `json:"limit"`                // Current limit
	Total        int    `json:"total"`                // Total number of items (folders + files, or just folders if foldersOnly=true)
	TotalFolders int    `json:"totalFolders"`         // Total number of folders
	TotalFiles   int    `json:"totalFiles"`           // Total number of files
	HasMore      bool   `json:"hasMore"`              // Whether there are more items beyond the current page
	NextCursor   string `json:"nextCursor,omitempty"` // Keyset cursor for next page (path of last item; use as afterPath)
}

// DriveInfo represents information about a drive/volume
type DriveInfo struct {
	Path        string `json:"path"`        // Absolute path to the drive (e.g., "C:\" on Windows, "/" on Unix)
	DisplayName string `json:"displayName"` // Display name (e.g., "C:" or "Local Disk (C:)")
	Type        string `json:"type"`        // Drive type (e.g., "fixed", "removable", "network")
}

type FolderDescriptor struct {
	ID           string `json:"id"`
	ParentID     string `json:"parentId,omitempty"`
	ParentPath   string `json:"parentPath,omitempty"`
	DisplayName  string `json:"displayName,omitempty"`
	LocationPath string `json:"locationPath,omitempty"`
	LastUpdated  string `json:"lastUpdated,omitempty"`
	DepthLevel   int    `json:"depthLevel,omitempty"`
	Type         string `json:"type,omitempty"`
}

type ServiceSelection struct {
	ServiceID    string           `json:"serviceId"`
	ConnectionID string           `json:"connectionId,omitempty"`
	Root         FolderDescriptor `json:"root"`
}

type VerificationOptions struct {
	AllowPending  bool `json:"allowPending"`
	AllowNotOnSrc bool `json:"allowNotOnSrc"`
}

type MigrationOptions struct {
	MigrationID             string              `json:"migrationId,omitempty"`
	DatabasePath            string              `json:"databasePath,omitempty"`
	RemoveExistingDB        bool                `json:"removeExistingDatabase,omitempty"`
	UsePreseededDB          bool                `json:"usePreseededDatabase,omitempty"`
	SourceConnectionID      string              `json:"sourceConnectionId,omitempty"`
	DestinationConnectionID string              `json:"destinationConnectionId,omitempty"`
	WorkerCount             int                 `json:"workerCount,omitempty"`
	MaxRetries              int                 `json:"maxRetries,omitempty"`
	CoordinatorLead         int                 `json:"coordinatorLead,omitempty"`
	LogAddress              string              `json:"logAddress,omitempty"`
	LogLevel                string              `json:"logLevel,omitempty"`
	SkipListener            *bool               `json:"skipListener,omitempty"` // If nil, defaults to true (skip listener)
	StartupDelaySec         int                 `json:"startupDelaySeconds,omitempty"`
	ProgressTickMillis      int                 `json:"progressTickMillis,omitempty"`
	Verification            VerificationOptions `json:"verification,omitempty"`
}

type StartMigrationRequest struct {
	MigrationID string           `json:"migrationId,omitempty"`
	Options     MigrationOptions `json:"options"`
}

type SetRootRequest struct {
	MigrationID  string           `json:"migrationId,omitempty"`
	Role         string           `json:"role"`
	ServiceID    string           `json:"serviceId"`
	ConnectionID string           `json:"connectionId,omitempty"`
	Root         FolderDescriptor `json:"root"`
	Config       map[string]any   `json:"config,omitempty"` // Optional service-specific config (e.g., Spectra config JSON)
}

type SetRootResponse struct {
	MigrationID             string                     `json:"migrationId"`
	Role                    string                     `json:"role"`
	Ready                   bool                       `json:"ready"`
	DatabasePath            string                     `json:"databasePath,omitempty"`
	RootSummary             *migration.RootSeedSummary `json:"rootSummary,omitempty"`
	SourceConnectionID      string                     `json:"sourceConnectionId,omitempty"`
	DestinationConnectionID string                     `json:"destinationConnectionId,omitempty"`
}

type Migration struct {
	ID            string    `json:"id"`
	SourceID      string    `json:"sourceId"`
	DestinationID string    `json:"destinationId"`
	StartedAt     time.Time `json:"startedAt"`
	Status        string    `json:"status"`
	Success       bool      `json:"success,omitempty"` // Indicates if operation succeeded
}

type Status struct {
	Migration
	CompletedAt *time.Time  `json:"completedAt,omitempty"`
	Error       string      `json:"error,omitempty"`
	Result      *ResultView `json:"result,omitempty"`
	// Status field in Migration now represents the checkpoint state (e.g., "Awaiting-Path-Review", "Traversal-In-Progress", "Copy-In-Progress")
}

type ResultView struct {
	RootSummary  RootSummaryView  `json:"rootSummary"`
	Runtime      RuntimeStatsView `json:"runtime"`
	Verification VerificationView `json:"verification"`
}

type RootSummaryView struct {
	SrcRoots int `json:"srcRoots"`
	DstRoots int `json:"dstRoots"`
}

type QueueStatsView struct {
	Name         string `json:"name"`
	Round        int    `json:"round"`
	Pending      int    `json:"pending"`
	InProgress   int    `json:"inProgress"`
	TotalTracked int    `json:"totalTracked"`
	Workers      int    `json:"workers"`
}

type RuntimeStatsView struct {
	Duration string         `json:"duration"`
	Src      QueueStatsView `json:"src"`
	Dst      QueueStatsView `json:"dst"`
}

type VerificationView struct {
	SrcTotal    int `json:"srcTotal"`
	DstTotal    int `json:"dstTotal"`
	SrcPending  int `json:"srcPending"`
	DstPending  int `json:"dstPending"`
	SrcFailed   int `json:"srcFailed"`
	DstFailed   int `json:"dstFailed"`
	DstNotOnSrc int `json:"dstNotOnSrc"`
}

type QueueStatsSnapshot struct {
	Round        int `json:"round"`
	Pending      int `json:"pending"`
	InProgress   int `json:"inProgress"`
	TotalTracked int `json:"totalTracked"`
	Workers      int `json:"workers"`
}

// QueueStats represents basic queue statistics
type QueueStats struct {
	Name         string `json:"name"`
	Round        int    `json:"round"`
	Pending      int    `json:"pending"`
	InProgress   int    `json:"inProgress"`
	TotalTracked int    `json:"totalTracked"`
	Workers      int    `json:"workers"`
}

// ExternalQueueMetrics contains user-facing metrics published to BoltDB for API access.
// This struct supports both traversal metrics and copy phase metrics.
// Fields will be populated based on which phase is active.
type ExternalQueueMetrics struct {
	// Traversal phase metrics
	FilesDiscoveredTotal     int64   `json:"files_discovered_total,omitempty"`
	FoldersDiscoveredTotal   int64   `json:"folders_discovered_total,omitempty"`
	DiscoveryRateItemsPerSec float64 `json:"discovery_rate_items_per_sec,omitempty"`
	TotalDiscovered          int64   `json:"total_discovered,omitempty"` // files + folders

	// Copy phase metrics (new format from engine)
	Folders        int64   `json:"folders,omitempty"`          // Total folders created
	Files          int64   `json:"files,omitempty"`            // Total files created
	Total          int64   `json:"total,omitempty"`            // Total items (folders + files)
	Bytes          int64   `json:"bytes,omitempty"`            // Total bytes transferred
	ItemsPerSecond float64 `json:"items_per_second,omitempty"` // Combined items/sec (EMA-smoothed)
	BytesPerSecond float64 `json:"bytes_per_second,omitempty"` // Bytes/sec transfer rate (EMA-smoothed)

	// Common state fields (used by both phases)
	Round        int    `json:"round"`
	Pending      int    `json:"pending"`
	InProgress   int    `json:"in_progress"`
	Workers      int    `json:"workers"`
	TotalPending int    `json:"total_pending,omitempty"` // Total pending from DB (copy phase)
	TotalFailed  int    `json:"total_failed,omitempty"`  // Total failed from DB (copy phase)
	Name         string `json:"name,omitempty"`          // Queue name ("copy", "src-traversal", etc.)
}

// QueueMetricsResponse represents all queue metrics for a migration
type QueueMetricsResponse struct {
	Success      bool                  `json:"success"`             // Whether the operation succeeded
	ErrorCode    string                `json:"errorCode,omitempty"` // Error code if success is false (e.g., "DATABASE_NOT_AVAILABLE")
	Error        string                `json:"error,omitempty"`     // Human-readable error message if success is false
	SrcTraversal *ExternalQueueMetrics `json:"srcTraversal,omitempty"`
	DstTraversal *ExternalQueueMetrics `json:"dstTraversal,omitempty"`
	Copy         *ExternalQueueMetrics `json:"copy,omitempty"`
}

// LogEntry represents a single log entry from the database
type LogEntry struct {
	ID    string         `json:"id"`
	Level string         `json:"level"`
	Data  map[string]any `json:"data"`
}

// GetLogsRequest represents a request to get logs for a migration
type GetLogsRequest struct {
	// No fields - always returns up to 1K logs per level (newest first)
}

// GetLogsResponse represents the response containing logs for all levels
type GetLogsResponse struct {
	Success   bool                  `json:"success"`             // Whether the operation succeeded
	ErrorCode string                `json:"errorCode,omitempty"` // Error code if success is false (e.g., "DATABASE_NOT_AVAILABLE")
	Error     string                `json:"error,omitempty"`     // Human-readable error message if success is false
	Logs      map[string][]LogEntry `json:"logs,omitempty"`      // Map of log level to array of log entries
}

type ProgressEvent struct {
	Event       string             `json:"event"`
	Timestamp   time.Time          `json:"timestamp"`
	Migration   Status             `json:"migration"`
	Source      QueueStatsSnapshot `json:"source"`
	Destination QueueStatsSnapshot `json:"destination"`
}

type UploadMigrationDBRequest struct {
	Filename  string `json:"filename"`
	Overwrite bool   `json:"overwrite,omitempty"`
}

type UploadMigrationDBResponse struct {
	Success bool   `json:"success"`
	Error   string `json:"error,omitempty"`
	Path    string `json:"path,omitempty"`
}

type MigrationDBInfo struct {
	Filename   string    `json:"filename"`
	Path       string    `json:"path"`
	Size       int64     `json:"size"`
	ModifiedAt time.Time `json:"modifiedAt"`
}

// MigrationMetadata represents minimal metadata for a migration
// The Migration Engine's YAML config file contains all the detailed state
type MigrationMetadata struct {
	ID             string    `json:"id"`
	Name           string    `json:"name"`
	ConfigPath     string    `json:"configPath"`   // Legacy path retained for older metadata files
	DatabasePath   string    `json:"databasePath"` // Canonical DB path in DB-only mode
	CreatedAt      time.Time `json:"createdAt"`
	IsNewMigration bool      `json:"isNewMigration"` // Flag to indicate this is a new migration (not a resume)
}

// ListChildrenDiffsRequest represents a request to list children diffs from migration database
type ListChildrenDiffsRequest struct {
	MigrationID string
	Path        string      // Optional, defaults to "/"
	Offset      int         // Pagination offset (default: 0); ignored when using keyset (AfterPath set)
	Limit       int         // Pagination limit (default: 100, max: 1000)
	AfterPath   string      // Keyset cursor: return children with path > AfterPath (empty = first page)
	FoldersOnly bool        // If true, only return folders and apply limit to folders only
	Sort        *SortOption `json:"sort,omitempty"` // Sort options (field and direction); keyset uses path order
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
}

// PathNodes represents the src and dst nodes for a given path
type PathNodes struct {
	Src *PathNodeItem `json:"src,omitempty"`
	Dst *PathNodeItem `json:"dst,omitempty"`
}

type FileSizeStats struct {
	Src int64 `json:"src"`
	Dst int64 `json:"dst"`
}

// PathReviewStats is the phase-aware path review stats from the engine (GetPathReviewStats). Returned as JSON for GET /migrations/{id}/stats.
type PathReviewStats struct {
	PendingCount        int           `json:"pendingCount"`
	FailedCount         int           `json:"failedCount"`
	ExcludedCount       int           `json:"excludedCount"`
	PendingRetriesCount int           `json:"pendingRetriesCount"`
	FoldersCount        int           `json:"foldersCount"`
	FilesCount          int           `json:"filesCount"`
	FoldersRatio        float64      `json:"foldersRatio"`
	FilesRatio          float64      `json:"filesRatio"`
	TotalFileSize       FileSizeStats `json:"totalFileSize"`
}

// ListChildrenDiffsResponse wraps the diff result with pagination metadata
type ListChildrenDiffsResponse struct {
	Items      map[string]PathNodes `json:"items"` // path -> {src?: {...}, dst?: {...}}
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
	Success       bool            `json:"success"`
	Error         string          `json:"error,omitempty"`
	TaskID        string          `json:"taskID,omitempty"` // Background task ID for 'all' operations
	AffectedCount int64           `json:"affectedCount"`
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
	Success bool   `json:"success"`
	Message string `json:"message,omitempty"`
	Error   string `json:"error,omitempty"`
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
// UI should apply traversal keys to traversal review counters and copy keys to copy review counters.
type MarkRetryResponse struct {
	Success       bool            `json:"success"`
	Error         string          `json:"error,omitempty"`
	TaskID        string          `json:"taskID,omitempty"` // Background task ID for 'all' operations
	AffectedCount int64           `json:"affectedCount"`
	Deltas        map[string]int64 `json:"deltas"` // Engine keys above; only keys that changed are present
}

// SearchCondition represents a single search condition
type SearchCondition struct {
	Field    string `json:"field"`              // Field to search: "name", "path", "type", "depth", "size", "traversalStatus", "copyStatus"
	Operator string `json:"operator,omitempty"` // Operator: "equals", "contains", "gt", "gte", "lt", "lte" (ignored for "name" and "path" - always uses contains)
	Value    any    `json:"value"`              // Value to compare against
}

// SearchRequest represents a request to search path review items
// If Conditions is empty or nil, lists all items (same as diff endpoint with path="/")
type SearchRequest struct {
	Conditions       []SearchCondition `json:"conditions,omitempty"`       // Search conditions
	Sort             *SortOption       `json:"sort,omitempty"`             // Sort options (field and direction)
	StatusSearchType string            `json:"statusSearchType,omitempty"` // Which status type(s) to search by: "traversal", "copy", or "both" (default: "both")
}

// SortOption represents sorting options for search results
type SortOption struct {
	Field     string `json:"field"`               // Field to sort by: "name", "path", "depth", "size", "type", "traversalStatus", etc.
	Direction string `json:"direction,omitempty"` // Sort direction: "asc" or "desc" (default: "asc")
}

// ListMigrationsRequest represents a request to list migrations with pagination
type ListMigrationsRequest struct {
	Offset int `json:"offset,omitempty"` // Pagination offset (default: 0)
	Limit  int `json:"limit,omitempty"`  // Pagination limit (default: 100, max: 1000)
}

// ListMigrationsResponse represents the response containing paginated migrations
type ListMigrationsResponse struct {
	Migrations []Status `json:"migrations"`
	Total      int      `json:"total"`   // Total number of migrations
	Offset     int      `json:"offset"`  // Current offset
	Limit      int      `json:"limit"`   // Current limit
	HasMore    bool     `json:"hasMore"` // True if there are more migrations
}

type Bridge interface {
	ListSources(ctx context.Context) ([]Source, error)
	ListChildren(ctx context.Context, req ListChildrenRequest) (ListChildrenResponse, error)
	ListDrives(ctx context.Context, serviceID string) ([]DriveInfo, error)
	SetRoot(ctx context.Context, req SetRootRequest) (SetRootResponse, error)
	StartMigration(ctx context.Context, req StartMigrationRequest) (Migration, error)
	GetMigrationStatus(ctx context.Context, id string) (Status, error)
	UploadMigrationDB(ctx context.Context, migrationID string, data []byte, overwrite bool) (UploadMigrationDBResponse, error)
	UploadMigrationYAML(ctx context.Context, migrationID string, data []byte, overwrite bool) (UploadMigrationDBResponse, error)
	UploadMigrationData(ctx context.Context, migrationID string, zipData []byte, overwrite bool) (UploadMigrationDBResponse, error)
	UploadByType(ctx context.Context, migrationID, uploadType string, data []byte, overwrite bool) (UploadMigrationDBResponse, error)
	ListMigrationDBs(ctx context.Context) ([]MigrationDBInfo, error)
	SubscribeProgress(ctx context.Context, id string) (<-chan ProgressEvent, func(), error)
	ToggleLogTerminal(ctx context.Context, enable bool, logAddress string) error
	ListAllMigrations(ctx context.Context, req ListMigrationsRequest) (ListMigrationsResponse, error)
	LoadMigration(ctx context.Context, migrationID string) (Migration, error)
	StopMigration(ctx context.Context, migrationID string) (Status, error)
	CheckPendingWork(ctx context.Context, migrationID string) (PendingWorkResponse, error)
	ChangePhase(ctx context.Context, migrationID string, phase string, req StartMigrationRequest) (Migration, error)
	GetBackgroundTasks(ctx context.Context, migrationID string) ([]BackgroundTask, error)
	GetRunningBackgroundTasks(ctx context.Context, migrationID string) ([]BackgroundTask, error)
	GetBackgroundTask(ctx context.Context, migrationID, taskID string) (*BackgroundTask, error)
	TriggerRetrySweep(ctx context.Context, migrationID string, config SweepConfigRequest) (SweepResponse, error)
}

const (
	MigrationStatusRunning   = "running"
	MigrationStatusCompleted = "completed"
	MigrationStatusSuspended = "suspended"
	MigrationStatusFailed    = "failed"
)
