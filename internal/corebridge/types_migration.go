package corebridge

import (
	"time"

	"codeberg.org/Sylos/Migration-Engine/pkg/migration"
)

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
	// PathCheckTarget selects destination-name rules: "none", "auto", or a provider id (e.g. "windows").
	PathCheckTarget string `json:"pathCheckTarget,omitempty"`
	// WindowsCompat enables Windows desktop-sync overlays on soft cloud destinations.
	WindowsCompat bool `json:"windowsCompat,omitempty"`
}

type StartMigrationRequest struct {
	MigrationID string           `json:"migrationId,omitempty"`
	Options     MigrationOptions `json:"options"`
}

// RootChildPlan is an immediate child of the migration root from the root-pick review UI.
type RootChildPlan struct {
	ID       string `json:"id"`
	Name     string `json:"name"`
	Type     string `json:"type"` // folder | file
	Size     int64  `json:"size,omitempty"`
	MTime    string `json:"mtime,omitempty"`
	Excluded bool   `json:"excluded,omitempty"`
	DstOnly  bool   `json:"dstOnly,omitempty"`
}

type SetRootRequest struct {
	MigrationID  string           `json:"migrationId,omitempty"`
	Role         string           `json:"role"`
	ServiceID    string           `json:"serviceId"`
	ConnectionID string           `json:"connectionId,omitempty"`
	Root         FolderDescriptor `json:"root"`
	Config       map[string]any   `json:"config,omitempty"` // Optional service-specific config (e.g., Spectra config JSON)
	// Children are immediate children listed during root-pick review (no DuckDB write until Start).
	Children []RootChildPlan `json:"children,omitempty"`
	// ExcludedIds are source child ids to exclude; ids not in children are ignored.
	ExcludedIds []string `json:"excludedIds,omitempty"`
}

type SetRootResponse struct {
	MigrationID             string                     `json:"migrationId"`
	Role                    string                     `json:"role"`
	Ready                   bool                       `json:"ready"`
	DatabasePath            string                     `json:"databasePath,omitempty"`
	RootSummary             *migration.RootSeedSummary `json:"rootSummary,omitempty"`
	SourceConnectionID      string                     `json:"sourceConnectionId,omitempty"`
	DestinationConnectionID string                     `json:"destinationConnectionId,omitempty"`
	SourceRootPrepared      bool                       `json:"sourceRootPrepared,omitempty"`
	DestinationRootPrepared bool                       `json:"destinationRootPrepared,omitempty"`
}

type Migration struct {
	ID            string    `json:"id"`
	Name          string    `json:"name,omitempty"` // Human-friendly name; defaults to "{source} -> {destination}"
	SourceID      string    `json:"sourceId"`
	DestinationID string    `json:"destinationId"`
	StartedAt     time.Time `json:"startedAt"`
	Status        string    `json:"status"`
	Success       bool      `json:"success,omitempty"` // Indicates if operation succeeded
}

// RootInfo describes a persisted source/destination root so the UI can rehydrate the
// setup screen (service, connection, and the selected folder) without relying on sessionStorage.
type RootInfo struct {
	ServiceID    string `json:"serviceId"`
	ServiceName  string `json:"serviceName,omitempty"`
	ServiceType  string `json:"serviceType,omitempty"`
	ConnectionID string `json:"connectionId,omitempty"`
	Name         string `json:"name,omitempty"`         // Folder display name (e.g. drive label or folder name)
	LocationPath string `json:"locationPath,omitempty"` // Root-relative path ("/" for a drive/service root)
	NativePath   string `json:"nativePath,omitempty"`   // OS-native absolute path/id (e.g. "C:\\Users\\Logan", "/mnt/2tb-ssd")
	Type         string `json:"type,omitempty"`
}

// RenameMigrationRequest sets a migration's display name.
type RenameMigrationRequest struct {
	Name string `json:"name"`
}

type Status struct {
	Migration
	CompletedAt *time.Time  `json:"completedAt,omitempty"`
	Error       string      `json:"error,omitempty"`
	Result      *ResultView `json:"result,omitempty"`
	// SourceRoot/DestinationRoot are the persisted roots (service + folder), used to rehydrate setup.
	SourceRoot      *RootInfo `json:"sourceRoot,omitempty"`
	DestinationRoot *RootInfo `json:"destinationRoot,omitempty"`
	// SourceRootPrepared / DestinationRootPrepared: UI reviewed immediate children (seed at round 1).
	SourceRootPrepared      bool `json:"sourceRootPrepared,omitempty"`
	DestinationRootPrepared bool `json:"destinationRootPrepared,omitempty"`
	// SourceRootChildNames are display names from the source root review (for dest greying).
	SourceRootChildNames []string `json:"sourceRootChildNames,omitempty"`
	// Live is true when the engine migration has an active run (traversal, copy, sweep, etc.).
	Live bool `json:"live,omitempty"`
	// SoftSuspendRequested is true after Stop() accepted a soft suspend; poll until phase is traversal-suspended or copy-suspended and live is false.
	SoftSuspendRequested bool `json:"softSuspendRequested,omitempty"`
	// Stopped is set only by StopMigration: true if the engine considered a run active when Stop() was called.
	Stopped bool `json:"stopped,omitempty"`
	// AlreadyStopped is true when StopMigration was called but the migration was not in a stoppable live phase.
	AlreadyStopped bool `json:"alreadyStopped,omitempty"`
	// PossibleStall is true when a queue watchdog recently detected no progress while the migration was live.
	PossibleStall bool `json:"possibleStall,omitempty"`
	// Status field in Migration is the lifecycle phase (engine lowercase-with-hyphens, e.g. traversal-in-progress, traversal-suspended).
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
	Bytes          int64   `json:"bytes,omitempty"`            // Bytes transferred (done)
	ItemsPerSecond float64 `json:"items_per_second,omitempty"` // Combined items/sec (EMA-smoothed)
	BytesPerSecond float64 `json:"bytes_per_second,omitempty"` // Bytes/sec transfer rate (EMA-smoothed)

	// Migration-wide expected denominators (copy/delete).
	FoldersExpected int64 `json:"folders_expected,omitempty"`
	FilesExpected   int64 `json:"files_expected,omitempty"`
	TotalExpected   int64 `json:"total_expected,omitempty"`

	// Items progress (copy/delete); progress_percent aliases items_progress_percent.
	ItemsCompleted       int64   `json:"items_completed,omitempty"`
	ItemsTotal           int64   `json:"items_total,omitempty"`
	ItemsProgressPercent float64 `json:"items_progress_percent,omitempty"`

	// Bytes progress (copy/delete): done vs fixed migration-wide eligible file size.
	BytesTotal           int64   `json:"bytes_total,omitempty"`
	BytesFailed          int64   `json:"bytes_failed,omitempty"`
	BytesProgressPercent float64 `json:"bytes_progress_percent,omitempty"`
	BytesFailedPercent   float64 `json:"bytes_failed_percent,omitempty"`
	ItemsFailedPercent   float64 `json:"items_failed_percent,omitempty"`

	// Deterministic copy/delete progress (0–100). Alias of items_progress_percent. Omitted for traversal.
	ProgressPercent float64 `json:"progress_percent,omitempty"`

	// Common state fields (used by both phases)
	Round         int    `json:"round"`
	Pending       int    `json:"pending"`
	InProgress    int    `json:"in_progress"`
	Workers       int    `json:"workers"`
	TotalPending  int    `json:"total_pending,omitempty"` // Total pending from DB (copy phase)
	TotalFailed   int    `json:"total_failed,omitempty"`  // Total failed from DB (copy phase)
	Name          string `json:"name,omitempty"`          // Queue name ("copy", "src-traversal", etc.)
	PossibleStall bool   `json:"possible_stall,omitempty"`
	// Queue lifecycle from the engine (running | paused | stopped | waiting | completed).
	State string `json:"state,omitempty"`

	// Current-round Expected/Completed (console progress line counters).
	RoundExpected  int `json:"round_expected,omitempty"`
	RoundCompleted int `json:"round_completed,omitempty"`
	CopyPass       int `json:"copy_pass,omitempty"`

	// Active FS rate-limit windows (RFC3339 UTC + remaining ms at poll time).
	RateLimitedUntilSrc       string `json:"rate_limited_until_src,omitempty"`
	RateLimitedUntilDst       string `json:"rate_limited_until_dst,omitempty"`
	RateLimitedRemainingMsSrc int64  `json:"rate_limited_remaining_ms_src,omitempty"`
	RateLimitedRemainingMsDst int64  `json:"rate_limited_remaining_ms_dst,omitempty"`
	InterOpDelayMs            int64  `json:"inter_op_delay_ms,omitempty"`

	// Engine-owned remaining-time estimate (UI renders only).
	EtaSeconds *float64 `json:"eta_seconds,omitempty"`
	EtaBasis   string   `json:"eta_basis,omitempty"` // "items" | "bytes"
}

// QueueMetricsResponse represents all queue metrics for a migration
type QueueMetricsResponse struct {
	Success       bool                  `json:"success"`             // Whether the operation succeeded
	ErrorCode     string                `json:"errorCode,omitempty"` // Error code if success is false (e.g., "DATABASE_NOT_AVAILABLE")
	Error         string                `json:"error,omitempty"`     // Human-readable error message if success is false
	SrcTraversal  *ExternalQueueMetrics `json:"srcTraversal,omitempty"`
	DstTraversal  *ExternalQueueMetrics `json:"dstTraversal,omitempty"`
	Copy          *ExternalQueueMetrics `json:"copy,omitempty"`
	Delete        *ExternalQueueMetrics `json:"delete,omitempty"`
	PossibleStall bool                  `json:"possibleStall,omitempty"`
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

// ListMigrationsRequest represents a request to list migrations with pagination
type ListMigrationsRequest struct {
	Offset int `json:"offset,omitempty"` // Pagination offset (default: 0)
	Limit  int `json:"limit,omitempty"`  // Pagination limit (default: 100, max: 1000)
}

// ListMigrationsResponse represents the response containing paginated migrations
type ListMigrationsResponse struct {
	Migrations   []Status         `json:"migrations"`
	Total        int              `json:"total"`   // Total number of migrations
	Offset       int              `json:"offset"`  // Current offset
	Limit        int              `json:"limit"`   // Current limit
	HasMore      bool             `json:"hasMore"` // True if there are more migrations
	Capabilities UserCapabilities `json:"capabilities"`
}

// UserCapabilities describes admin-only actions available to the current user.
type UserCapabilities struct {
	CleanSlate bool `json:"cleanSlate"`
}

// DeleteSummaryResponse is returned by GET /migrations/{id}/delete-summary for the confirmation modal.
type DeleteSummaryResponse struct {
	SourceRootPath string `json:"sourceRootPath"`
	SourceHost     string `json:"sourceHost,omitempty"`
	Pending        int64  `json:"pending"`
	Failed         int64  `json:"failed"`
	Deleted        int64  `json:"deleted"`
}
