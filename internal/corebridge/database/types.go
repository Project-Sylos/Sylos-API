package database

// ExternalQueueMetrics contains user-facing metrics for API access.
// Supports both traversal metrics and copy phase metrics.
type ExternalQueueMetrics struct {
	// Traversal phase metrics
	FilesDiscoveredTotal     int64   `json:"files_discovered_total,omitempty"`
	FoldersDiscoveredTotal   int64   `json:"folders_discovered_total,omitempty"`
	DiscoveryRateItemsPerSec float64 `json:"discovery_rate_items_per_sec,omitempty"`
	TotalDiscovered          int64   `json:"total_discovered,omitempty"` // files + folders

	// Copy phase metrics
	Folders        int64   `json:"folders,omitempty"`
	Files          int64   `json:"files,omitempty"`
	Total          int64   `json:"total,omitempty"`
	Bytes          int64   `json:"bytes,omitempty"`
	ItemsPerSecond float64 `json:"items_per_second,omitempty"`
	BytesPerSecond float64 `json:"bytes_per_second,omitempty"`

	// Common state fields
	Round        int    `json:"round"`
	Pending      int    `json:"pending"`
	InProgress   int    `json:"in_progress"`
	Workers      int    `json:"workers"`
	TotalPending int    `json:"total_pending,omitempty"`
	TotalFailed  int    `json:"total_failed,omitempty"`
	Name         string `json:"name,omitempty"`
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

// QueueMetricsResponse represents all queue metrics for a migration
type QueueMetricsResponse struct {
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

// PaginationInfo provides pagination metadata
type PaginationInfo struct {
	Offset       int    `json:"offset"`
	Limit        int    `json:"limit"`
	Total        int    `json:"total"`
	TotalFolders int    `json:"totalFolders"`
	TotalFiles   int    `json:"totalFiles"`
	HasMore      bool   `json:"hasMore"`
	NextCursor   string `json:"nextCursor,omitempty"`
}

// PathNodes represents the src and dst nodes for a given path
type PathNodes struct {
	Src *PathNodeItem `json:"src,omitempty"`
	Dst *PathNodeItem `json:"dst,omitempty"`
}
