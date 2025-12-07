package database

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/Project-Sylos/Migration-Engine/pkg/db"
	"github.com/Project-Sylos/Migration-Engine/pkg/migration"
	"github.com/rs/zerolog"
	bolt "go.etcd.io/bbolt"
)

// UploadMigrationDBResponse represents the response from uploading a migration DB
type UploadMigrationDBResponse struct {
	Success bool   `json:"success"`
	Error   string `json:"error,omitempty"`
	Path    string `json:"path,omitempty"`
}

// MigrationDBInfo represents information about a migration database file
type MigrationDBInfo struct {
	Filename   string    `json:"filename"`
	Path       string    `json:"path"`
	Size       int64     `json:"size"`
	ModifiedAt time.Time `json:"modifiedAt"`
}

func UploadMigrationDB(ctx context.Context, logger zerolog.Logger, storageDir, filename string, data []byte, overwrite bool) (UploadMigrationDBResponse, error) {
	// Validate filename
	if filename == "" {
		return UploadMigrationDBResponse{
			Success: false,
			Error:   "filename is required",
		}, nil
	}

	// Ensure filename ends with .db
	if !strings.HasSuffix(filename, ".db") {
		filename = filename + ".db"
	}

	// Sanitize filename to prevent path traversal
	filename = filepath.Base(filename)
	if filename == "." || filename == ".." {
		return UploadMigrationDBResponse{
			Success: false,
			Error:   "invalid filename",
		}, nil
	}

	// Construct full path
	dbPath := filepath.Join(storageDir, filename)

	// Check if file already exists
	if _, err := os.Stat(dbPath); err == nil {
		if !overwrite {
			return UploadMigrationDBResponse{
				Success: false,
				Error:   "file already present on API",
			}, nil
		}
	}

	// Write file
	if err := os.WriteFile(dbPath, data, 0o644); err != nil {
		logger.Error().Err(err).Str("filename", filename).Msg("failed to write migration DB file")
		return UploadMigrationDBResponse{
			Success: false,
			Error:   fmt.Sprintf("failed to save file: %v", err),
		}, nil
	}

	logger.Info().
		Str("filename", filename).
		Str("path", dbPath).
		Int("size", len(data)).
		Bool("overwrite", overwrite).
		Msg("uploaded migration DB file")

	return UploadMigrationDBResponse{
		Success: true,
		Path:    dbPath,
	}, nil
}

func ListMigrationDBs(ctx context.Context, logger zerolog.Logger, storageDir string) ([]MigrationDBInfo, error) {
	entries, err := os.ReadDir(storageDir)
	if err != nil {
		if os.IsNotExist(err) {
			return []MigrationDBInfo{}, nil
		}
		return []MigrationDBInfo{}, fmt.Errorf("failed to read migration DB storage directory: %w", err)
	}

	var dbs []MigrationDBInfo
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		if !strings.HasSuffix(entry.Name(), ".db") {
			continue
		}

		fullPath := filepath.Join(storageDir, entry.Name())
		info, err := entry.Info()
		if err != nil {
			logger.Warn().Err(err).Str("filename", entry.Name()).Msg("failed to get file info for migration DB")
			continue
		}

		dbs = append(dbs, MigrationDBInfo{
			Filename:   entry.Name(),
			Path:       fullPath,
			Size:       info.Size(),
			ModifiedAt: info.ModTime(),
		})
	}

	return dbs, nil
}

// ResolveDatabasePath resolves the database path from migration ID or explicit path
func ResolveDatabasePath(dataDir, explicitPath, migrationID string) (string, error) {
	if explicitPath != "" {
		return explicitPath, nil
	}

	if migrationID == "" {
		return "", fmt.Errorf("migration ID is required when path is not provided")
	}

	// Construct database path from migration ID
	dbPath := filepath.Join(dataDir, migrationID+".db")
	return dbPath, nil
}

// DatabasePathFromConfigPath derives the database path from a config path
// Follows pattern: {config_path sans .yaml}.db
func DatabasePathFromConfigPath(configPath string) string {
	return strings.TrimSuffix(configPath, ".yaml") + ".db"
}

// ConfigPathFromDatabasePath derives the config path from a database path
// Follows pattern: {database_path sans .db}.yaml
func ConfigPathFromDatabasePath(dbPath string) string {
	return strings.TrimSuffix(dbPath, ".db") + ".yaml"
}

func InspectMigrationStatusFromDB(ctx context.Context, logger zerolog.Logger, dbPath string) (migration.MigrationStatus, error) {
	options := db.Options{
		Path: dbPath,
	}
	// Open database
	database, err := db.Open(options)
	if err != nil {
		return migration.MigrationStatus{}, fmt.Errorf("failed to open database: %w", err)
	}
	defer func() {
		if err := database.Close(); err != nil {
			logger.Warn().Err(err).Str("db_path", dbPath).Msg("failed to close database after inspection")
		}
	}()

	// Validate schema
	if err := database.ValidateCoreSchema(); err != nil {
		return migration.MigrationStatus{}, fmt.Errorf("database schema invalid: %w", err)
	}

	// Inspect status
	status, err := migration.InspectMigrationStatus(database)
	if err != nil {
		return migration.MigrationStatus{}, fmt.Errorf("failed to inspect migration status: %w", err)
	}

	return status, nil
}

// QueueObserverMetrics represents queue metrics from the migration engine observer
type QueueObserverMetrics struct {
	QueueStats
	AverageExecutionTime time.Duration `json:"averageExecutionTime"` // in nanoseconds
	TasksPerSecond       float64       `json:"tasksPerSecond"`
	TotalCompleted       int           `json:"totalCompleted"`
	LastPollTime         time.Time     `json:"lastPollTime"`
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
	SrcTraversal *QueueObserverMetrics `json:"srcTraversal,omitempty"`
	DstTraversal *QueueObserverMetrics `json:"dstTraversal,omitempty"`
	Copy         *QueueObserverMetrics `json:"copy,omitempty"`
}

// GetQueueMetricsFromDB retrieves queue metrics from a migration database
func GetQueueMetricsFromDB(ctx context.Context, logger zerolog.Logger, dbPath string) (*QueueMetricsResponse, error) {
	// Open BoltDB directly
	boltDB, err := bolt.Open(dbPath, 0o444, &bolt.Options{ReadOnly: true})
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}
	defer func() {
		if err := boltDB.Close(); err != nil {
			logger.Warn().Err(err).Str("db_path", dbPath).Msg("failed to close database after reading queue metrics")
		}
	}()

	response := &QueueMetricsResponse{}

	// Query queue stats from /Traversal-Data/STATS/queue-stats bucket using helper function
	err = boltDB.View(func(tx *bolt.Tx) error {
		queueStatsBucket := db.GetQueueStatsBucket(tx)
		if queueStatsBucket == nil {
			// queue-stats bucket doesn't exist - observer hasn't started yet
			return nil
		}

		// Get metrics for each queue
		queueKeys := []string{"src-traversal", "dst-traversal", "copy"}
		for _, key := range queueKeys {
			metricsJSON := queueStatsBucket.Get([]byte(key))
			if metricsJSON == nil {
				continue
			}

			var metrics QueueObserverMetrics
			if err := json.Unmarshal(metricsJSON, &metrics); err != nil {
				logger.Warn().Err(err).Str("queue", key).Msg("failed to unmarshal queue metrics")
				continue
			}

			switch key {
			case "src-traversal":
				response.SrcTraversal = &metrics
			case "dst-traversal":
				response.DstTraversal = &metrics
			case "copy":
				response.Copy = &metrics
			}
		}

		return nil
	})

	if err != nil {
		return nil, fmt.Errorf("failed to read queue metrics: %w", err)
	}

	return response, nil
}

// GetQueueMetricsFromDBInstance retrieves queue metrics from a migration database instance
// This uses the shared DB instance from a running migration to avoid opening a new connection
func GetQueueMetricsFromDBInstance(ctx context.Context, logger zerolog.Logger, dbInstance *db.DB) (*QueueMetricsResponse, error) {
	response := &QueueMetricsResponse{}

	// Query queue stats from /Traversal-Data/STATS/queue-stats bucket using helper function
	err := dbInstance.View(func(tx *bolt.Tx) error {
		queueStatsBucket := db.GetQueueStatsBucket(tx)
		if queueStatsBucket == nil {
			// queue-stats bucket doesn't exist - observer hasn't started yet
			return nil
		}

		// Get metrics for each queue
		queueKeys := []string{"src-traversal", "dst-traversal", "copy"}
		for _, key := range queueKeys {
			metricsJSON := queueStatsBucket.Get([]byte(key))
			if metricsJSON == nil {
				continue
			}

			var metrics QueueObserverMetrics
			if err := json.Unmarshal(metricsJSON, &metrics); err != nil {
				logger.Warn().Err(err).Str("queue", key).Msg("failed to unmarshal queue metrics")
				continue
			}

			switch key {
			case "src-traversal":
				response.SrcTraversal = &metrics
			case "dst-traversal":
				response.DstTraversal = &metrics
			case "copy":
				response.Copy = &metrics
			}
		}

		return nil
	})

	if err != nil {
		return nil, fmt.Errorf("failed to read queue metrics: %w", err)
	}

	return response, nil
}

// LogEntry represents a single log entry from the database
type LogEntry struct {
	ID    string                 `json:"id"`
	Level string                 `json:"level"`
	Data  map[string]interface{} `json:"data"`
}

// GetLogsFromDB retrieves logs from a migration database
// Returns up to 1000 logs per level, ordered descending by ID (newest first)
func GetLogsFromDB(ctx context.Context, logger zerolog.Logger, dbPath string) (map[string][]LogEntry, error) {
	// Open BoltDB directly
	boltDB, err := bolt.Open(dbPath, 0o444, &bolt.Options{ReadOnly: true})
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}
	defer func() {
		if err := boltDB.Close(); err != nil {
			logger.Warn().Err(err).Str("db_path", dbPath).Msg("failed to close database after reading logs")
		}
	}()

	result := make(map[string][]LogEntry)
	logLevels := []string{"trace", "debug", "info", "warning", "error", "critical"}

	// Query logs from LOGS bucket
	err = boltDB.View(func(tx *bolt.Tx) error {
		logsBucket := tx.Bucket([]byte("LOGS"))
		if logsBucket == nil {
			// LOGS bucket doesn't exist - no logs yet
			return nil
		}

		// Query each log level bucket
		for _, level := range logLevels {
			levelBucket := logsBucket.Bucket([]byte(level))
			if levelBucket == nil {
				// This log level bucket doesn't exist - return empty array
				result[level] = []LogEntry{}
				continue
			}

			logs := make([]LogEntry, 0, 1000)

			// Iterate in descending order (newest first)
			cursor := levelBucket.Cursor()
			count := 0
			maxLogs := 1000

			// Start from the last (newest) entry and collect up to maxLogs
			for k, v := cursor.Last(); k != nil && count < maxLogs; k, v = cursor.Prev() {
				logID := string(k)

				// Parse the log entry
				var logData map[string]interface{}
				if err := json.Unmarshal(v, &logData); err != nil {
					logger.Warn().Err(err).Str("level", level).Str("log_id", logID).Msg("failed to unmarshal log entry")
					continue
				}

				logs = append(logs, LogEntry{
					ID:    logID,
					Level: level,
					Data:  logData,
				})
				count++
			}

			result[level] = logs
		}

		return nil
	})

	if err != nil {
		return nil, fmt.Errorf("failed to read logs: %w", err)
	}

	return result, nil
}

// GetLogsFromDBInstance retrieves logs from a migration database instance
// This uses the shared DB instance from a running migration to avoid opening a new connection
// Returns up to 1000 logs per level, ordered descending by ID (newest first)
func GetLogsFromDBInstance(ctx context.Context, logger zerolog.Logger, dbInstance *db.DB) (map[string][]LogEntry, error) {
	result := make(map[string][]LogEntry)
	logLevels := []string{"trace", "debug", "info", "warning", "error", "critical"}

	// Query logs from LOGS bucket using the db.DB instance's View method
	err := dbInstance.View(func(tx *bolt.Tx) error {
		logsBucket := tx.Bucket([]byte("LOGS"))
		if logsBucket == nil {
			// LOGS bucket doesn't exist - no logs yet
			return nil
		}

		// Query each log level bucket
		for _, level := range logLevels {
			levelBucket := logsBucket.Bucket([]byte(level))
			if levelBucket == nil {
				// This log level bucket doesn't exist - return empty array
				result[level] = []LogEntry{}
				continue
			}

			logs := make([]LogEntry, 0, 1000)

			// Iterate in descending order (newest first)
			cursor := levelBucket.Cursor()
			count := 0
			maxLogs := 1000

			// Start from the last (newest) entry and collect up to maxLogs
			for k, v := cursor.Last(); k != nil && count < maxLogs; k, v = cursor.Prev() {
				logID := string(k)

				// Parse the log entry
				var logData map[string]interface{}
				if err := json.Unmarshal(v, &logData); err != nil {
					logger.Warn().Err(err).Str("level", level).Str("log_id", logID).Msg("failed to unmarshal log entry")
					continue
				}

				logs = append(logs, LogEntry{
					ID:    logID,
					Level: level,
					Data:  logData,
				})
				count++
			}

			result[level] = logs
		}

		return nil
	})

	if err != nil {
		return nil, fmt.Errorf("failed to read logs: %w", err)
	}

	return result, nil
}

// DiffItem represents a folder or file with status information from both queues
// Defined in database package to avoid import cycles
type DiffItem struct {
	Id              string `json:"id"`
	ParentId        string `json:"parentId,omitempty"`
	ParentPath      string `json:"parentPath,omitempty"`
	DisplayName     string `json:"displayName"`
	LocationPath    string `json:"locationPath"`
	LastUpdated     string `json:"lastUpdated,omitempty"`
	DepthLevel      int    `json:"depthLevel"`
	Type            string `json:"type"`                 // "folder" or "file"
	Size            int64  `json:"size,omitempty"`       // Only for files
	TraversalStatus string `json:"traversalStatus"`      // "pending", "successful", "failed", "not_on_src"
	CopyStatus      string `json:"copyStatus,omitempty"` // "pending", "successful", "failed" (for future copy phase)
	InSrc           bool   `json:"inSrc"`                // Whether item exists in source queue
	InDst           bool   `json:"inDst"`                // Whether item exists in destination queue
}

// PaginationInfo provides pagination metadata
// Defined in database package to avoid import cycles
type PaginationInfo struct {
	Offset       int  `json:"offset"`       // Current offset
	Limit        int  `json:"limit"`        // Current limit
	Total        int  `json:"total"`        // Total number of items (folders + files, or just folders if foldersOnly=true)
	TotalFolders int  `json:"totalFolders"` // Total number of folders
	TotalFiles   int  `json:"totalFiles"`   // Total number of files
	HasMore      bool `json:"hasMore"`      // Whether there are more items beyond the current page
}

// hashPath hashes a path using SHA256 and returns the hex-encoded hash
func hashPath(path string) []byte {
	h := sha256.Sum256([]byte(path))
	return h[:]
}

// formatLevel formats a level number as an 8-digit zero-padded string
func formatLevel(level int) string {
	return fmt.Sprintf("%08d", level)
}

// GetChildrenDiffsFromDB retrieves merged children from both SRC and DST queues with their status information
func GetChildrenDiffsFromDB(ctx context.Context, logger zerolog.Logger, dbPath string, path string, offset, limit int, foldersOnly bool) ([]DiffItem, []DiffItem, PaginationInfo, error) {
	// Open BoltDB directly
	boltDB, err := bolt.Open(dbPath, 0o444, &bolt.Options{ReadOnly: true})
	if err != nil {
		return nil, nil, PaginationInfo{}, fmt.Errorf("failed to open database: %w", err)
	}
	defer func() {
		if err := boltDB.Close(); err != nil {
			logger.Warn().Err(err).Str("db_path", dbPath).Msg("failed to close database after reading children diffs")
		}
	}()

	var folders []DiffItem
	var files []DiffItem
	var pagination PaginationInfo

	err = boltDB.View(func(tx *bolt.Tx) error {
		folders, files, pagination, err = getChildrenDiffsFromTx(ctx, logger, tx, path, offset, limit, foldersOnly)
		return err
	})

	if err != nil {
		return nil, nil, PaginationInfo{}, fmt.Errorf("failed to read children diffs: %w", err)
	}

	return folders, files, pagination, nil
}

// GetChildrenDiffsFromDBInstance retrieves merged children from both SRC and DST queues using a DB instance
func GetChildrenDiffsFromDBInstance(ctx context.Context, logger zerolog.Logger, dbInstance *db.DB, path string, offset, limit int, foldersOnly bool) ([]DiffItem, []DiffItem, PaginationInfo, error) {
	var folders []DiffItem
	var files []DiffItem
	var pagination PaginationInfo
	var err error

	err = dbInstance.View(func(tx *bolt.Tx) error {
		folders, files, pagination, err = getChildrenDiffsFromTx(ctx, logger, tx, path, offset, limit, foldersOnly)
		return err
	})

	if err != nil {
		return nil, nil, PaginationInfo{}, fmt.Errorf("failed to read children diffs: %w", err)
	}

	return folders, files, pagination, nil
}

// getChildrenDiffsFromTx implements the core logic for retrieving and merging children from SRC and DST
func getChildrenDiffsFromTx(ctx context.Context, logger zerolog.Logger, tx *bolt.Tx, path string, offset, limit int, foldersOnly bool) ([]DiffItem, []DiffItem, PaginationInfo, error) {
	// Check for context cancellation
	if ctx.Err() != nil {
		return nil, nil, PaginationInfo{}, ctx.Err()
	}

	// Hash the parent path to get the key for children bucket
	parentHash := hashPath(path)

	// Collect all unique child path hashes from both SRC and DST
	// Use byte slices as keys for pathHash comparison
	childHashesMap := make(map[string]bool) // pathHash hex string -> exists

	// Get children from SRC queue
	srcChildrenBucket := db.GetChildrenBucket(tx, "SRC")
	if srcChildrenBucket != nil {
		childrenJSON := srcChildrenBucket.Get(parentHash)
		if childrenJSON != nil {
			var childHashes [][]byte
			if err := json.Unmarshal(childrenJSON, &childHashes); err != nil {
				logger.Warn().Err(err).Str("path", path).Str("queue", "SRC").Msg("failed to unmarshal children JSON")
			} else {
				for _, hashBytes := range childHashes {
					hashHex := hex.EncodeToString(hashBytes)
					childHashesMap[hashHex] = true
				}
			}
		}
	} else {
		logger.Debug().Str("path", path).Msg("SRC children bucket does not exist")
	}

	// Get children from DST queue
	dstChildrenBucket := db.GetChildrenBucket(tx, "DST")
	if dstChildrenBucket != nil {
		childrenJSON := dstChildrenBucket.Get(parentHash)
		if childrenJSON != nil {
			var childHashes [][]byte
			if err := json.Unmarshal(childrenJSON, &childHashes); err != nil {
				logger.Warn().Err(err).Str("path", path).Str("queue", "DST").Msg("failed to unmarshal children JSON")
			} else {
				for _, hashBytes := range childHashes {
					hashHex := hex.EncodeToString(hashBytes)
					childHashesMap[hashHex] = true
				}
			}
		}
	} else {
		logger.Debug().Str("path", path).Msg("DST children bucket does not exist")
	}

	if len(childHashesMap) == 0 {
		return []DiffItem{}, []DiffItem{}, PaginationInfo{
			Offset:       offset,
			Limit:        limit,
			Total:        0,
			TotalFolders: 0,
			TotalFiles:   0,
			HasMore:      false,
		}, nil
	}

	// Build merged diff items from all child hashes
	allItems := make([]DiffItem, 0, len(childHashesMap))

	// Get nodes buckets for both queues
	srcNodesBucket := db.GetNodesBucket(tx, "SRC")
	dstNodesBucket := db.GetNodesBucket(tx, "DST")

	for pathHashHex := range childHashesMap {
		pathHash, err := hex.DecodeString(pathHashHex)
		if err != nil {
			logger.Warn().Err(err).Str("pathHash", pathHashHex).Msg("failed to decode path hash")
			continue
		}

		var srcNodeState *db.NodeState
		var dstNodeState *db.NodeState

		// Fetch NodeState from SRC if available
		if srcNodesBucket != nil {
			nodeData := srcNodesBucket.Get(pathHash)
			if nodeData != nil {
				var nodeState db.NodeState
				if err := json.Unmarshal(nodeData, &nodeState); err != nil {
					logger.Warn().Err(err).Str("pathHash", pathHashHex).Str("queue", "SRC").Msg("failed to unmarshal NodeState")
				} else {
					srcNodeState = &nodeState
				}
			}
		}

		// Fetch NodeState from DST if available
		if dstNodesBucket != nil {
			nodeData := dstNodesBucket.Get(pathHash)
			if nodeData != nil {
				var nodeState db.NodeState
				if err := json.Unmarshal(nodeData, &nodeState); err != nil {
					logger.Warn().Err(err).Str("pathHash", pathHashHex).Str("queue", "DST").Msg("failed to unmarshal NodeState")
				} else {
					dstNodeState = &nodeState
				}
			}
		}

		// Determine which NodeState to use as primary (prefer SRC if available)
		var primaryNodeState *db.NodeState
		if srcNodeState != nil {
			primaryNodeState = srcNodeState
		} else if dstNodeState != nil {
			primaryNodeState = dstNodeState
		} else {
			// No node state found for this hash, skip
			logger.Debug().Str("pathHash", pathHashHex).Msg("no NodeState found in SRC or DST for child hash, skipping")
			continue
		}

		// Get traversal status from status-lookup index
		traversalStatus := determineTraversalStatus(tx, logger, srcNodeState, dstNodeState)

		// Build DiffItem from NodeState
		// Extract path and derive display name from it
		displayName := filepath.Base(primaryNodeState.Path)
		if displayName == "" || displayName == "." || displayName == "/" {
			displayName = primaryNodeState.Path
		}

		item := DiffItem{
			Id:              primaryNodeState.ID,
			ParentId:        primaryNodeState.ParentID,
			ParentPath:      primaryNodeState.ParentPath,
			DisplayName:     displayName,
			LocationPath:    primaryNodeState.Path,
			LastUpdated:     time.Now().Format(time.RFC3339), // NodeState doesn't store LastUpdated
			DepthLevel:      primaryNodeState.Depth,
			Type:            primaryNodeState.Type,
			TraversalStatus: traversalStatus,
			InSrc:           srcNodeState != nil,
			InDst:           dstNodeState != nil,
		}

		// Add size for files (Size is int64, not pointer)
		if primaryNodeState.Type == "file" {
			item.Size = primaryNodeState.Size
		}

		allItems = append(allItems, item)
	}

	// Separate folders and files
	folders := make([]DiffItem, 0)
	files := make([]DiffItem, 0)

	for _, item := range allItems {
		if item.Type == "folder" {
			folders = append(folders, item)
		} else {
			files = append(files, item)
		}
	}

	// Sort folders and files by display name
	sort.Slice(folders, func(i, j int) bool {
		return folders[i].DisplayName < folders[j].DisplayName
	})
	sort.Slice(files, func(i, j int) bool {
		return files[i].DisplayName < files[j].DisplayName
	})

	totalFolders := len(folders)
	totalFiles := len(files)

	// Apply foldersOnly filter if requested
	var itemsToPaginate []DiffItem
	var totalItems int

	if foldersOnly {
		itemsToPaginate = folders
		totalItems = totalFolders
	} else {
		// Combine folders and files (folders first)
		itemsToPaginate = make([]DiffItem, 0, len(folders)+len(files))
		itemsToPaginate = append(itemsToPaginate, folders...)
		itemsToPaginate = append(itemsToPaginate, files...)
		totalItems = totalFolders + totalFiles
	}

	// Apply pagination
	startIdx := offset
	endIdx := offset + limit
	if startIdx > len(itemsToPaginate) {
		startIdx = len(itemsToPaginate)
	}
	if endIdx > len(itemsToPaginate) {
		endIdx = len(itemsToPaginate)
	}

	var paginatedItems []DiffItem
	if startIdx < endIdx {
		paginatedItems = itemsToPaginate[startIdx:endIdx]
	} else {
		paginatedItems = []DiffItem{}
	}

	// Separate paginated items back into folders and files
	resultFolders := make([]DiffItem, 0)
	resultFiles := make([]DiffItem, 0)

	if foldersOnly {
		resultFolders = paginatedItems
	} else {
		for _, item := range paginatedItems {
			if item.Type == "folder" {
				resultFolders = append(resultFolders, item)
			} else {
				resultFiles = append(resultFiles, item)
			}
		}
	}

	pagination := PaginationInfo{
		Offset:       offset,
		Limit:        limit,
		Total:        totalItems,
		TotalFolders: totalFolders,
		TotalFiles:   totalFiles,
		HasMore:      endIdx < len(itemsToPaginate),
	}

	return resultFolders, resultFiles, pagination, nil
}

// determineTraversalStatus determines the overall traversal status based on SRC and DST status
func determineTraversalStatus(tx *bolt.Tx, logger zerolog.Logger, srcNodeState *db.NodeState, dstNodeState *db.NodeState) string {
	// Priority order: failed > pending > not_on_src > successful

	srcStatus := ""
	dstStatus := ""

	if srcNodeState != nil {
		srcStatus = getStatusFromLookup(tx, logger, "SRC", srcNodeState.Depth, srcNodeState.Path)
	}
	if dstNodeState != nil {
		dstStatus = getStatusFromLookup(tx, logger, "DST", dstNodeState.Depth, dstNodeState.Path)
	}

	// If only in DST and status is "not_on_src", return that
	if srcNodeState == nil && dstNodeState != nil && dstStatus == "not_on_src" {
		return "not_on_src"
	}

	// If only in SRC, return SRC status (default to "pending" if not found)
	if dstNodeState == nil && srcNodeState != nil {
		if srcStatus == "" {
			return "pending"
		}
		return srcStatus
	}

	// Both exist - use the highest priority status
	statusPriority := map[string]int{
		"failed":     4,
		"pending":    3,
		"not_on_src": 2,
		"successful": 1,
	}

	srcPriority := statusPriority[srcStatus]
	dstPriority := statusPriority[dstStatus]

	if srcPriority > dstPriority {
		return srcStatus
	}
	if dstPriority > 0 {
		return dstStatus
	}

	// Default fallback
	return "pending"
}

// getStatusFromLookup retrieves the traversal status from the status-lookup index
// Manually navigates to /Traversal-Data/{queueType}/levels/{formattedLevel}/status-lookup
func getStatusFromLookup(tx *bolt.Tx, logger zerolog.Logger, queueType string, level int, path string) string {
	pathHash := hashPath(path)
	formattedLevel := formatLevel(level)

	// Navigate to /Traversal-Data/{queueType}/levels/{formattedLevel}/status-lookup
	traversalDataBucket := tx.Bucket([]byte("Traversal-Data"))
	if traversalDataBucket == nil {
		logger.Debug().
			Str("queueType", queueType).
			Int("level", level).
			Str("formattedLevel", formattedLevel).
			Str("path", path).
			Msg("Traversal-Data bucket does not exist")
		return ""
	}

	queueBucket := traversalDataBucket.Bucket([]byte(queueType))
	if queueBucket == nil {
		logger.Debug().
			Str("queueType", queueType).
			Int("level", level).
			Str("formattedLevel", formattedLevel).
			Str("path", path).
			Msg("queue bucket does not exist in Traversal-Data")
		return ""
	}

	levelsBucket := queueBucket.Bucket([]byte("levels"))
	if levelsBucket == nil {
		logger.Debug().
			Str("queueType", queueType).
			Int("level", level).
			Str("formattedLevel", formattedLevel).
			Str("path", path).
			Msg("levels bucket does not exist")
		return ""
	}

	levelBucket := levelsBucket.Bucket([]byte(formattedLevel))
	if levelBucket == nil {
		logger.Debug().
			Str("queueType", queueType).
			Int("level", level).
			Str("formattedLevel", formattedLevel).
			Str("path", path).
			Msg("level bucket does not exist")
		return ""
	}

	statusLookupBucket := levelBucket.Bucket([]byte("status-lookup"))
	if statusLookupBucket == nil {
		logger.Debug().
			Str("queueType", queueType).
			Int("level", level).
			Str("formattedLevel", formattedLevel).
			Str("path", path).
			Msg("status-lookup bucket does not exist for level")
		return ""
	}

	statusBytes := statusLookupBucket.Get(pathHash)
	if statusBytes == nil {
		logger.Debug().
			Str("queueType", queueType).
			Int("level", level).
			Str("formattedLevel", formattedLevel).
			Str("path", path).
			Msg("status not found in status-lookup index")
		return ""
	}

	status := string(statusBytes)
	logger.Debug().
		Str("queueType", queueType).
		Int("level", level).
		Str("formattedLevel", formattedLevel).
		Str("path", path).
		Str("status", status).
		Msg("retrieved status from status-lookup index")
	return status
}
