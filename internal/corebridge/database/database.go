package database

import (
	"archive/zip"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
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

// UploadMigrationDB uploads a migration database file to the migration-specific folder
// New structure: saves to dataDir/{migrationID}/{migrationID}.db
func UploadMigrationDB(ctx context.Context, logger zerolog.Logger, dataDir, migrationID string, data []byte, overwrite bool) (UploadMigrationDBResponse, error) {
	if migrationID == "" {
		return UploadMigrationDBResponse{
			Success: false,
			Error:   "migration ID is required",
		}, nil
	}

	// Construct path in migration-specific folder
	migrationDir := GetMigrationDir(dataDir, migrationID)
	dbPath := filepath.Join(migrationDir, migrationID+".db")

	// Check if file already exists
	if _, err := os.Stat(dbPath); err == nil {
		if !overwrite {
			return UploadMigrationDBResponse{
				Success: false,
				Error:   "file already present on API",
			}, nil
		}
	}

	// Ensure migration directory exists
	if err := os.MkdirAll(migrationDir, 0o755); err != nil {
		logger.Error().Err(err).Str("migration_id", migrationID).Msg("failed to create migration directory")
		return UploadMigrationDBResponse{
			Success: false,
			Error:   fmt.Sprintf("failed to create migration directory: %v", err),
		}, nil
	}

	// Write file
	if err := os.WriteFile(dbPath, data, 0o644); err != nil {
		logger.Error().Err(err).Str("migration_id", migrationID).Msg("failed to write migration DB file")
		return UploadMigrationDBResponse{
			Success: false,
			Error:   fmt.Sprintf("failed to save file: %v", err),
		}, nil
	}

	logger.Info().
		Str("migration_id", migrationID).
		Str("path", dbPath).
		Int("size", len(data)).
		Bool("overwrite", overwrite).
		Msg("uploaded migration DB file")

	return UploadMigrationDBResponse{
		Success: true,
		Path:    dbPath,
	}, nil
}

// UploadMigrationYAML uploads a migration YAML config file to the migration-specific folder
// New structure: saves to dataDir/{migrationID}/{migrationID}.yaml
func UploadMigrationYAML(ctx context.Context, logger zerolog.Logger, dataDir, migrationID string, data []byte, overwrite bool) (UploadMigrationDBResponse, error) {
	if migrationID == "" {
		return UploadMigrationDBResponse{
			Success: false,
			Error:   "migration ID is required",
		}, nil
	}

	// Construct path in migration-specific folder
	migrationDir := GetMigrationDir(dataDir, migrationID)
	yamlPath := filepath.Join(migrationDir, migrationID+".yaml")

	// Check if file already exists
	if _, err := os.Stat(yamlPath); err == nil {
		if !overwrite {
			return UploadMigrationDBResponse{
				Success: false,
				Error:   "file already present on API",
			}, nil
		}
	}

	// Ensure migration directory exists
	if err := os.MkdirAll(migrationDir, 0o755); err != nil {
		logger.Error().Err(err).Str("migration_id", migrationID).Msg("failed to create migration directory")
		return UploadMigrationDBResponse{
			Success: false,
			Error:   fmt.Sprintf("failed to create migration directory: %v", err),
		}, nil
	}

	// Write file
	if err := os.WriteFile(yamlPath, data, 0o644); err != nil {
		logger.Error().Err(err).Str("migration_id", migrationID).Msg("failed to write migration YAML file")
		return UploadMigrationDBResponse{
			Success: false,
			Error:   fmt.Sprintf("failed to save file: %v", err),
		}, nil
	}

	logger.Info().
		Str("migration_id", migrationID).
		Str("path", yamlPath).
		Int("size", len(data)).
		Bool("overwrite", overwrite).
		Msg("uploaded migration YAML file")

	return UploadMigrationDBResponse{
		Success: true,
		Path:    yamlPath,
	}, nil
}

// UploadMigrationData uploads a zip file containing migration data (YAML, DB, and related files)
// Extracts the zip to dataDir/{migrationID}/
func UploadMigrationData(ctx context.Context, logger zerolog.Logger, dataDir, migrationID string, zipData []byte, overwrite bool) (UploadMigrationDBResponse, error) {
	if migrationID == "" {
		return UploadMigrationDBResponse{
			Success: false,
			Error:   "migration ID is required",
		}, nil
	}

	// Construct migration directory path
	migrationDir := GetMigrationDir(dataDir, migrationID)

	// Check if directory already exists and has files
	if info, err := os.Stat(migrationDir); err == nil && info.IsDir() {
		entries, err := os.ReadDir(migrationDir)
		if err == nil && len(entries) > 0 {
			if !overwrite {
				return UploadMigrationDBResponse{
					Success: false,
					Error:   "migration directory already contains files",
				}, nil
			}
		}
	}

	// Create a temporary file for the zip
	tmpZip, err := os.CreateTemp("", "migration-upload-*.zip")
	if err != nil {
		return UploadMigrationDBResponse{
			Success: false,
			Error:   fmt.Sprintf("failed to create temporary file: %v", err),
		}, nil
	}
	tmpZipPath := tmpZip.Name()
	defer os.Remove(tmpZipPath)
	defer tmpZip.Close()

	// Write zip data to temporary file
	if _, err := tmpZip.Write(zipData); err != nil {
		return UploadMigrationDBResponse{
			Success: false,
			Error:   fmt.Sprintf("failed to write zip data: %v", err),
		}, nil
	}
	tmpZip.Close()

	// Open the zip file
	zipReader, err := zip.OpenReader(tmpZipPath)
	if err != nil {
		return UploadMigrationDBResponse{
			Success: false,
			Error:   fmt.Sprintf("failed to open zip file: %v", err),
		}, nil
	}
	defer zipReader.Close()

	// Ensure migration directory exists
	if err := os.MkdirAll(migrationDir, 0o755); err != nil {
		logger.Error().Err(err).Str("migration_id", migrationID).Msg("failed to create migration directory")
		return UploadMigrationDBResponse{
			Success: false,
			Error:   fmt.Sprintf("failed to create migration directory: %v", err),
		}, nil
	}

	// Extract all files from the zip
	for _, file := range zipReader.File {
		// Sanitize file path to prevent path traversal
		filePath := filepath.Join(migrationDir, filepath.Base(file.Name))

		// Open file from zip
		rc, err := file.Open()
		if err != nil {
			logger.Warn().Err(err).Str("file", file.Name).Msg("failed to open file from zip, skipping")
			continue
		}

		// Create destination file
		dstFile, err := os.Create(filePath)
		if err != nil {
			rc.Close()
			logger.Warn().Err(err).Str("file", filePath).Msg("failed to create destination file, skipping")
			continue
		}

		// Copy file contents
		_, err = io.Copy(dstFile, rc)
		rc.Close()
		dstFile.Close()

		if err != nil {
			logger.Warn().Err(err).Str("file", filePath).Msg("failed to extract file from zip, skipping")
			continue
		}

		// Set file permissions
		if err := os.Chmod(filePath, 0o644); err != nil {
			logger.Warn().Err(err).Str("file", filePath).Msg("failed to set file permissions")
		}
	}

	logger.Info().
		Str("migration_id", migrationID).
		Str("path", migrationDir).
		Int("size", len(zipData)).
		Bool("overwrite", overwrite).
		Msg("uploaded and extracted migration data zip file")

	return UploadMigrationDBResponse{
		Success: true,
		Path:    migrationDir,
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
// New structure: dataDir/{migrationID}/{migrationID}.db
func ResolveDatabasePath(dataDir, explicitPath, migrationID string) (string, error) {
	if explicitPath != "" {
		return explicitPath, nil
	}

	if migrationID == "" {
		return "", fmt.Errorf("migration ID is required when path is not provided")
	}

	// Construct database path in migration-specific folder
	migrationDir := filepath.Join(dataDir, migrationID)
	dbPath := filepath.Join(migrationDir, migrationID+".db")
	return dbPath, nil
}

// GetMigrationDir returns the directory path for a migration
func GetMigrationDir(dataDir, migrationID string) string {
	return filepath.Join(dataDir, migrationID)
}

// DatabasePathFromConfigPath derives the database path from a config path
// New structure: config is in dataDir/{migrationID}/{migrationID}.yaml
// DB is in dataDir/{migrationID}/{migrationID}.db
func DatabasePathFromConfigPath(configPath string) string {
	dir := filepath.Dir(configPath)
	filename := filepath.Base(configPath)
	migrationID := strings.TrimSuffix(filename, ".yaml")
	return filepath.Join(dir, migrationID+".db")
}

// ConfigPathFromDatabasePath derives the config path from a database path
// New structure: DB is in dataDir/{migrationID}/{migrationID}.db
// Config is in dataDir/{migrationID}/{migrationID}.yaml
func ConfigPathFromDatabasePath(dbPath string) string {
	dir := filepath.Dir(dbPath)
	filename := filepath.Base(dbPath)
	migrationID := strings.TrimSuffix(filename, ".db")
	return filepath.Join(dir, migrationID+".yaml")
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

// PathNodeItem represents a single node (from either SRC or DST) with its metadata
// Defined in database package to avoid import cycles
type PathNodeItem struct {
	Queue           string `json:"queue"` // "SRC" or "DST"
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

// formatLevel formats a level number as an 8-digit zero-padded string
func formatLevel(level int) string {
	return fmt.Sprintf("%08d", level)
}

// PathNodes represents the src and dst nodes for a given path
type PathNodes struct {
	Src *PathNodeItem `json:"src,omitempty"` // SRC node if exists
	Dst *PathNodeItem `json:"dst,omitempty"` // DST node if exists
}

// GetChildrenDiffsFromDB retrieves merged children from both SRC and DST queues with their status information
// Returns a map where key is the path and value contains src/dst nodes (if they exist)
func GetChildrenDiffsFromDB(ctx context.Context, logger zerolog.Logger, dbPath string, path string, offset, limit int, foldersOnly bool) (map[string]PathNodes, PaginationInfo, error) {
	// Open BoltDB directly
	boltDB, err := bolt.Open(dbPath, 0o444, &bolt.Options{ReadOnly: true})
	if err != nil {
		return map[string]PathNodes{}, PaginationInfo{}, fmt.Errorf("failed to open database: %w", err)
	}
	defer func() {
		if err := boltDB.Close(); err != nil {
			logger.Warn().Err(err).Str("db_path", dbPath).Msg("failed to close database after reading children diffs")
		}
	}()

	var items map[string]PathNodes
	var pagination PaginationInfo

	err = boltDB.View(func(tx *bolt.Tx) error {
		// DIAGNOSTIC: Inspect root paths when querying root
		if path == "/" {
			DebugInspectRootPathsInTx(ctx, logger, tx)
		}

		items, pagination, err = getChildrenDiffsFromTx(ctx, logger, tx, path, offset, limit, foldersOnly)
		return err
	})

	if err != nil {
		return map[string]PathNodes{}, PaginationInfo{}, fmt.Errorf("failed to read children diffs: %w", err)
	}

	return items, pagination, nil
}

// GetChildrenDiffsFromDBInstance retrieves merged children from both SRC and DST queues using a DB instance
// Returns a map where key is the path and value contains src/dst nodes (if they exist)
func GetChildrenDiffsFromDBInstance(ctx context.Context, logger zerolog.Logger, dbInstance *db.DB, path string, offset, limit int, foldersOnly bool) (map[string]PathNodes, PaginationInfo, error) {
	var items map[string]PathNodes
	var pagination PaginationInfo
	var err error

	err = dbInstance.View(func(tx *bolt.Tx) error {
		// DIAGNOSTIC: Inspect root paths when querying root
		if path == "/" {
			DebugInspectRootPathsInTx(ctx, logger, tx)
		}

		items, pagination, err = getChildrenDiffsFromTx(ctx, logger, tx, path, offset, limit, foldersOnly)
		return err
	})

	if err != nil {
		return map[string]PathNodes{}, PaginationInfo{}, fmt.Errorf("failed to read children diffs: %w", err)
	}

	return items, pagination, nil
}

// getChildrenDiffsFromTx implements the core logic for retrieving and merging children from SRC and DST
// Returns a map where key is the path and value contains src/dst nodes (if they exist)
func getChildrenDiffsFromTx(ctx context.Context, logger zerolog.Logger, tx *bolt.Tx, path string, offset, limit int, foldersOnly bool) (map[string]PathNodes, PaginationInfo, error) {
	// Check for context cancellation
	if ctx.Err() != nil {
		return map[string]PathNodes{}, PaginationInfo{}, ctx.Err()
	}

	// Hash the parent path to get the key for children bucket
	// Use Migration Engine SDK's HashPath to ensure consistency with bucket keys
	parentHashHex := db.HashPath(path)
	parentHash := []byte(parentHashHex)

	// Collect all unique child path hashes from both SRC and DST
	// Use byte slices as keys for pathHash comparison
	childHashesMap := make(map[string]bool) // pathHash hex string -> exists

	// Get children from SRC queue
	// Children bucket stores JSON array of string hashes (32-char hex strings from HashPath)
	// Key: parentHash (as []byte - ASCII bytes of 32-char hex string)
	// Value: JSON([]string) where each string is a 32-char hex hash of child paths
	srcChildrenBucket := db.GetChildrenBucket(tx, "SRC")
	if srcChildrenBucket != nil {
		childrenJSON := srcChildrenBucket.Get(parentHash)
		if childrenJSON != nil {
			var childHashes []string // Children are stored as []string (hex strings)
			if err := json.Unmarshal(childrenJSON, &childHashes); err != nil {
				previewLen := len(childrenJSON)
				if previewLen > 100 {
					previewLen = 100
				}
				logger.Warn().Err(err).Str("path", path).Str("queue", "SRC").Str("json_preview", string(childrenJSON[:previewLen])).Msg("failed to unmarshal children JSON")
			} else {
				for _, hashHex := range childHashes {
					// hashHex is already a hex string (32 characters from HashPath)
					childHashesMap[hashHex] = true
				}
			}
		}
	}

	// Get children from DST queue
	dstChildrenBucket := db.GetChildrenBucket(tx, "DST")
	if dstChildrenBucket != nil {
		childrenJSON := dstChildrenBucket.Get(parentHash)
		if childrenJSON != nil {
			var childHashes []string // Children are stored as []string (hex strings)
			if err := json.Unmarshal(childrenJSON, &childHashes); err != nil {
				previewLen := len(childrenJSON)
				if previewLen > 100 {
					previewLen = 100
				}
				logger.Warn().Err(err).Str("path", path).Str("queue", "DST").Str("json_preview", string(childrenJSON[:previewLen])).Msg("failed to unmarshal children JSON")
			} else {
				for _, hashHex := range childHashes {
					// hashHex is already a hex string (32 characters from HashPath)
					childHashesMap[hashHex] = true
				}
			}
		}
	}

	if len(childHashesMap) == 0 {
		return map[string]PathNodes{}, PaginationInfo{
			Offset:       offset,
			Limit:        limit,
			Total:        0,
			TotalFolders: 0,
			TotalFiles:   0,
			HasMore:      false,
		}, nil
	}

	// Group nodes by path - each path can have SRC node, DST node, or both
	pathItemsMap := make(map[string]PathNodes) // path -> {src?: {...}, dst?: {...}}

	// Get nodes buckets for both queues
	srcNodesBucket := db.GetNodesBucket(tx, "SRC")
	dstNodesBucket := db.GetNodesBucket(tx, "DST")

	for pathHashHex := range childHashesMap {
		// pathHashHex is a 32-character hex string (from HashPath)
		// The nodes bucket stores keys as ASCII bytes of the hex string, not decoded bytes
		// So we need to convert the hex string directly to []byte (ASCII representation)
		pathHash := []byte(pathHashHex)

		var srcNodeState *db.NodeState
		var dstNodeState *db.NodeState

		// Query SRC nodes bucket with child hash key
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

		// Query DST nodes bucket with child hash key (same key in both buckets)
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

		// Skip if no node state found for this hash
		if srcNodeState == nil && dstNodeState == nil {
			continue
		}

		// Build PathNodes with src and/or dst nodes
		// Determine display name from whichever node exists (they should be the same for the same logical item)
		var displayName string
		var srcItem *PathNodeItem
		var dstItem *PathNodeItem

		if srcNodeState != nil {
			srcStatus := getStatusFromLookup(tx, "SRC", srcNodeState.Depth, srcNodeState.Path)
			displayName = filepath.Base(srcNodeState.Path)
			if displayName == "" || displayName == "." || displayName == "/" {
				displayName = srcNodeState.Path
			}

			srcItem = &PathNodeItem{
				Queue:           "SRC",
				Id:              srcNodeState.ID,
				ParentId:        srcNodeState.ParentID,
				ParentPath:      srcNodeState.ParentPath,
				DisplayName:     displayName,
				LocationPath:    srcNodeState.Path,
				LastUpdated:     time.Now().Format(time.RFC3339), // NodeState doesn't store LastUpdated
				DepthLevel:      srcNodeState.Depth,
				Type:            srcNodeState.Type,
				TraversalStatus: srcStatus,
			}

			if srcNodeState.Type == "file" {
				srcItem.Size = srcNodeState.Size
			}
		}

		if dstNodeState != nil {
			// If displayName not set from src, get it from dst
			if displayName == "" {
				displayName = filepath.Base(dstNodeState.Path)
				if displayName == "" || displayName == "." || displayName == "/" {
					displayName = dstNodeState.Path
				}
			}

			dstStatus := getStatusFromLookup(tx, "DST", dstNodeState.Depth, dstNodeState.Path)

			dstItem = &PathNodeItem{
				Queue:           "DST",
				Id:              dstNodeState.ID,
				ParentId:        dstNodeState.ParentID,
				ParentPath:      dstNodeState.ParentPath,
				DisplayName:     displayName, // Use the same display name
				LocationPath:    dstNodeState.Path,
				LastUpdated:     time.Now().Format(time.RFC3339), // NodeState doesn't store LastUpdated
				DepthLevel:      dstNodeState.Depth,
				Type:            dstNodeState.Type,
				TraversalStatus: dstStatus,
			}

			if dstNodeState.Type == "file" {
				dstItem.Size = dstNodeState.Size
			}
		}

		// Add to map using display name as key (at least one node should exist since we check above)
		if displayName != "" {
			pathItemsMap[displayName] = PathNodes{
				Src: srcItem,
				Dst: dstItem,
			}
		}
	}

	// Separate folders and files for counting and pagination
	type pathWithType struct {
		path  string
		nodes PathNodes
		typ   string // "folder" or "file"
	}

	allPaths := make([]pathWithType, 0, len(pathItemsMap))
	for path, nodes := range pathItemsMap {
		// Determine type from src node if available, otherwise dst
		var typ string
		if nodes.Src != nil {
			typ = nodes.Src.Type
		} else if nodes.Dst != nil {
			typ = nodes.Dst.Type
		} else {
			continue // Skip if neither exists (shouldn't happen)
		}

		allPaths = append(allPaths, pathWithType{
			path:  path,
			nodes: nodes,
			typ:   typ,
		})
	}

	// Sort by path (which naturally groups folders and files)
	sort.Slice(allPaths, func(i, j int) bool {
		return allPaths[i].path < allPaths[j].path
	})

	// Count folders and files
	totalFolders := 0
	totalFiles := 0
	for _, pwt := range allPaths {
		if pwt.typ == "folder" {
			totalFolders++
		} else {
			totalFiles++
		}
	}

	// Apply foldersOnly filter if requested
	var pathsToPaginate []pathWithType
	var totalItems int

	if foldersOnly {
		pathsToPaginate = make([]pathWithType, 0, totalFolders)
		for _, pwt := range allPaths {
			if pwt.typ == "folder" {
				pathsToPaginate = append(pathsToPaginate, pwt)
			}
		}
		totalItems = totalFolders
	} else {
		pathsToPaginate = allPaths
		totalItems = totalFolders + totalFiles
	}

	// Apply pagination
	startIdx := offset
	endIdx := offset + limit
	if startIdx > len(pathsToPaginate) {
		startIdx = len(pathsToPaginate)
	}
	if endIdx > len(pathsToPaginate) {
		endIdx = len(pathsToPaginate)
	}

	var paginatedPaths []pathWithType
	if startIdx < endIdx {
		paginatedPaths = pathsToPaginate[startIdx:endIdx]
	} else {
		paginatedPaths = []pathWithType{}
	}

	// Build result map with paginated items
	resultMap := make(map[string]PathNodes)
	for _, pwt := range paginatedPaths {
		resultMap[pwt.path] = pwt.nodes
	}

	pagination := PaginationInfo{
		Offset:       offset,
		Limit:        limit,
		Total:        totalItems,
		TotalFolders: totalFolders,
		TotalFiles:   totalFiles,
		HasMore:      endIdx < len(pathsToPaginate),
	}

	return resultMap, pagination, nil
}

// getStatusFromLookupByHash retrieves the traversal status from the status-lookup index using a path hash directly
// Manually navigates to /Traversal-Data/{queueType}/levels/{formattedLevel}/status-lookup
func getStatusFromLookupByHash(tx *bolt.Tx, queueType string, level int, pathHash []byte) string {
	formattedLevel := formatLevel(level)

	// Navigate to /Traversal-Data/{queueType}/levels/{formattedLevel}/status-lookup
	traversalDataBucket := tx.Bucket([]byte("Traversal-Data"))
	if traversalDataBucket == nil {
		return ""
	}

	queueBucket := traversalDataBucket.Bucket([]byte(queueType))
	if queueBucket == nil {
		return ""
	}

	levelsBucket := queueBucket.Bucket([]byte("levels"))
	if levelsBucket == nil {
		return ""
	}

	levelBucket := levelsBucket.Bucket([]byte(formattedLevel))
	if levelBucket == nil {
		return ""
	}

	statusLookupBucket := levelBucket.Bucket([]byte("status-lookup"))
	if statusLookupBucket == nil {
		return ""
	}

	statusBytes := statusLookupBucket.Get(pathHash)
	if statusBytes == nil {
		return ""
	}

	return string(statusBytes)
}

// getStatusFromLookup retrieves the traversal status from the status-lookup index
// Manually navigates to /Traversal-Data/{queueType}/levels/{formattedLevel}/status-lookup
func getStatusFromLookup(tx *bolt.Tx, queueType string, level int, path string) string {
	// Use Migration Engine SDK's HashPath to ensure consistency with bucket keys
	pathHash := []byte(db.HashPath(path))
	return getStatusFromLookupByHash(tx, queueType, level, pathHash)
}

// DebugInspectRootPathsInTx is a diagnostic function to inspect what root paths are actually stored in the database
// This helps debug path normalization issues where the Migration Engine might store absolute paths instead of "/"
func DebugInspectRootPathsInTx(ctx context.Context, logger zerolog.Logger, tx *bolt.Tx) {
	srcNodesBucket := db.GetNodesBucket(tx, "SRC")
	dstNodesBucket := db.GetNodesBucket(tx, "DST")

	logger.Debug().Msg("DEBUG: Inspecting root paths in database...")

	// Check SRC nodes - find nodes with depth 0 (should be root)
	if srcNodesBucket != nil {
		cursor := srcNodesBucket.Cursor()
		rootCount := 0
		for key, value := cursor.First(); key != nil; key, value = cursor.Next() {
			var nodeState db.NodeState
			if err := json.Unmarshal(value, &nodeState); err == nil {
				if nodeState.Depth == 0 {
					rootCount++
					// Use Migration Engine SDK's HashPath for consistency
					expectedHash := string(db.HashPath("/"))
					actualHash := string(key)
					pathHashFromDB := string(db.HashPath(nodeState.Path))

					logger.Debug().
						Str("queue", "SRC").
						Str("root_path", nodeState.Path).
						Str("root_hash_from_path", pathHashFromDB).
						Str("expected_root_hash", expectedHash).
						Str("actual_bucket_key", actualHash).
						Bool("matches_expected", actualHash == expectedHash).
						Bool("matches_path_hash", actualHash == pathHashFromDB).
						Msg("DEBUG: Found SRC root node")
				}
			}
		}
		if rootCount == 0 {
			logger.Debug().Msg("DEBUG: No root nodes (depth=0) found in SRC bucket")
		}
	}

	// Check DST nodes - find nodes with depth 0
	if dstNodesBucket != nil {
		cursor := dstNodesBucket.Cursor()
		rootCount := 0
		for key, value := cursor.First(); key != nil; key, value = cursor.Next() {
			var nodeState db.NodeState
			if err := json.Unmarshal(value, &nodeState); err == nil {
				if nodeState.Depth == 0 {
					rootCount++
					// Use Migration Engine SDK's HashPath for consistency
					expectedHash := string(db.HashPath("/"))
					actualHash := string(key)
					pathHashFromDB := string(db.HashPath(nodeState.Path))

					logger.Debug().
						Str("queue", "DST").
						Str("root_path", nodeState.Path).
						Str("root_hash_from_path", pathHashFromDB).
						Str("expected_root_hash", expectedHash).
						Str("actual_bucket_key", actualHash).
						Bool("matches_expected", actualHash == expectedHash).
						Bool("matches_path_hash", actualHash == pathHashFromDB).
						Msg("DEBUG: Found DST root node")
				}
			}
		}
		if rootCount == 0 {
			logger.Debug().Msg("DEBUG: No root nodes (depth=0) found in DST bucket")
		}
	}
}

// NodeInfo represents information about a node found by ID
type NodeInfo struct {
	QueueType string // "SRC" or "DST"
	NodeState *db.NodeState
	Path      string
}

// FindNodeByID searches for a node by ID in both SRC and DST nodes buckets
// Returns the first matching node found (checking SRC first, then DST)
func FindNodeByID(ctx context.Context, logger zerolog.Logger, dbInstance *db.DB, nodeID string) (*NodeInfo, error) {
	var result *NodeInfo

	err := dbInstance.View(func(tx *bolt.Tx) error {
		// Check SRC nodes bucket first
		srcNodesBucket := db.GetNodesBucket(tx, "SRC")
		if srcNodesBucket != nil {
			cursor := srcNodesBucket.Cursor()
			for key, value := cursor.First(); key != nil; key, value = cursor.Next() {
				if ctx.Err() != nil {
					return ctx.Err()
				}

				var nodeState db.NodeState
				if err := json.Unmarshal(value, &nodeState); err != nil {
					continue // Skip invalid entries
				}

				if nodeState.ID == nodeID {
					result = &NodeInfo{
						QueueType: "SRC",
						NodeState: &nodeState,
						Path:      nodeState.Path,
					}
					return nil // Found in SRC
				}
			}
		}

		// Check DST nodes bucket if not found in SRC
		dstNodesBucket := db.GetNodesBucket(tx, "DST")
		if dstNodesBucket != nil {
			cursor := dstNodesBucket.Cursor()
			for key, value := cursor.First(); key != nil; key, value = cursor.Next() {
				if ctx.Err() != nil {
					return ctx.Err()
				}

				var nodeState db.NodeState
				if err := json.Unmarshal(value, &nodeState); err != nil {
					continue // Skip invalid entries
				}

				if nodeState.ID == nodeID {
					result = &NodeInfo{
						QueueType: "DST",
						NodeState: &nodeState,
						Path:      nodeState.Path,
					}
					return nil // Found in DST
				}
			}
		}

		return nil // Not found
	})

	if err != nil {
		return nil, fmt.Errorf("failed to search for node by ID: %w", err)
	}

	return result, nil
}

// GetDirectChildrenHashes retrieves the direct children hash strings for a given parent path
// Returns a map of path hash hex string -> true (for easy lookup)
func GetDirectChildrenHashes(tx *bolt.Tx, queueType string, parentPath string) (map[string]bool, error) {
	childrenMap := make(map[string]bool)
	// Use Migration Engine SDK's HashPath to ensure consistency with bucket keys
	parentHashHex := db.HashPath(parentPath)
	parentHash := []byte(parentHashHex)

	// Get children from the specified queue
	childrenBucket := db.GetChildrenBucket(tx, queueType)
	if childrenBucket != nil {
		childrenJSON := childrenBucket.Get(parentHash)
		if childrenJSON != nil {
			var childHashes []string // Children are stored as []string (hex strings)
			if err := json.Unmarshal(childrenJSON, &childHashes); err != nil {
				return nil, fmt.Errorf("failed to unmarshal children JSON for queue %s, path %s: %w", queueType, parentPath, err)
			}
			for _, hashHex := range childHashes {
				childrenMap[hashHex] = true
			}
		}
	}

	return childrenMap, nil
}

// GetChildNodeStates retrieves NodeState objects for child hash strings
// Returns a slice of NodeState objects with their queue types
func GetChildNodeStates(tx *bolt.Tx, childHashes map[string]bool) ([]struct {
	QueueType string
	NodeState *db.NodeState
}, error) {
	var result []struct {
		QueueType string
		NodeState *db.NodeState
	}

	srcNodesBucket := db.GetNodesBucket(tx, "SRC")
	dstNodesBucket := db.GetNodesBucket(tx, "DST")

	for pathHashHex := range childHashes {
		pathHash := []byte(pathHashHex) // Convert hex string to ASCII bytes

		// Check SRC first
		if srcNodesBucket != nil {
			nodeData := srcNodesBucket.Get(pathHash)
			if nodeData != nil {
				var nodeState db.NodeState
				if err := json.Unmarshal(nodeData, &nodeState); err == nil {
					result = append(result, struct {
						QueueType string
						NodeState *db.NodeState
					}{
						QueueType: "SRC",
						NodeState: &nodeState,
					})
					continue // Found in SRC, skip DST check
				}
			}
		}

		// Check DST if not found in SRC
		if dstNodesBucket != nil {
			nodeData := dstNodesBucket.Get(pathHash)
			if nodeData != nil {
				var nodeState db.NodeState
				if err := json.Unmarshal(nodeData, &nodeState); err == nil {
					result = append(result, struct {
						QueueType string
						NodeState *db.NodeState
					}{
						QueueType: "DST",
						NodeState: &nodeState,
					})
				}
			}
		}
	}

	return result, nil
}

// GetOrCreateExclusionFlagsBucket gets or creates the exclusion-flags bucket
// This bucket stores node path hash -> explicit_excluded flag (as JSON bool)
func GetOrCreateExclusionFlagsBucket(tx *bolt.Tx) (*bolt.Bucket, error) {
	traversalDataBucket := tx.Bucket([]byte("Traversal-Data"))
	if traversalDataBucket == nil {
		return nil, fmt.Errorf("Traversal-Data bucket not found")
	}

	exclusionFlagsBucket, err := traversalDataBucket.CreateBucketIfNotExists([]byte("exclusion-flags"))
	if err != nil {
		return nil, fmt.Errorf("failed to create exclusion-flags bucket: %w", err)
	}

	return exclusionFlagsBucket, nil
}

// SetNodeExclusionFlag sets the explicit_excluded flag for a node
func SetNodeExclusionFlag(tx *bolt.Tx, nodePath string, excluded bool) error {
	exclusionFlagsBucket, err := GetOrCreateExclusionFlagsBucket(tx)
	if err != nil {
		return err
	}

	// Use Migration Engine SDK's HashPath to ensure consistency with bucket keys
	pathHash := []byte(db.HashPath(nodePath))
	flagJSON, err := json.Marshal(excluded)
	if err != nil {
		return fmt.Errorf("failed to marshal exclusion flag: %w", err)
	}

	return exclusionFlagsBucket.Put(pathHash, flagJSON)
}

// GetNodeExclusionFlag retrieves the explicit_excluded flag for a node
// Returns false if not set (default)
func GetNodeExclusionFlag(tx *bolt.Tx, nodePath string) (bool, error) {
	exclusionFlagsBucket, err := GetOrCreateExclusionFlagsBucket(tx)
	if err != nil {
		return false, err
	}

	// Use Migration Engine SDK's HashPath to ensure consistency with bucket keys
	pathHash := []byte(db.HashPath(nodePath))
	flagJSON := exclusionFlagsBucket.Get(pathHash)
	if flagJSON == nil {
		return false, nil // Default to not excluded
	}

	var excluded bool
	if err := json.Unmarshal(flagJSON, &excluded); err != nil {
		return false, fmt.Errorf("failed to unmarshal exclusion flag: %w", err)
	}

	return excluded, nil
}

// getOrCreateExclusionHoldingBucket gets or creates the exclusion-holding bucket for a queue type
// Path: Traversal-Data/{queueType}/exclusion-holding
func getOrCreateExclusionHoldingBucket(tx *bolt.Tx, queueType string) (*bolt.Bucket, error) {
	traversalDataBucket := tx.Bucket([]byte("Traversal-Data"))
	if traversalDataBucket == nil {
		return nil, fmt.Errorf("Traversal-Data bucket not found")
	}

	queueBucket, err := traversalDataBucket.CreateBucketIfNotExists([]byte(queueType))
	if err != nil {
		return nil, fmt.Errorf("failed to create/get %s queue bucket: %w", queueType, err)
	}

	holdingBucket, err := queueBucket.CreateBucketIfNotExists([]byte("exclusion-holding"))
	if err != nil {
		return nil, fmt.Errorf("failed to create/get exclusion-holding bucket for %s: %w", queueType, err)
	}

	return holdingBucket, nil
}

// QueueChildrenInExclusionHolding queues children in the exclusion-holding bucket for propagation
// This is an atomic operation that queues all children for a given parent node
func QueueChildrenInExclusionHolding(tx *bolt.Tx, queueType string, childStates []struct {
	QueueType string
	NodeState *db.NodeState
}) error {
	// Group children by queue type to minimize bucket lookups
	childrenByQueue := make(map[string][]struct {
		QueueType string
		NodeState *db.NodeState
	})

	for _, child := range childStates {
		childrenByQueue[child.QueueType] = append(childrenByQueue[child.QueueType], child)
	}

	// Queue children for each queue type
	for childQueueType, children := range childrenByQueue {
		// Get or create the exclusion-holding bucket for the child's queue type
		holdingBucket, err := getOrCreateExclusionHoldingBucket(tx, childQueueType)
		if err != nil {
			return fmt.Errorf("failed to get/create exclusion-holding bucket for %s: %w", childQueueType, err)
		}

		// Store each child in the holding bucket
		for _, child := range children {
			// Calculate path hash using Migration Engine's HashPath
			pathHash := []byte(db.HashPath(child.NodeState.Path))

			// Convert depth to bytes (8-byte big-endian int64)
			depthBytes := make([]byte, 8)
			binary.BigEndian.PutUint64(depthBytes, uint64(child.NodeState.Depth))

			// Store in holding bucket
			if err := holdingBucket.Put(pathHash, depthBytes); err != nil {
				return fmt.Errorf("failed to queue child in exclusion-holding bucket: %w", err)
			}
		}
	}

	return nil
}

// SetNodeExclusion performs the complete exclusion/unexclusion operation atomically:
// 1. Updates the node's explicit_excluded flag
// 2. Adds the node itself to the appropriate holding bucket (exclusion-holding or unexclusion-holding)
// nodeID is expected to be a path hash hex string (from db.HashPath)
// Per SDK docs: The exclusion sweep will propagate the flag to all descendants
func SetNodeExclusion(ctx context.Context, logger zerolog.Logger, dbInstance *db.DB, nodeID string, excluded bool) error {
	return dbInstance.Update(func(tx *bolt.Tx) error {
		// Step 1: Find the node by path hash (same pattern as ListChildrenDiffs)
		// nodeID is a path hash hex string (32 characters from db.HashPath)
		// Convert to []byte to get the bucket key (ASCII bytes of hex string)
		pathHash := []byte(nodeID)

		var nodeInfo *NodeInfo
		srcNodesBucket := db.GetNodesBucket(tx, "SRC")
		dstNodesBucket := db.GetNodesBucket(tx, "DST")

		// Query SRC nodes bucket directly (prefer SRC if both exist)
		if srcNodesBucket != nil {
			nodeData := srcNodesBucket.Get(pathHash)
			if nodeData != nil {
				var nodeState db.NodeState
				if err := json.Unmarshal(nodeData, &nodeState); err == nil {
					nodeInfo = &NodeInfo{
						QueueType: "SRC",
						NodeState: &nodeState,
						Path:      nodeState.Path,
					}
				}
			}
		}

		// Query DST nodes bucket if not found in SRC
		if nodeInfo == nil && dstNodesBucket != nil {
			nodeData := dstNodesBucket.Get(pathHash)
			if nodeData != nil {
				var nodeState db.NodeState
				if err := json.Unmarshal(nodeData, &nodeState); err == nil {
					nodeInfo = &NodeInfo{
						QueueType: "DST",
						NodeState: &nodeState,
						Path:      nodeState.Path,
					}
				}
			}
		}

		if nodeInfo == nil {
			return fmt.Errorf("node with path hash %s not found", nodeID)
		}

		// Step 2: Set the exclusion flag
		if err := SetNodeExclusionFlag(tx, nodeInfo.Path, excluded); err != nil {
			return fmt.Errorf("failed to set exclusion flag: %w", err)
		}

		// Step 3: Add the node itself to the appropriate holding bucket
		// Per SDK docs: Add the node to exclusion-holding or unexclusion-holding bucket
		// The SDK's exclusion sweep will then propagate the flag to all descendants
		bucketName := "exclusion-holding"
		if !excluded {
			bucketName = "unexclusion-holding"
		}

		// Get or create the appropriate holding bucket for the node's queue type
		traversalDataBucket := tx.Bucket([]byte("Traversal-Data"))
		if traversalDataBucket == nil {
			return fmt.Errorf("Traversal-Data bucket not found")
		}

		queueBucket, err := traversalDataBucket.CreateBucketIfNotExists([]byte(nodeInfo.QueueType))
		if err != nil {
			return fmt.Errorf("failed to create/get %s queue bucket: %w", nodeInfo.QueueType, err)
		}

		holdingBucket, err := queueBucket.CreateBucketIfNotExists([]byte(bucketName))
		if err != nil {
			return fmt.Errorf("failed to create/get %s bucket for %s: %w", bucketName, nodeInfo.QueueType, err)
		}

		// Store path hash -> depth in the holding bucket
		// pathHash is already []byte (the bucket key)
		// depth as 8-byte big-endian int64 (same format as SDK uses)
		depthBytes := make([]byte, 8)
		binary.BigEndian.PutUint64(depthBytes, uint64(nodeInfo.NodeState.Depth))

		if err := holdingBucket.Put(pathHash, depthBytes); err != nil {
			return fmt.Errorf("failed to add node to %s bucket: %w", bucketName, err)
		}

		logger.Info().
			Str("node_id", nodeID).
			Str("queue", nodeInfo.QueueType).
			Str("path", nodeInfo.NodeState.Path).
			Int("depth", nodeInfo.NodeState.Depth).
			Bool("excluded", excluded).
			Str("bucket", bucketName).
			Msg("marked node for exclusion sweep")

		return nil
	})
}

// CheckPendingExclusions checks if there are any pending exclusions in the exclusion-holding buckets
// Returns true if any exclusion-holding bucket (SRC or DST) has entries
func CheckPendingExclusions(ctx context.Context, logger zerolog.Logger, dbInstance *db.DB) (bool, error) {
	count, err := CountPendingExclusions(ctx, logger, dbInstance)
	if err != nil {
		return false, err
	}
	return count > 0, nil
}

// CountPendingExclusions counts the number of pending exclusions in the exclusion-holding buckets
// Returns the total count across both SRC and DST queues
func CountPendingExclusions(ctx context.Context, logger zerolog.Logger, dbInstance *db.DB) (int, error) {
	var count int
	err := dbInstance.View(func(tx *bolt.Tx) error {
		// Count SRC exclusion-holding bucket
		srcHoldingBucket := getExclusionHoldingBucket(tx, "SRC")
		if srcHoldingBucket != nil {
			cursor := srcHoldingBucket.Cursor()
			for key, _ := cursor.First(); key != nil; key, _ = cursor.Next() {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				count++
			}
		}

		// Count DST exclusion-holding bucket
		dstHoldingBucket := getExclusionHoldingBucket(tx, "DST")
		if dstHoldingBucket != nil {
			cursor := dstHoldingBucket.Cursor()
			for key, _ := cursor.First(); key != nil; key, _ = cursor.Next() {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				count++
			}
		}

		return nil
	})

	if err != nil {
		return 0, fmt.Errorf("failed to count pending exclusions: %w", err)
	}

	return count, nil
}

// getExclusionHoldingBucket gets the exclusion-holding bucket for a queue type (read-only)
// Returns nil if bucket doesn't exist
func getExclusionHoldingBucket(tx *bolt.Tx, queueType string) *bolt.Bucket {
	traversalDataBucket := tx.Bucket([]byte("Traversal-Data"))
	if traversalDataBucket == nil {
		return nil
	}

	queueBucket := traversalDataBucket.Bucket([]byte(queueType))
	if queueBucket == nil {
		return nil
	}

	return queueBucket.Bucket([]byte("exclusion-holding"))
}

// CheckPendingRetries checks if there are any pending items in status-lookup buckets
// Returns true if any status-lookup bucket has entries with status "pending"
func CheckPendingRetries(ctx context.Context, logger zerolog.Logger, dbInstance *db.DB) (bool, error) {
	count, err := CountPendingRetries(ctx, logger, dbInstance)
	if err != nil {
		return false, err
	}
	return count > 0, nil
}

// CountPendingRetries counts the number of pending items in status-lookup buckets
// Returns the total count across all levels in both SRC and DST queues
func CountPendingRetries(ctx context.Context, logger zerolog.Logger, dbInstance *db.DB) (int, error) {
	var count int
	err := dbInstance.View(func(tx *bolt.Tx) error {
		traversalDataBucket := tx.Bucket([]byte("Traversal-Data"))
		if traversalDataBucket == nil {
			return nil
		}

		// Check both SRC and DST queues
		for _, queueType := range []string{"SRC", "DST"} {
			queueBucket := traversalDataBucket.Bucket([]byte(queueType))
			if queueBucket == nil {
				continue
			}

			levelsBucket := queueBucket.Bucket([]byte("levels"))
			if levelsBucket == nil {
				continue
			}

			// Iterate through all level buckets
			cursor := levelsBucket.Cursor()
			for levelKey, _ := cursor.First(); levelKey != nil; levelKey, _ = cursor.Next() {
				if ctx.Err() != nil {
					return ctx.Err()
				}

				levelBucket := levelsBucket.Bucket(levelKey)
				if levelBucket == nil {
					continue
				}

				statusLookupBucket := levelBucket.Bucket([]byte("status-lookup"))
				if statusLookupBucket == nil {
					continue
				}

				// Count all entries in status-lookup with "pending" status
				statusCursor := statusLookupBucket.Cursor()
				for key, statusBytes := statusCursor.First(); key != nil; key, statusBytes = statusCursor.Next() {
					if ctx.Err() != nil {
						return ctx.Err()
					}
					if statusBytes != nil && string(statusBytes) == "pending" {
						count++
					}
				}
			}
		}

		return nil
	})

	if err != nil {
		return 0, fmt.Errorf("failed to count pending retries: %w", err)
	}

	return count, nil
}

// MarkNodeForRetry marks a failed node for retry by updating its status from "failed" to "pending"
// Updates multiple buckets:
// 1. Node status in nodes bucket
// 2. Status-lookup bucket entry
// 3. Status buckets (moves from failed to pending)
// 4. Stats counters (decrements failed, increments pending)
// nodeID is expected to be a path hash hex string (from db.HashPath)
func MarkNodeForRetry(ctx context.Context, logger zerolog.Logger, dbInstance *db.DB, nodeID string) error {
	return dbInstance.Update(func(tx *bolt.Tx) error {
		// Step 1: Find the node by path hash (same pattern as ListChildrenDiffs)
		// nodeID is a path hash hex string (32 characters from db.HashPath)
		// Convert to []byte to get the bucket key (ASCII bytes of hex string)
		pathHash := []byte(nodeID)

		var nodeInfo *NodeInfo
		srcNodesBucket := db.GetNodesBucket(tx, "SRC")
		dstNodesBucket := db.GetNodesBucket(tx, "DST")

		// Query SRC nodes bucket directly (prefer SRC if both exist)
		if srcNodesBucket != nil {
			nodeData := srcNodesBucket.Get(pathHash)
			if nodeData != nil {
				var nodeState db.NodeState
				if err := json.Unmarshal(nodeData, &nodeState); err == nil {
					nodeInfo = &NodeInfo{
						QueueType: "SRC",
						NodeState: &nodeState,
						Path:      nodeState.Path,
					}
				}
			}
		}

		// Query DST nodes bucket if not found in SRC
		if nodeInfo == nil && dstNodesBucket != nil {
			nodeData := dstNodesBucket.Get(pathHash)
			if nodeData != nil {
				var nodeState db.NodeState
				if err := json.Unmarshal(nodeData, &nodeState); err == nil {
					nodeInfo = &NodeInfo{
						QueueType: "DST",
						NodeState: &nodeState,
						Path:      nodeState.Path,
					}
				}
			}
		}

		if nodeInfo == nil {
			return fmt.Errorf("node with path hash %s not found", nodeID)
		}

		// Step 2: Verify node is currently "failed"
		// pathHash is already set from Step 1, use it for status lookup
		currentStatus := getStatusFromLookupByHash(tx, nodeInfo.QueueType, nodeInfo.NodeState.Depth, pathHash)
		if currentStatus != "failed" {
			return fmt.Errorf("node is not in failed status (current: %s), cannot mark for retry", currentStatus)
		}
		formattedLevel := formatLevel(nodeInfo.NodeState.Depth)

		// Step 3: Update status-lookup bucket
		traversalDataBucket := tx.Bucket([]byte("Traversal-Data"))
		if traversalDataBucket == nil {
			return fmt.Errorf("Traversal-Data bucket not found")
		}

		queueBucket := traversalDataBucket.Bucket([]byte(nodeInfo.QueueType))
		if queueBucket == nil {
			return fmt.Errorf("%s queue bucket not found", nodeInfo.QueueType)
		}

		levelsBucket := queueBucket.Bucket([]byte("levels"))
		if levelsBucket == nil {
			return fmt.Errorf("levels bucket not found")
		}

		levelBucket := levelsBucket.Bucket([]byte(formattedLevel))
		if levelBucket == nil {
			return fmt.Errorf("level bucket %s not found", formattedLevel)
		}

		statusLookupBucket := levelBucket.Bucket([]byte("status-lookup"))
		if statusLookupBucket == nil {
			return fmt.Errorf("status-lookup bucket not found")
		}

		// Update status-lookup from "failed" to "pending"
		if err := statusLookupBucket.Put(pathHash, []byte("pending")); err != nil {
			return fmt.Errorf("failed to update status-lookup: %w", err)
		}

		// Step 4: Update status buckets (move from failed to pending)
		// Status buckets are at: Traversal-Data/{queueType}/levels/{level}/status/{status}
		statusBucket := levelBucket.Bucket([]byte("status"))
		if statusBucket != nil {
			// Remove from failed bucket
			failedBucket := statusBucket.Bucket([]byte("failed"))
			if failedBucket != nil {
				if err := failedBucket.Delete(pathHash); err != nil {
					logger.Warn().Err(err).Str("node_id", nodeID).Msg("failed to remove from failed status bucket")
					// Continue - not critical
				}
			}

			// Add to pending status bucket
			pendingBucket, err := statusBucket.CreateBucketIfNotExists([]byte("pending"))
			if err != nil {
				return fmt.Errorf("failed to create pending status bucket: %w", err)
			}
			// Add empty value (key is the path hash, value can be empty)
			if err := pendingBucket.Put(pathHash, []byte{}); err != nil {
				return fmt.Errorf("failed to add to pending status bucket: %w", err)
			}
		}

		// Step 5: Update node in nodes bucket (if NodeState has Status field)
		nodesBucket := db.GetNodesBucket(tx, nodeInfo.QueueType)
		if nodesBucket != nil {
			nodeData := nodesBucket.Get(pathHash)
			if nodeData != nil {
				var nodeState db.NodeState
				if err := json.Unmarshal(nodeData, &nodeState); err == nil {
					// Update Status field if it exists (Migration Engine SDK may have this field)
					// For now, the status-lookup is the source of truth, so we may not need to update the node
					// But we'll marshal it back to ensure consistency
					if err := json.Unmarshal(nodeData, &nodeState); err == nil {
						// NodeState struct may not have a Status field directly
						// The status is tracked in status-lookup buckets
						// So we just update the node data back (in case there are other changes needed)
						updatedNodeData, err := json.Marshal(nodeState)
						if err == nil {
							if err := nodesBucket.Put(pathHash, updatedNodeData); err != nil {
								logger.Warn().Err(err).Str("node_id", nodeID).Msg("failed to update node in nodes bucket")
								// Continue - status-lookup is the source of truth
							}
						}
					}
				}
			}
		}

		// Step 6: Update stats counters
		// Stats are in Traversal-Data/STATS/queue-stats or similar
		// We need to decrement failed count and increment pending count
		statsBucket := traversalDataBucket.Bucket([]byte("STATS"))
		if statsBucket != nil {
			queueStatsBucket := statsBucket.Bucket([]byte("queue-stats"))
			if queueStatsBucket != nil {
				queueKey := "src-traversal"
				if nodeInfo.QueueType == "DST" {
					queueKey = "dst-traversal"
				}

				statsJSON := queueStatsBucket.Get([]byte(queueKey))
				if statsJSON != nil {
					var stats QueueObserverMetrics
					if err := json.Unmarshal(statsJSON, &stats); err == nil {
						// Note: QueueObserverMetrics may not have direct failed/pending counts
						// The Migration Engine may track these differently
						// For now, we'll update what we can
						// This might need to be adjusted based on actual stats structure
						updatedStatsJSON, err := json.Marshal(stats)
						if err == nil {
							if err := queueStatsBucket.Put([]byte(queueKey), updatedStatsJSON); err != nil {
								logger.Warn().Err(err).Str("queue", queueKey).Msg("failed to update stats")
								// Continue - not critical for marking retry
							}
						}
					}
				}
			}
		}

		logger.Info().
			Str("node_id", nodeID).
			Str("queue", nodeInfo.QueueType).
			Str("path", nodeInfo.NodeState.Path).
			Int("level", nodeInfo.NodeState.Depth).
			Msg("marked node for retry (failed -> pending)")

		return nil
	})
}

// UnmarkNodeForRetry unmarks a node for retry by updating its status from "pending" back to "failed"
// Updates multiple buckets:
// 1. Node status in nodes bucket
// 2. Status-lookup bucket entry
// 3. Status buckets (moves from pending to failed)
// 4. Stats counters (decrements pending, increments failed)
// nodeID is expected to be a path hash hex string (from db.HashPath)
func UnmarkNodeForRetry(ctx context.Context, logger zerolog.Logger, dbInstance *db.DB, nodeID string) error {
	return dbInstance.Update(func(tx *bolt.Tx) error {
		// Step 1: Find the node by path hash (same pattern as MarkNodeForRetry)
		// nodeID is a path hash hex string (32 characters from db.HashPath)
		// Convert to []byte to get the bucket key (ASCII bytes of hex string)
		pathHash := []byte(nodeID)

		var nodeInfo *NodeInfo
		srcNodesBucket := db.GetNodesBucket(tx, "SRC")
		dstNodesBucket := db.GetNodesBucket(tx, "DST")

		// Query SRC nodes bucket directly (prefer SRC if both exist)
		if srcNodesBucket != nil {
			nodeData := srcNodesBucket.Get(pathHash)
			if nodeData != nil {
				var nodeState db.NodeState
				if err := json.Unmarshal(nodeData, &nodeState); err == nil {
					nodeInfo = &NodeInfo{
						QueueType: "SRC",
						NodeState: &nodeState,
						Path:      nodeState.Path,
					}
				}
			}
		}

		// Query DST nodes bucket if not found in SRC
		if nodeInfo == nil && dstNodesBucket != nil {
			nodeData := dstNodesBucket.Get(pathHash)
			if nodeData != nil {
				var nodeState db.NodeState
				if err := json.Unmarshal(nodeData, &nodeState); err == nil {
					nodeInfo = &NodeInfo{
						QueueType: "DST",
						NodeState: &nodeState,
						Path:      nodeState.Path,
					}
				}
			}
		}

		if nodeInfo == nil {
			return fmt.Errorf("node with path hash %s not found", nodeID)
		}

		// Step 2: Verify node is currently "pending"
		// pathHash is already set from Step 1, use it for status lookup
		currentStatus := getStatusFromLookupByHash(tx, nodeInfo.QueueType, nodeInfo.NodeState.Depth, pathHash)
		if currentStatus != "pending" {
			return fmt.Errorf("node is not in pending status (current: %s), cannot unmark for retry", currentStatus)
		}
		formattedLevel := formatLevel(nodeInfo.NodeState.Depth)

		// Step 3: Update status-lookup bucket
		traversalDataBucket := tx.Bucket([]byte("Traversal-Data"))
		if traversalDataBucket == nil {
			return fmt.Errorf("Traversal-Data bucket not found")
		}

		queueBucket := traversalDataBucket.Bucket([]byte(nodeInfo.QueueType))
		if queueBucket == nil {
			return fmt.Errorf("%s queue bucket not found", nodeInfo.QueueType)
		}

		levelsBucket := queueBucket.Bucket([]byte("levels"))
		if levelsBucket == nil {
			return fmt.Errorf("levels bucket not found")
		}

		levelBucket := levelsBucket.Bucket([]byte(formattedLevel))
		if levelBucket == nil {
			return fmt.Errorf("level bucket %s not found", formattedLevel)
		}

		statusLookupBucket := levelBucket.Bucket([]byte("status-lookup"))
		if statusLookupBucket == nil {
			return fmt.Errorf("status-lookup bucket not found")
		}

		// Update status-lookup from "pending" to "failed"
		if err := statusLookupBucket.Put(pathHash, []byte("failed")); err != nil {
			return fmt.Errorf("failed to update status-lookup: %w", err)
		}

		// Step 4: Update status buckets (move from pending to failed)
		// Status buckets are at: Traversal-Data/{queueType}/levels/{level}/status/{status}
		statusBucket := levelBucket.Bucket([]byte("status"))
		if statusBucket != nil {
			// Remove from pending bucket
			pendingBucket := statusBucket.Bucket([]byte("pending"))
			if pendingBucket != nil {
				if err := pendingBucket.Delete(pathHash); err != nil {
					logger.Warn().Err(err).Str("node_id", nodeID).Msg("failed to remove from pending status bucket")
					// Continue - not critical
				}
			}

			// Add to failed status bucket
			failedBucket, err := statusBucket.CreateBucketIfNotExists([]byte("failed"))
			if err != nil {
				return fmt.Errorf("failed to create failed status bucket: %w", err)
			}
			// Add empty value (key is the path hash, value can be empty)
			if err := failedBucket.Put(pathHash, []byte{}); err != nil {
				return fmt.Errorf("failed to add to failed status bucket: %w", err)
			}
		}

		// Step 5: Update node in nodes bucket (if NodeState has Status field)
		nodesBucket := db.GetNodesBucket(tx, nodeInfo.QueueType)
		if nodesBucket != nil {
			nodeData := nodesBucket.Get(pathHash)
			if nodeData != nil {
				var nodeState db.NodeState
				if err := json.Unmarshal(nodeData, &nodeState); err == nil {
					// NodeState struct may not have a Status field directly
					// The status is tracked in status-lookup buckets
					// So we just update the node data back (in case there are other changes needed)
					updatedNodeData, err := json.Marshal(nodeState)
					if err == nil {
						if err := nodesBucket.Put(pathHash, updatedNodeData); err != nil {
							logger.Warn().Err(err).Str("node_id", nodeID).Msg("failed to update node in nodes bucket")
							// Continue - status-lookup is the source of truth
						}
					}
				}
			}
		}

		// Step 6: Update stats counters
		// Stats are in Traversal-Data/STATS/queue-stats or similar
		// We need to decrement pending count and increment failed count
		statsBucket := traversalDataBucket.Bucket([]byte("STATS"))
		if statsBucket != nil {
			queueStatsBucket := statsBucket.Bucket([]byte("queue-stats"))
			if queueStatsBucket != nil {
				queueKey := "src-traversal"
				if nodeInfo.QueueType == "DST" {
					queueKey = "dst-traversal"
				}

				statsJSON := queueStatsBucket.Get([]byte(queueKey))
				if statsJSON != nil {
					var stats QueueObserverMetrics
					if err := json.Unmarshal(statsJSON, &stats); err == nil {
						// Note: QueueObserverMetrics may not have direct failed/pending counts
						// The Migration Engine may track these differently
						// For now, we'll update what we can
						// This might need to be adjusted based on actual stats structure
						updatedStatsJSON, err := json.Marshal(stats)
						if err == nil {
							if err := queueStatsBucket.Put([]byte(queueKey), updatedStatsJSON); err != nil {
								logger.Warn().Err(err).Str("queue", queueKey).Msg("failed to update stats")
								// Continue - not critical for unmarking retry
							}
						}
					}
				}
			}
		}

		logger.Info().
			Str("node_id", nodeID).
			Str("queue", nodeInfo.QueueType).
			Str("path", nodeInfo.NodeState.Path).
			Int("level", nodeInfo.NodeState.Depth).
			Msg("unmarked node for retry (pending -> failed)")

		return nil
	})
}
