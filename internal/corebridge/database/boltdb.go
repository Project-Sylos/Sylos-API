package database

import (
	"archive/zip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
	"codeberg.org/Sylos/Migration-Engine/pkg/migration"
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
	dbPathMinusDB := strings.TrimSuffix(dbPath, ".db")
	return dbPathMinusDB + ".yaml"
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

// ExternalQueueMetrics contains user-facing metrics published to BoltDB for API access.
// This struct supports both traversal metrics and copy phase metrics.
// Fields will be populated based on which phase is active.
// The JSON from BoltDB will have different fields depending on the phase.
type ExternalQueueMetrics struct {
	// Traversal phase metrics
	FilesDiscoveredTotal     int64   `json:"files_discovered_total,omitempty"`
	FoldersDiscoveredTotal   int64   `json:"folders_discovered_total,omitempty"`
	DiscoveryRateItemsPerSec float64 `json:"discovery_rate_items_per_sec,omitempty"`
	TotalDiscovered          int64   `json:"total_discovered,omitempty"` // files + folders

	// Copy phase metrics (new format from engine - published every ~200ms)
	Folders        int64   `json:"folders,omitempty"`          // Total folders created
	Files          int64   `json:"files,omitempty"`            // Total files created
	Total          int64   `json:"total,omitempty"`            // Total items (folders + files)
	Bytes          int64   `json:"bytes,omitempty"`            // Total bytes transferred
	ItemsPerSecond float64 `json:"items_per_second,omitempty"` // Combined items/sec (EMA-smoothed, ~5 second window)
	BytesPerSecond float64 `json:"bytes_per_second,omitempty"` // Bytes/sec transfer rate (EMA-smoothed, ~5 second window)

	// Common state fields (used by both phases)
	// Note: JSON field names match engine output (snake_case for copy, camelCase for traversal)
	Round        int    `json:"round"`
	Pending      int    `json:"pending"`     // Also available as "pending" in copy phase JSON
	InProgress   int    `json:"in_progress"` // Copy phase uses "in_progress", traversal may use "inProgress"
	Workers      int    `json:"workers"`
	TotalPending int    `json:"total_pending,omitempty"` // Total pending from DB (copy phase only)
	TotalFailed  int    `json:"total_failed,omitempty"`  // Total failed from DB (copy phase only)
	Name         string `json:"name,omitempty"`          // Queue name ("copy", "src-traversal", "dst-traversal")
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

			var metrics ExternalQueueMetrics
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

			var metrics ExternalQueueMetrics
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
	ID    string         `json:"id"`
	Level string         `json:"level"`
	Data  map[string]any `json:"data"`
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
				var logData map[string]any
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
				var logData map[string]any
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
	Name            string `json:"name"`
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

// PathNodes represents the src and dst nodes for a given path
type PathNodes struct {
	Src *PathNodeItem `json:"src,omitempty"` // SRC node if exists
	Dst *PathNodeItem `json:"dst,omitempty"` // DST node if exists
}

// Removed: GetChildrenDiffsFromDB, GetChildrenDiffsFromDBInstance, getChildrenDiffsFromTx, getStatusFromLookupByULID, formatLevel
// These functions used BoltDB and have been replaced by DuckDB equivalents in duckdb_operations.go

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

// Removed: FindNodeByID, GetChildNodeStates, GetOrCreateExclusionFlagsBucket,
// SetNodeExclusionFlag, GetNodeExclusionFlag, getOrCreateExclusionHoldingBucket,
// QueueChildrenInExclusionHolding, SetNodeExclusion, getExclusionHoldingBucket,
// CheckPendingRetries, CountPendingRetries
// These functions used BoltDB and have been replaced by DuckDB equivalents in duckdb_operations.go
