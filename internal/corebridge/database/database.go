package database

import (
	"context"
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

	// Initialize as empty slice (not nil) to ensure JSON serializes as [] instead of null
	dbs := make([]MigrationDBInfo, 0)
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		// Only list .db files
		if !strings.HasSuffix(entry.Name(), ".db") {
			continue
		}

		info, err := entry.Info()
		if err != nil {
			logger.Warn().Err(err).Str("filename", entry.Name()).Msg("failed to get file info")
			continue
		}

		fullPath := filepath.Join(storageDir, entry.Name())
		dbs = append(dbs, MigrationDBInfo{
			Filename:   entry.Name(),
			Path:       fullPath,
			Size:       info.Size(),
			ModifiedAt: info.ModTime(),
		})
	}

	// Sort by modified time (newest first)
	sort.Slice(dbs, func(i, j int) bool {
		return dbs[i].ModifiedAt.After(dbs[j].ModifiedAt)
	})

	// Ensure we always return a non-nil slice (even if empty)
	if dbs == nil {
		return []MigrationDBInfo{}, nil
	}

	return dbs, nil
}

func ResolveDatabasePath(dataDir, path, migrationID string) (string, error) {
	if path == "" {
		filename := fmt.Sprintf("%s.db", migrationID)
		path = filepath.Join(dataDir, filename)
	}

	abs, err := filepath.Abs(path)
	if err != nil {
		return "", fmt.Errorf("failed to resolve database path: %w", err)
	}

	if err := os.MkdirAll(filepath.Dir(abs), 0o755); err != nil {
		return "", fmt.Errorf("failed to create database directory: %w", err)
	}

	return filepath.Clean(abs), nil
}

// ConfigPathFromDatabasePath returns the config YAML path for a given database path.
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

	// Query queue stats from /STATS/queue-stats bucket
	err = boltDB.View(func(tx *bolt.Tx) error {
		statsBucket := tx.Bucket([]byte("STATS"))
		if statsBucket == nil {
			// STATS bucket doesn't exist - migration observer hasn't started yet
			return nil
		}

		queueStatsBucket := statsBucket.Bucket([]byte("queue-stats"))
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

	// Query queue stats from /STATS/queue-stats bucket using the db.DB instance's View method
	err := dbInstance.View(func(tx *bolt.Tx) error {
		statsBucket := tx.Bucket([]byte("STATS"))
		if statsBucket == nil {
			// STATS bucket doesn't exist - migration observer hasn't started yet
			return nil
		}

		queueStatsBucket := statsBucket.Bucket([]byte("queue-stats"))
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
