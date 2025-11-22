package database

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/Project-Sylos/Migration-Engine/pkg/db"
	"github.com/Project-Sylos/Migration-Engine/pkg/migration"
	"github.com/rs/zerolog"
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
	// Open database
	database, err := db.NewDB(dbPath)
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
