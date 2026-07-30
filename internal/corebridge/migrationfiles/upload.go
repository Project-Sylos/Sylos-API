package migrationfiles

import (
	"archive/zip"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/rs/zerolog"
)

// UploadType is the type of migration data being uploaded
const (
	UploadTypeZip  = "zip"
	UploadTypeDB   = "db"
	UploadTypeYAML = "yaml"
)

// UploadMigrationDBResponse represents the response from uploading migration data
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

// UploadByType uploads migration data based on the given type (zip, db, or yaml)
func UploadByType(ctx context.Context, logger zerolog.Logger, dataDir, migrationID, uploadType string, data []byte, overwrite bool) (UploadMigrationDBResponse, error) {
	switch uploadType {
	case UploadTypeZip:
		return UploadMigrationData(ctx, logger, dataDir, migrationID, data, overwrite)
	case UploadTypeDB:
		return UploadMigrationDB(ctx, logger, dataDir, migrationID, data, overwrite)
	case UploadTypeYAML:
		return UploadMigrationYAML(ctx, logger, dataDir, migrationID, data, overwrite)
	default:
		return UploadMigrationDBResponse{
			Success: false,
			Error:   fmt.Sprintf("invalid upload type %q: must be zip, db, or yaml", uploadType),
		}, nil
	}
}

// UploadMigrationDB uploads a migration database file to the migration-specific folder
// Saves to dataDir/{migrationID}/{migrationID}.db
func UploadMigrationDB(ctx context.Context, logger zerolog.Logger, dataDir, migrationID string, data []byte, overwrite bool) (UploadMigrationDBResponse, error) {
	if migrationID == "" {
		return UploadMigrationDBResponse{
			Success: false,
			Error:   "migration ID is required",
		}, nil
	}

	migrationDir := filepath.Join(dataDir, migrationID)
	dbPath := filepath.Join(migrationDir, migrationID+".db")

	if _, err := os.Stat(dbPath); err == nil {
		if !overwrite {
			return UploadMigrationDBResponse{
				Success: false,
				Error:   "file already present on API",
			}, nil
		}
	}

	if err := os.MkdirAll(migrationDir, 0o755); err != nil {
		logger.Error().Err(err).Str("migration_id", migrationID).Msg("failed to create migration directory")
		return UploadMigrationDBResponse{
			Success: false,
			Error:   fmt.Sprintf("failed to create migration directory: %v", err),
		}, nil
	}

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
// Saves to dataDir/{migrationID}/{migrationID}.yaml
func UploadMigrationYAML(ctx context.Context, logger zerolog.Logger, dataDir, migrationID string, data []byte, overwrite bool) (UploadMigrationDBResponse, error) {
	if migrationID == "" {
		return UploadMigrationDBResponse{
			Success: false,
			Error:   "migration ID is required",
		}, nil
	}

	migrationDir := filepath.Join(dataDir, migrationID)
	yamlPath := filepath.Join(migrationDir, migrationID+".yaml")

	if _, err := os.Stat(yamlPath); err == nil {
		if !overwrite {
			return UploadMigrationDBResponse{
				Success: false,
				Error:   "file already present on API",
			}, nil
		}
	}

	if err := os.MkdirAll(migrationDir, 0o755); err != nil {
		logger.Error().Err(err).Str("migration_id", migrationID).Msg("failed to create migration directory")
		return UploadMigrationDBResponse{
			Success: false,
			Error:   fmt.Sprintf("failed to create migration directory: %v", err),
		}, nil
	}

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

	migrationDir := filepath.Join(dataDir, migrationID)

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

	if _, err := tmpZip.Write(zipData); err != nil {
		return UploadMigrationDBResponse{
			Success: false,
			Error:   fmt.Sprintf("failed to write zip data: %v", err),
		}, nil
	}
	tmpZip.Close()

	zipReader, err := zip.OpenReader(tmpZipPath)
	if err != nil {
		return UploadMigrationDBResponse{
			Success: false,
			Error:   fmt.Sprintf("failed to open zip file: %v", err),
		}, nil
	}
	defer zipReader.Close()

	if err := os.MkdirAll(migrationDir, 0o755); err != nil {
		logger.Error().Err(err).Str("migration_id", migrationID).Msg("failed to create migration directory")
		return UploadMigrationDBResponse{
			Success: false,
			Error:   fmt.Sprintf("failed to create migration directory: %v", err),
		}, nil
	}

	for _, file := range zipReader.File {
		filePath := filepath.Join(migrationDir, filepath.Base(file.Name))

		rc, err := file.Open()
		if err != nil {
			logger.Warn().Err(err).Str("file", file.Name).Msg("failed to open file from zip, skipping")
			continue
		}

		dstFile, err := os.Create(filePath)
		if err != nil {
			rc.Close()
			logger.Warn().Err(err).Str("file", filePath).Msg("failed to create destination file, skipping")
			continue
		}

		_, err = io.Copy(dstFile, rc)
		rc.Close()
		dstFile.Close()

		if err != nil {
			logger.Warn().Err(err).Str("file", filePath).Msg("failed to extract file from zip, skipping")
			continue
		}

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

// ListMigrationDBs scans DataDir for migration folders and lists {migrationID}.db in each.
// Aligns with upload structure: dataDir/{migrationID}/{migrationID}.db
func ListMigrationDBs(ctx context.Context, logger zerolog.Logger, dataDir string) ([]MigrationDBInfo, error) {
	entries, err := os.ReadDir(dataDir)
	if err != nil {
		if os.IsNotExist(err) {
			return []MigrationDBInfo{}, nil
		}
		return []MigrationDBInfo{}, fmt.Errorf("failed to read data directory: %w", err)
	}

	var dbs []MigrationDBInfo
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}

		migrationID := entry.Name()
		dbPath := filepath.Join(dataDir, migrationID, migrationID+".db")

		info, err := os.Stat(dbPath)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			logger.Warn().Err(err).Str("migration_id", migrationID).Msg("failed to stat migration DB, skipping")
			continue
		}

		if info.IsDir() {
			continue
		}

		if !strings.HasSuffix(dbPath, ".db") {
			continue
		}

		dbs = append(dbs, MigrationDBInfo{
			Filename:   migrationID + ".db",
			Path:       dbPath,
			Size:       info.Size(),
			ModifiedAt: info.ModTime(),
		})
	}

	return dbs, nil
}
