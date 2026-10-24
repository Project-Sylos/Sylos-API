package migrationfiles

import (
	"archive/zip"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"codeberg.org/Sylos/Migration-Engine/pkg/opsdb"
	"github.com/rs/zerolog"
)

// NormalizeMigrationID strips archive / .ops suffixes from a folder- or zip-derived name.
func NormalizeMigrationID(raw string) string {
	id := strings.TrimSpace(raw)
	id = strings.TrimSuffix(id, "/")
	id = strings.TrimSuffix(id, "\\")
	if id == "" {
		return ""
	}
	base := filepath.Base(id)
	if base == "." || base == string(filepath.Separator) {
		return ""
	}
	lower := strings.ToLower(base)
	if strings.HasSuffix(lower, ".zip") {
		base = base[:len(base)-4]
		lower = strings.ToLower(base)
	}
	if strings.HasSuffix(lower, ".ops") {
		base = base[:len(base)-4]
	}
	return base
}

// UploadMigrationOps extracts a zip of Badger ops folder contents into
// dataDir/{migrationID}/{migrationID}.ops/, creating a logical {id}.db marker.
// The source folder is not used in place; contents are copied.
func UploadMigrationOps(ctx context.Context, logger zerolog.Logger, dataDir, migrationID string, zipData []byte, overwrite bool) (UploadMigrationDBResponse, error) {
	_ = ctx
	migrationID = NormalizeMigrationID(migrationID)
	if migrationID == "" {
		return UploadMigrationDBResponse{
			Success: false,
			Error:   "migration ID is required",
		}, nil
	}
	if len(zipData) == 0 {
		return UploadMigrationDBResponse{
			Success: false,
			Error:   "ops zip is empty",
		}, nil
	}

	migrationDir := filepath.Join(dataDir, migrationID)
	opsDir := filepath.Join(migrationDir, migrationID+".ops")
	dbPath := filepath.Join(migrationDir, migrationID+".db")

	if info, err := os.Stat(opsDir); err == nil && info.IsDir() {
		if !overwrite {
			return UploadMigrationDBResponse{
				Success: false,
				Error:   "file already present on API",
			}, nil
		}
		if err := os.RemoveAll(opsDir); err != nil {
			return UploadMigrationDBResponse{
				Success: false,
				Error:   fmt.Sprintf("failed to clear existing ops dir: %v", err),
			}, nil
		}
	}

	if err := os.MkdirAll(opsDir, 0o755); err != nil {
		logger.Error().Err(err).Str("migration_id", migrationID).Msg("failed to create ops directory")
		return UploadMigrationDBResponse{
			Success: false,
			Error:   fmt.Sprintf("failed to create ops directory: %v", err),
		}, nil
	}

	if err := extractZipTree(zipData, opsDir); err != nil {
		_ = os.RemoveAll(opsDir)
		return UploadMigrationDBResponse{
			Success: false,
			Error:   fmt.Sprintf("failed to extract ops zip: %v", err),
		}, nil
	}

	if err := flattenOpsWrapper(opsDir); err != nil {
		_ = os.RemoveAll(opsDir)
		return UploadMigrationDBResponse{
			Success: false,
			Error:   fmt.Sprintf("failed to normalize ops layout: %v", err),
		}, nil
	}

	if !looksLikeBadgerDir(opsDir) {
		_ = os.RemoveAll(opsDir)
		return UploadMigrationDBResponse{
			Success: false,
			Error:   "uploaded archive does not look like a Badger ops directory (missing MANIFEST)",
		}, nil
	}

	if _, err := os.Stat(dbPath); err != nil {
		if err := os.WriteFile(dbPath, nil, 0o644); err != nil {
			_ = os.RemoveAll(opsDir)
			return UploadMigrationDBResponse{
				Success: false,
				Error:   fmt.Sprintf("failed to create db marker: %v", err),
			}, nil
		}
	}

	store, err := opsdb.Open(opsdb.Options{Dir: opsDir})
	if err != nil {
		_ = os.RemoveAll(opsDir)
		_ = os.Remove(dbPath)
		return UploadMigrationDBResponse{
			Success: false,
			Error:   fmt.Sprintf("ops store failed validation: %v", err),
		}, nil
	}
	_ = store.Close()

	logger.Info().
		Str("migration_id", migrationID).
		Str("path", opsDir).
		Int("size", len(zipData)).
		Bool("overwrite", overwrite).
		Msg("uploaded migration ops folder")

	return UploadMigrationDBResponse{
		Success: true,
		Path:    dbPath,
	}, nil
}

func looksLikeBadgerDir(dir string) bool {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return false
	}
	for _, e := range entries {
		name := e.Name()
		if strings.HasPrefix(name, "MANIFEST") {
			return true
		}
	}
	return false
}

// flattenOpsWrapper lifts contents when the zip wrapped files in a single *.ops/ folder.
func flattenOpsWrapper(opsDir string) error {
	if looksLikeBadgerDir(opsDir) {
		return nil
	}
	entries, err := os.ReadDir(opsDir)
	if err != nil {
		return err
	}
	var subdirs []string
	for _, e := range entries {
		if e.Name() == "sylos-export.json" {
			continue
		}
		if e.IsDir() {
			subdirs = append(subdirs, e.Name())
			continue
		}
		// Non-meta file at root that isn't MANIFEST: not a simple wrapper.
		return nil
	}
	if len(subdirs) != 1 {
		return nil
	}
	inner := filepath.Join(opsDir, subdirs[0])
	if !looksLikeBadgerDir(inner) && !strings.HasSuffix(strings.ToLower(subdirs[0]), ".ops") {
		return nil
	}
	innerEntries, err := os.ReadDir(inner)
	if err != nil {
		return err
	}
	for _, e := range innerEntries {
		src := filepath.Join(inner, e.Name())
		dst := filepath.Join(opsDir, e.Name())
		if err := os.Rename(src, dst); err != nil {
			return err
		}
	}
	return os.RemoveAll(inner)
}

func extractZipTree(zipData []byte, destDir string) error {
	tmpZip, err := os.CreateTemp("", "migration-ops-upload-*.zip")
	if err != nil {
		return err
	}
	tmpZipPath := tmpZip.Name()
	defer os.Remove(tmpZipPath)

	if _, err := tmpZip.Write(zipData); err != nil {
		tmpZip.Close()
		return err
	}
	if err := tmpZip.Close(); err != nil {
		return err
	}

	zr, err := zip.OpenReader(tmpZipPath)
	if err != nil {
		return err
	}
	defer zr.Close()

	destAbs, err := filepath.Abs(destDir)
	if err != nil {
		return err
	}

	for _, file := range zr.File {
		name := filepath.Clean(filepath.FromSlash(file.Name))
		if name == "." || name == "" {
			continue
		}
		if strings.HasPrefix(name, ".."+string(os.PathSeparator)) || name == ".." {
			return fmt.Errorf("invalid zip path %q", file.Name)
		}
		target := filepath.Join(destAbs, name)
		rel, err := filepath.Rel(destAbs, target)
		if err != nil || strings.HasPrefix(rel, "..") {
			return fmt.Errorf("zip path escapes destination: %q", file.Name)
		}

		if file.FileInfo().IsDir() {
			if err := os.MkdirAll(target, 0o755); err != nil {
				return err
			}
			continue
		}

		if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
			return err
		}

		rc, err := file.Open()
		if err != nil {
			return fmt.Errorf("open %s: %w", file.Name, err)
		}
		out, err := os.OpenFile(target, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
		if err != nil {
			rc.Close()
			return err
		}
		_, copyErr := io.Copy(out, rc)
		rc.Close()
		closeErr := out.Close()
		if copyErr != nil {
			return copyErr
		}
		if closeErr != nil {
			return closeErr
		}
	}
	return nil
}
