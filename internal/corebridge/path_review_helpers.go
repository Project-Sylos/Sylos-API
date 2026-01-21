package corebridge

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"codeberg.org/Sylos/Migration-Engine/pkg/migration"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge/database"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge/metadata"
)

// PathReviewContext holds the context needed for path review operations
type PathReviewContext struct {
	MigrationID string
	DBPath      string
	DuckDBPath  string
	DuckDBConn  *sql.DB
	ReviewPhase string // "traversal" or "copy" - indicates which phase we're reviewing
}

// preparePathReviewContext prepares the context for path review operations
// It gets migration metadata, derives database paths, checks DuckDB availability,
// and opens the DuckDB connection if needed.
func (m *Manager) preparePathReviewContext(ctx context.Context, migrationID string) (*PathReviewContext, error) {
	// Get migration metadata
	metaMgr := metadata.NewManager(m.cfg.Runtime.DataDir)
	meta, err := metaMgr.GetMigrationMetadata(migrationID)
	if err != nil {
		return nil, ErrMigrationNotFound
	}

	// Derive database path from config path
	dbPath := strings.TrimSuffix(meta.ConfigPath, ".yaml") + ".db"
	if dbPath == ".db" {
		dbPath, err = database.ResolveDatabasePath(m.cfg.Runtime.DataDir, "", migrationID)
		if err != nil {
			return nil, fmt.Errorf("failed to resolve database path: %w", err)
		}
	}

	// Check if DuckDB is available (status is Awaiting-Path-Review or Awaiting-Copy-Review)
	useDuckDB := false
	var reviewPhase string // "traversal" or "copy"
	if meta.ConfigPath != "" {
		yamlCfg, err := migration.LoadMigrationConfig(meta.ConfigPath)
		if err == nil {
			status := strings.TrimSpace(yamlCfg.State.Status)
			if status == "Awaiting-Path-Review" {
				useDuckDB = true
				reviewPhase = "traversal"
			} else if status == "Awaiting-Copy-Review" {
				useDuckDB = true
				reviewPhase = "copy"
			} else if status == "Preparing-Path-Review" {
				// Ensure ETL is running or completed
				err := m.migrationsMgr.EnsureETLCompleted(migrationID, meta.ConfigPath, dbPath)
				if err != nil {
					return nil, fmt.Errorf("ETL not ready: %w", err)
				}
				// Re-check status after ETL
				yamlCfg, err = migration.LoadMigrationConfig(meta.ConfigPath)
				if err == nil {
					updatedStatus := strings.TrimSpace(yamlCfg.State.Status)
					if updatedStatus == "Awaiting-Path-Review" {
						useDuckDB = true
						reviewPhase = "traversal"
					} else if updatedStatus == "Awaiting-Copy-Review" {
						useDuckDB = true
						reviewPhase = "copy"
					}
				}
			}
		}
	}

	if !useDuckDB {
		return nil, fmt.Errorf("DuckDB not available: migration status is not Awaiting-Path-Review or Awaiting-Copy-Review")
	}

	// Get or open DuckDB connection
	duckdbPath := database.GetDuckDBPath(dbPath)
	duckdbConn := m.migrationsMgr.GetDuckDB(migrationID)
	if duckdbConn == nil {
		// Try to open DuckDB
		duckdbPool := m.migrationsMgr.GetDuckDBPool()
		if duckdbPool != nil {
			duckdbConn, err = duckdbPool.OpenDuckDB(migrationID, duckdbPath)
			if err != nil {
				return nil, fmt.Errorf("failed to open DuckDB: %w", err)
			}
		} else {
			return nil, fmt.Errorf("DuckDB pool not available")
		}
	}

	return &PathReviewContext{
		MigrationID: migrationID,
		DBPath:      dbPath,
		DuckDBPath:  duckdbPath,
		DuckDBConn:  duckdbConn,
		ReviewPhase: reviewPhase,
	}, nil
}

// findNodePathFromID finds the node path from a ULID in DuckDB
func (m *Manager) findNodePathFromID(ctx context.Context, prc *PathReviewContext, nodeID string) (string, error) {
	nodePath, err := database.FindNodePathByIDDuckDB(ctx, m.logger, prc.DuckDBConn, nodeID)
	if err != nil {
		return "", fmt.Errorf("failed to find node path: %w", err)
	}
	return nodePath, nil
}
