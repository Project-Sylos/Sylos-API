package manager

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"codeberg.org/Sylos/Migration-Engine/pkg/migration"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge/database"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge/metadata"
)

// PathReviewContext holds the context needed for path review operations.
type PathReviewContext struct {
	MigrationID string
	DBPath      string
	DuckDBPath  string
	DuckDBConn  *sql.DB
	ReviewPhase string
}

func (m *Manager) preparePathReviewContext(_ context.Context, migrationID string) (*PathReviewContext, error) {
	metaMgr := metadata.NewManager(m.cfg.Runtime.DataDir)
	meta, err := metaMgr.GetMigrationMetadata(migrationID)
	if err != nil {
		return nil, corebridge.ErrMigrationNotFound
	}

	dbPath := strings.TrimSuffix(meta.ConfigPath, ".yaml") + ".db"
	if dbPath == ".db" {
		dbPath, err = database.ResolveDatabasePath(m.cfg.Runtime.DataDir, "", migrationID)
		if err != nil {
			return nil, fmt.Errorf("failed to resolve database path: %w", err)
		}
	}

	useDuckDB := false
	var reviewPhase string
	if meta.ConfigPath != "" {
		yamlCfg, err := migration.LoadMigrationConfig(meta.ConfigPath)
		if err == nil {
			status := strings.TrimSpace(yamlCfg.State.Status)
			switch status {
			case "Awaiting-Path-Review":
				useDuckDB = true
				reviewPhase = "traversal"
			case "Awaiting-Copy-Review":
				useDuckDB = true
				reviewPhase = "copy"
			}
		}
	}

	if !useDuckDB {
		return nil, fmt.Errorf("DuckDB not available: migration status is not Awaiting-Path-Review or Awaiting-Copy-Review")
	}

	duckdbConn := m.migrationsMgr.GetDuckDB(migrationID)
	if duckdbConn == nil {
		duckdbPool := m.migrationsMgr.GetDuckDBPool()
		if duckdbPool != nil {
			duckdbConn, err = duckdbPool.OpenDuckDB(migrationID, dbPath)
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
		DuckDBPath:  dbPath,
		DuckDBConn:  duckdbConn,
		ReviewPhase: reviewPhase,
	}, nil
}

func (m *Manager) findNodePathFromID(ctx context.Context, prc *PathReviewContext, nodeID string) (string, error) {
	nodePath, err := database.FindNodePathByIDDuckDB(ctx, m.logger, prc.DuckDBConn, nodeID)
	if err != nil {
		return "", fmt.Errorf("failed to find node path: %w", err)
	}
	return nodePath, nil
}
