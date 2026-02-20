package manager

import (
	"context"
	"fmt"
	"strings"

	"codeberg.org/Sylos/Migration-Engine/pkg/migration"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge/database"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge/metadata"
)

func convertDBPathNodesToCorebridge(dbItems map[string]database.PathNodes) map[string]corebridge.PathNodes {
	items := make(map[string]corebridge.PathNodes, len(dbItems))
	for path, dbPathNodes := range dbItems {
		pathNodes := corebridge.PathNodes{}

		if dbPathNodes.Src != nil {
			pathNodes.Src = &corebridge.PathNodeItem{
				Queue:           dbPathNodes.Src.Queue,
				Id:              dbPathNodes.Src.Id,
				ParentId:        dbPathNodes.Src.ParentId,
				ParentPath:      dbPathNodes.Src.ParentPath,
				Name:            dbPathNodes.Src.Name,
				LocationPath:    dbPathNodes.Src.LocationPath,
				LastUpdated:     dbPathNodes.Src.LastUpdated,
				DepthLevel:      dbPathNodes.Src.DepthLevel,
				Type:            dbPathNodes.Src.Type,
				Size:            dbPathNodes.Src.Size,
				TraversalStatus: dbPathNodes.Src.TraversalStatus,
				CopyStatus:      dbPathNodes.Src.CopyStatus,
			}
		}

		if dbPathNodes.Dst != nil {
			pathNodes.Dst = &corebridge.PathNodeItem{
				Queue:           dbPathNodes.Dst.Queue,
				Id:              dbPathNodes.Dst.Id,
				ParentId:        dbPathNodes.Dst.ParentId,
				ParentPath:      dbPathNodes.Dst.ParentPath,
				Name:            dbPathNodes.Dst.Name,
				LocationPath:    dbPathNodes.Dst.LocationPath,
				LastUpdated:     dbPathNodes.Dst.LastUpdated,
				DepthLevel:      dbPathNodes.Dst.DepthLevel,
				Type:            dbPathNodes.Dst.Type,
				Size:            dbPathNodes.Dst.Size,
				TraversalStatus: dbPathNodes.Dst.TraversalStatus,
				CopyStatus:      dbPathNodes.Dst.CopyStatus,
			}
		}

		items[path] = pathNodes
	}
	return items
}

func (m *Manager) ListChildrenDiffs(ctx context.Context, req corebridge.ListChildrenDiffsRequest) (corebridge.ListChildrenDiffsResponse, error) {
	metaMgr := metadata.NewManager(m.cfg.Runtime.DataDir)
	meta, err := metaMgr.GetMigrationMetadata(req.MigrationID)
	if err != nil {
		return corebridge.ListChildrenDiffsResponse{}, corebridge.ErrMigrationNotFound
	}

	dbPath := strings.TrimSuffix(meta.ConfigPath, ".yaml") + ".db"
	if dbPath == ".db" {
		dbPath, err = database.ResolveDatabasePath(m.cfg.Runtime.DataDir, "", req.MigrationID)
		if err != nil {
			return corebridge.ListChildrenDiffsResponse{}, fmt.Errorf("failed to resolve database path: %w", err)
		}
	}

	useDuckDB := false
	if meta.ConfigPath != "" {
		yamlCfg, err := migration.LoadMigrationConfig(meta.ConfigPath)
		if err == nil {
			status := strings.TrimSpace(yamlCfg.State.Status)
			switch status {
			case "Awaiting-Path-Review", "Awaiting-Copy-Review":
				useDuckDB = true
			}
		}
	}

	var dbItems map[string]database.PathNodes
	var dbPagination database.PaginationInfo

	if useDuckDB {
		duckdbConn := m.migrationsMgr.GetDuckDB(req.MigrationID)
		if duckdbConn == nil {
			duckdbPool := m.migrationsMgr.GetDuckDBPool()
			if duckdbPool != nil {
				duckdbConn, err = duckdbPool.OpenDuckDB(req.MigrationID, dbPath)
				if err != nil {
					return corebridge.ListChildrenDiffsResponse{}, fmt.Errorf("failed to open DuckDB: %w", err)
				}
			} else {
				return corebridge.ListChildrenDiffsResponse{}, fmt.Errorf("DuckDB pool not available")
			}
		}

		sortField := ""
		sortDir := "asc"
		if req.Sort != nil {
			sortField = req.Sort.Field
			if req.Sort.Direction != "" {
				sortDir = req.Sort.Direction
			}
		}

		reviewPhase := "traversal"
		if meta.ConfigPath != "" {
			yamlCfg, err := migration.LoadMigrationConfig(meta.ConfigPath)
			if err == nil {
				status := strings.TrimSpace(yamlCfg.State.Status)
				if status == "Awaiting-Copy-Review" {
					reviewPhase = "copy"
				}
			}
		}

		dbItems, dbPagination, err = database.GetChildrenDiffsFromDuckDB(ctx, m.logger, duckdbConn, req.Path, req.AfterPath, req.Offset, req.Limit, req.FoldersOnly, sortField, sortDir, reviewPhase, false)
		if err != nil {
			return corebridge.ListChildrenDiffsResponse{}, fmt.Errorf("failed to get children diffs from DuckDB: %w", err)
		}
	}

	items := convertDBPathNodesToCorebridge(dbItems)

	return corebridge.ListChildrenDiffsResponse{
		Items: items,
		Pagination: corebridge.PaginationInfo{
			Offset:       dbPagination.Offset,
			Limit:        dbPagination.Limit,
			Total:        dbPagination.Total,
			TotalFolders: dbPagination.TotalFolders,
			TotalFiles:   dbPagination.TotalFiles,
			HasMore:      dbPagination.HasMore,
			NextCursor:   dbPagination.NextCursor,
		},
	}, nil
}

func (m *Manager) GetChildrenDiffsStats(ctx context.Context, migrationID, path string, foldersOnly bool) (corebridge.DiffsStatsResponse, error) {
	metaMgr := metadata.NewManager(m.cfg.Runtime.DataDir)
	meta, err := metaMgr.GetMigrationMetadata(migrationID)
	if err != nil {
		return corebridge.DiffsStatsResponse{}, corebridge.ErrMigrationNotFound
	}

	dbPath := strings.TrimSuffix(meta.ConfigPath, ".yaml") + ".db"
	if dbPath == ".db" {
		dbPath, err = database.ResolveDatabasePath(m.cfg.Runtime.DataDir, "", migrationID)
		if err != nil {
			return corebridge.DiffsStatsResponse{}, fmt.Errorf("failed to resolve database path: %w", err)
		}
	}

	useDuckDB := false
	if meta.ConfigPath != "" {
		yamlCfg, err := migration.LoadMigrationConfig(meta.ConfigPath)
		if err == nil {
			status := strings.TrimSpace(yamlCfg.State.Status)
			switch status {
			case "Awaiting-Path-Review", "Awaiting-Copy-Review":
				useDuckDB = true
			}
		}
	}

	if !useDuckDB {
		return corebridge.DiffsStatsResponse{}, corebridge.ErrDatabaseNotAvailable
	}

	duckdbConn := m.migrationsMgr.GetDuckDB(migrationID)
	if duckdbConn == nil {
		pool := m.migrationsMgr.GetDuckDBPool()
		if pool == nil {
			return corebridge.DiffsStatsResponse{}, fmt.Errorf("DuckDB pool not available")
		}
		var openErr error
		duckdbConn, openErr = pool.OpenDuckDB(migrationID, dbPath)
		if openErr != nil {
			return corebridge.DiffsStatsResponse{}, fmt.Errorf("failed to open DuckDB: %w", openErr)
		}
	}

	total, foldersCount, filesCount, err := database.GetChildrenDiffsStatsFromDuckDB(ctx, m.logger, duckdbConn, path, foldersOnly)
	if err != nil {
		return corebridge.DiffsStatsResponse{}, fmt.Errorf("failed to get diffs stats from DuckDB: %w", err)
	}
	return corebridge.DiffsStatsResponse{
		Total:        total,
		TotalFolders: foldersCount,
		TotalFiles:   filesCount,
	}, nil
}

func (m *Manager) SearchPathReviewItems(ctx context.Context, migrationID string, req corebridge.SearchRequest, offset, limit int) (corebridge.ListChildrenDiffsResponse, error) {
	metaMgr := metadata.NewManager(m.cfg.Runtime.DataDir)
	meta, err := metaMgr.GetMigrationMetadata(migrationID)
	if err != nil {
		return corebridge.ListChildrenDiffsResponse{}, corebridge.ErrMigrationNotFound
	}

	dbPath := strings.TrimSuffix(meta.ConfigPath, ".yaml") + ".db"
	if dbPath == ".db" {
		dbPath, err = database.ResolveDatabasePath(m.cfg.Runtime.DataDir, "", migrationID)
		if err != nil {
			return corebridge.ListChildrenDiffsResponse{}, fmt.Errorf("failed to resolve database path: %w", err)
		}
	}

	useDuckDB := false
	if meta.ConfigPath != "" {
		yamlCfg, err := migration.LoadMigrationConfig(meta.ConfigPath)
		if err == nil {
			status := strings.TrimSpace(yamlCfg.State.Status)
			if status == "Awaiting-Path-Review" || status == "Awaiting-Copy-Review" {
				useDuckDB = true
			}
		}
	}

	if !useDuckDB {
		return corebridge.ListChildrenDiffsResponse{}, corebridge.ErrDatabaseNotAvailable
	}

	duckdbConn := m.migrationsMgr.GetDuckDB(migrationID)
	if duckdbConn == nil {
		duckdbPool := m.migrationsMgr.GetDuckDBPool()
		if duckdbPool != nil {
			duckdbConn, err = duckdbPool.OpenDuckDB(migrationID, dbPath)
			if err != nil {
				return corebridge.ListChildrenDiffsResponse{}, fmt.Errorf("failed to open DuckDB: %w", err)
			}
		} else {
			return corebridge.ListChildrenDiffsResponse{}, fmt.Errorf("DuckDB pool not available")
		}
	}

	dbConditions := make([]database.SearchCondition, len(req.Conditions))
	for i, cond := range req.Conditions {
		dbConditions[i] = database.SearchCondition{
			Field:    cond.Field,
			Operator: cond.Operator,
			Value:    cond.Value,
		}
	}

	sortField := ""
	sortDir := "asc"
	if req.Sort != nil {
		sortField = req.Sort.Field
		if req.Sort.Direction != "" {
			sortDir = req.Sort.Direction
		}
	}

	statusSearchType := req.StatusSearchType
	if statusSearchType == "" {
		statusSearchType = "both"
	}

	reviewPhase := "traversal"
	if meta.ConfigPath != "" {
		yamlCfg, err := migration.LoadMigrationConfig(meta.ConfigPath)
		if err == nil {
			status := strings.TrimSpace(yamlCfg.State.Status)
			if status == "Awaiting-Copy-Review" {
				reviewPhase = "copy"
			}
		}
	}

	dbItems, dbPagination, err := database.SearchPathReviewItemsDuckDB(ctx, m.logger, duckdbConn, dbConditions, offset, limit, sortField, sortDir, statusSearchType, reviewPhase)
	if err != nil {
		return corebridge.ListChildrenDiffsResponse{}, fmt.Errorf("failed to search path review items: %w", err)
	}

	items := convertDBPathNodesToCorebridge(dbItems)

	return corebridge.ListChildrenDiffsResponse{
		Items: items,
		Pagination: corebridge.PaginationInfo{
			Offset:       dbPagination.Offset,
			Limit:        dbPagination.Limit,
			Total:        dbPagination.Total,
			TotalFolders: dbPagination.TotalFolders,
			TotalFiles:   dbPagination.TotalFiles,
			HasMore:      dbPagination.HasMore,
		},
	}, nil
}
