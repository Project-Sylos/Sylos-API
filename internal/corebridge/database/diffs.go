package database

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/rs/zerolog"
)

// buildEnrichmentFromPaths returns the SELECT that joins a path set CTE to the node tables.
// Engine schema: src_nodes, dst_nodes with path, id, type, depth, traversal_status, copy_status, size, mtime, excluded, etc.
func buildEnrichmentFromPaths(cteName string) string {
	return `
	SELECT
		p.path,
		su.path AS src_path,
		du.path AS dst_path,
		CASE WHEN COALESCE(su.path, p.path) = '/' THEN '/' ELSE regexp_replace(COALESCE(su.path, p.path), '^.*/', '') END AS src_name,
		CASE WHEN COALESCE(du.path, p.path) = '/' THEN '/' ELSE regexp_replace(COALESCE(du.path, p.path), '^.*/', '') END AS dst_name,
		su.type AS src_type,
		du.type AS dst_type,
		su.size AS src_size,
		du.size AS dst_size,
		su.mtime AS src_mtime,
		du.mtime AS dst_mtime,
		su.depth AS src_depth,
		du.depth AS dst_depth,
		su.traversal_status AS src_traversal_status,
		du.traversal_status AS dst_traversal_status,
		su.copy_status AS src_copy_status,
		du.copy_status AS dst_copy_status,
		su.id AS src_id,
		du.id AS dst_id,
		su.parent_id AS src_parent_id,
		du.parent_id AS dst_parent_id,
		su.service_id AS src_service_id,
		du.service_id AS dst_service_id,
		su.parent_service_id AS src_parent_service_id,
		du.parent_service_id AS dst_parent_service_id
	FROM ` + cteName + ` p
	LEFT JOIN src_nodes su ON su.path = p.path
	LEFT JOIN dst_nodes du ON du.path = p.path`
}

func getStringValue(ns sql.NullString) string {
	if ns.Valid {
		return ns.String
	}
	return ""
}

func getInt64Value(ni sql.NullInt64) int64 {
	if ni.Valid {
		return ni.Int64
	}
	return 0
}

// buildOrderByClause builds the ORDER BY clause based on sort field and direction
func buildOrderByClause(sortField, sortDir string) string {
	typeSort := "CASE WHEN COALESCE(src_type, dst_type) = 'folder' THEN 0 ELSE 1 END ASC"

	if sortField == "" {
		return typeSort + ", COALESCE(src_name, dst_name) ASC"
	}

	direction := "ASC"
	if sortDir == "desc" || sortDir == "DESC" {
		direction = "DESC"
	}

	var fieldSort string
	switch sortField {
	case "name":
		fieldSort = "COALESCE(src_name, dst_name) " + direction
	case "path":
		fieldSort = "COALESCE(src_path, dst_path) " + direction
	case "depth":
		fieldSort = "COALESCE(src_depth, dst_depth, 0) " + direction
	case "size":
		fieldSort = "COALESCE(src_size, dst_size, 0) " + direction
	case "type":
		fieldSort = "COALESCE(src_type, dst_type) " + direction
	case "traversalStatus":
		fieldSort = "COALESCE(src_traversal_status, dst_traversal_status) " + direction
	case "copyStatus":
		fieldSort = "COALESCE(src_copy_status, dst_copy_status) " + direction
	default:
		fieldSort = "COALESCE(src_name, dst_name) " + direction
	}

	return typeSort + ", " + fieldSort
}

// GetChildrenDiffsFromDuckDB retrieves merged children from both SRC and DST using DuckDB
func GetChildrenDiffsFromDuckDB(ctx context.Context, logger zerolog.Logger, duckdbConn *sql.DB, parentPath string, afterPath string, offset, limit int, foldersOnly bool, sortField, sortDir string, reviewPhase string, includeStats bool) (map[string]PathNodes, PaginationInfo, error) {
	if ctx.Err() != nil {
		return map[string]PathNodes{}, PaginationInfo{}, ctx.Err()
	}

	pageSize := limit
	if !includeStats {
		pageSize = limit + 1
	}

	useKeyset := afterPath != "" || offset == 0

	var phaseA string
	if useKeyset {
		phaseA = `
	WITH all_children AS (
		SELECT path FROM src_nodes WHERE parent_path = ? AND path > ?
		UNION
		SELECT path FROM dst_nodes WHERE parent_path = ? AND path > ?
	),
	candidate_paths AS (
		SELECT DISTINCT path FROM all_children`
	} else {
		phaseA = `
	WITH all_children AS (
		SELECT path FROM src_nodes WHERE parent_path = ?
		UNION
		SELECT path FROM dst_nodes WHERE parent_path = ?
	),
	candidate_paths AS (
		SELECT DISTINCT path FROM all_children`
	}
	if foldersOnly {
		phaseA += `
		WHERE EXISTS (SELECT 1 FROM src_nodes WHERE path = all_children.path AND type = 'folder')
		   OR EXISTS (SELECT 1 FROM dst_nodes WHERE path = all_children.path AND type = 'folder')`
	}
	phaseA += `
	)`

	var query string
	var args []any
	if useKeyset {
		query = phaseA + `
	SELECT * FROM (` + buildEnrichmentFromPaths("candidate_paths") + `) enriched
	ORDER BY path ASC
	LIMIT ?`
		args = []any{parentPath, afterPath, parentPath, afterPath, pageSize}
	} else {
		orderBy := buildOrderByClause(sortField, sortDir)
		query = phaseA + `
	SELECT * FROM (` + buildEnrichmentFromPaths("candidate_paths") + `) enriched
	ORDER BY ` + orderBy + `
	LIMIT ? OFFSET ?`
		args = []any{parentPath, parentPath, pageSize, offset}
	}

	rows, err := duckdbConn.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, PaginationInfo{}, fmt.Errorf("failed to query DuckDB for children: %w", err)
	}
	defer rows.Close()

	items := make(map[string]PathNodes)
	var pathOrder []string

	for rows.Next() {
		if ctx.Err() != nil {
			return nil, PaginationInfo{}, ctx.Err()
		}

		var path, srcPath, dstPath sql.NullString
		var srcName, dstName sql.NullString
		var srcType, dstType sql.NullString
		var srcSize, dstSize sql.NullInt64
		var srcMtime, dstMtime sql.NullString
		var srcDepth, dstDepth sql.NullInt64
		var srcTraversalStatus, dstTraversalStatus sql.NullString
		var srcCopyStatus, dstCopyStatus sql.NullString
		var srcID, dstID sql.NullString
		var srcParentID, dstParentID sql.NullString
		var srcServiceID, dstServiceID sql.NullString
		var srcParentServiceID, dstParentServiceID sql.NullString

		err := rows.Scan(
			&path, &srcPath, &dstPath,
			&srcName, &dstName,
			&srcType, &dstType,
			&srcSize, &dstSize,
			&srcMtime, &dstMtime,
			&srcDepth, &dstDepth,
			&srcTraversalStatus, &dstTraversalStatus,
			&srcCopyStatus, &dstCopyStatus,
			&srcID, &dstID,
			&srcParentID, &dstParentID,
			&srcServiceID, &dstServiceID,
			&srcParentServiceID, &dstParentServiceID,
		)
		if err != nil {
			logger.Warn().Err(err).Msg("failed to scan row from DuckDB")
			continue
		}

		if !path.Valid {
			continue
		}

		pathKey := path.String
		pathOrder = append(pathOrder, pathKey)
		pn := PathNodes{}

		if srcPath.Valid {
			pn.Src = &PathNodeItem{
				Queue:           "SRC",
				Id:              getStringValue(srcID),
				ParentId:        getStringValue(srcParentID),
				ParentPath:      parentPath,
				Name:            getStringValue(srcName),
				LocationPath:    srcPath.String,
				LastUpdated:     getStringValue(srcMtime),
				DepthLevel:      int(getInt64Value(srcDepth)),
				Type:            getStringValue(srcType),
				Size:            getInt64Value(srcSize),
				TraversalStatus: getStringValue(srcTraversalStatus),
				CopyStatus:      getStringValue(srcCopyStatus),
			}
		}

		if dstPath.Valid {
			pn.Dst = &PathNodeItem{
				Queue:           "DST",
				Id:              getStringValue(dstID),
				ParentId:        getStringValue(dstParentID),
				ParentPath:      parentPath,
				Name:            getStringValue(dstName),
				LocationPath:    dstPath.String,
				LastUpdated:     getStringValue(dstMtime),
				DepthLevel:      int(getInt64Value(dstDepth)),
				Type:            getStringValue(dstType),
				Size:            getInt64Value(dstSize),
				TraversalStatus: getStringValue(dstTraversalStatus),
				CopyStatus:      getStringValue(dstCopyStatus),
			}
		}

		items[pathKey] = pn
	}

	if err := rows.Err(); err != nil {
		return nil, PaginationInfo{}, fmt.Errorf("error iterating DuckDB rows: %w", err)
	}

	if !includeStats {
		hasMore := len(pathOrder) > limit
		if hasMore {
			delete(items, pathOrder[limit])
		}
		nextCursor := ""
		if useKeyset && hasMore && len(pathOrder) >= limit {
			nextCursor = pathOrder[limit-1]
		}
		return items, PaginationInfo{
			Offset:       offset,
			Limit:        limit,
			Total:        0,
			TotalFolders: 0,
			TotalFiles:   0,
			HasMore:      hasMore,
			NextCursor:   nextCursor,
		}, nil
	}

	totalQuery := `
	WITH all_children AS (
		SELECT path FROM src_nodes WHERE parent_path = ?
		UNION
		SELECT path FROM dst_nodes WHERE parent_path = ?
	)`
	if foldersOnly {
		totalQuery += `
	SELECT COUNT(*) FROM all_children
	WHERE EXISTS (SELECT 1 FROM src_nodes WHERE path = all_children.path AND type = 'folder')
	   OR EXISTS (SELECT 1 FROM dst_nodes WHERE path = all_children.path AND type = 'folder')`
	} else {
		totalQuery += `
	SELECT COUNT(*) FROM all_children`
	}

	total := 0
	err = duckdbConn.QueryRowContext(ctx, totalQuery, parentPath, parentPath).Scan(&total)
	if err != nil {
		return nil, PaginationInfo{}, fmt.Errorf("failed to get total count from DuckDB: %w", err)
	}

	foldersQuery := `
	WITH child_paths AS (
		SELECT path FROM src_nodes WHERE parent_path = ?
		UNION
		SELECT path FROM dst_nodes WHERE parent_path = ?
	),
	path_type AS (
		SELECT p.path, COALESCE(sc.type, dc.type) AS type
		FROM child_paths p
		LEFT JOIN src_nodes sc ON sc.path = p.path
		LEFT JOIN dst_nodes dc ON dc.path = p.path
	)
	SELECT 
		COUNT(*) FILTER (WHERE type = 'folder') AS folders_count,
		COUNT(*) FILTER (WHERE type = 'file') AS files_count
	FROM path_type
	WHERE type IS NOT NULL`

	var foldersCount, filesCount int
	err = duckdbConn.QueryRowContext(ctx, foldersQuery, parentPath, parentPath).Scan(&foldersCount, &filesCount)
	if err != nil {
		logger.Warn().Err(err).Msg("failed to get folders/files count from DuckDB")
		foldersCount = 0
		filesCount = 0
	}

	nextCursor := ""
	if useKeyset && (offset+limit) < total && len(pathOrder) >= limit {
		nextCursor = pathOrder[limit-1]
	}
	pagination := PaginationInfo{
		Offset:       offset,
		Limit:        limit,
		Total:        total,
		TotalFolders: foldersCount,
		TotalFiles:   filesCount,
		HasMore:      (offset + limit) < total,
		NextCursor:   nextCursor,
	}

	return items, pagination, nil
}

// GetChildrenDiffsStatsFromDuckDB returns total count and folders/files count for children of parentPath
func GetChildrenDiffsStatsFromDuckDB(ctx context.Context, logger zerolog.Logger, duckdbConn *sql.DB, parentPath string, foldersOnly bool) (total int, foldersCount int, filesCount int, err error) {
	if ctx.Err() != nil {
		return 0, 0, 0, ctx.Err()
	}

	totalQuery := `
	WITH all_children AS (
		SELECT path FROM src_nodes WHERE parent_path = ?
		UNION
		SELECT path FROM dst_nodes WHERE parent_path = ?
	)`
	if foldersOnly {
		totalQuery += `
	SELECT COUNT(*) FROM all_children
	WHERE EXISTS (SELECT 1 FROM src_nodes WHERE path = all_children.path AND type = 'folder')
	   OR EXISTS (SELECT 1 FROM dst_nodes WHERE path = all_children.path AND type = 'folder')`
	} else {
		totalQuery += `
	SELECT COUNT(*) FROM all_children`
	}

	err = duckdbConn.QueryRowContext(ctx, totalQuery, parentPath, parentPath).Scan(&total)
	if err != nil {
		return 0, 0, 0, fmt.Errorf("failed to get total count from DuckDB: %w", err)
	}

	foldersQuery := `
	WITH child_paths AS (
		SELECT path FROM src_nodes WHERE parent_path = ?
		UNION
		SELECT path FROM dst_nodes WHERE parent_path = ?
	),
	path_type AS (
		SELECT p.path, COALESCE(sc.type, dc.type) AS type
		FROM child_paths p
		LEFT JOIN src_nodes sc ON sc.path = p.path
		LEFT JOIN dst_nodes dc ON dc.path = p.path
	)
	SELECT 
		COUNT(*) FILTER (WHERE type = 'folder') AS folders_count,
		COUNT(*) FILTER (WHERE type = 'file') AS files_count
	FROM path_type
	WHERE type IS NOT NULL`

	err = duckdbConn.QueryRowContext(ctx, foldersQuery, parentPath, parentPath).Scan(&foldersCount, &filesCount)
	if err != nil {
		logger.Warn().Err(err).Msg("failed to get folders/files count from DuckDB")
		foldersCount = 0
		filesCount = 0
	}
	return total, foldersCount, filesCount, nil
}
