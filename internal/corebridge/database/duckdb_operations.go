package database

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"strings"

	"github.com/rs/zerolog"
)

// GetChildrenDiffsFromDuckDB retrieves merged children from both SRC and DST using DuckDB
// Uses direct path-based joins instead of path hashing
// sortField: field to sort by ("name", "path", "depth", "size", "type", "traversalStatus", etc.)
// sortDir: sort direction ("asc" or "desc", defaults to "asc")
// Returns items, pagination info, and stats (stats can be nil if calculation fails)
func GetChildrenDiffsFromDuckDB(ctx context.Context, logger zerolog.Logger, duckdbConn *sql.DB, parentPath string, offset, limit int, foldersOnly bool, sortField, sortDir string) (map[string]PathNodes, PaginationInfo, *PathReviewStats, error) {
	// Check for context cancellation
	if ctx.Err() != nil {
		return map[string]PathNodes{}, PaginationInfo{}, nil, ctx.Err()
	}

	// Build the query to get children from both src_nodes and dst_nodes
	// Join on path to find corresponding nodes
	// Find children where parent_path matches the given path

	query := `
	WITH src_children AS (
		SELECT path, parent_path, name, type, size, mtime, depth, traversal_status, copy_status, id, parent_id, service_id, parent_service_id
		FROM src_nodes
		WHERE parent_path = ?
	),
	dst_children AS (
		SELECT path, parent_path, name, type, size, mtime, depth, traversal_status, copy_status, id, parent_id, service_id, parent_service_id
		FROM dst_nodes
		WHERE parent_path = ?
	),
	merged AS (
		SELECT 
			COALESCE(s.path, d.path) AS path,
			s.path AS src_path,
			d.path AS dst_path,
			s.name AS src_name,
			d.name AS dst_name,
			s.type AS src_type,
			d.type AS dst_type,
			s.size AS src_size,
			d.size AS dst_size,
			s.mtime AS src_mtime,
			d.mtime AS dst_mtime,
			s.depth AS src_depth,
			d.depth AS dst_depth,
			s.traversal_status AS src_traversal_status,
			d.traversal_status AS dst_traversal_status,
			s.copy_status AS src_copy_status,
			d.copy_status AS dst_copy_status,
			s.id AS src_id,
			d.id AS dst_id,
			s.parent_id AS src_parent_id,
			d.parent_id AS dst_parent_id,
			s.service_id AS src_service_id,
			d.service_id AS dst_service_id,
			s.parent_service_id AS src_parent_service_id,
			d.parent_service_id AS dst_parent_service_id
		FROM src_children s
		FULL OUTER JOIN dst_children d ON s.path = d.path
	)
	SELECT * FROM merged
	`

	// Add foldersOnly filter if needed
	if foldersOnly {
		query += ` WHERE (src_type = 'folder' OR dst_type = 'folder' OR src_type IS NULL OR dst_type IS NULL)`
	}

	// Build ORDER BY clause based on sort field
	orderBy := buildOrderByClause(sortField, sortDir)
	query += ` ORDER BY ` + orderBy + ` LIMIT ? OFFSET ?`

	// Execute query
	rows, err := duckdbConn.QueryContext(ctx, query, parentPath, parentPath, limit, offset)
	if err != nil {
		return nil, PaginationInfo{}, nil, fmt.Errorf("failed to query DuckDB for children: %w", err)
	}
	defer rows.Close()

	// Build result map
	items := make(map[string]PathNodes)

	for rows.Next() {
		if ctx.Err() != nil {
			return nil, PaginationInfo{}, nil, ctx.Err()
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
		pn := PathNodes{}

		// Add SRC node if present
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

		// Add DST node if present
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
		return nil, PaginationInfo{}, nil, fmt.Errorf("error iterating DuckDB rows: %w", err)
	}

	// Get stats for all matching items (before pagination)
	stats, err := GetChildrenDiffsStatsFromDuckDB(ctx, logger, duckdbConn, parentPath, foldersOnly)
	if err != nil {
		logger.Warn().Err(err).Msg("failed to get stats from DuckDB")
		stats = nil // Continue without stats
	}

	// Get total count for pagination (can use stats if available)
	var total int
	if stats != nil {
		total = stats.FoldersCount + stats.FilesCount
	} else {
		// Fallback to separate count query
		totalQuery := `
		SELECT COUNT(*) FROM (
			SELECT path FROM src_nodes WHERE parent_path = ?
			UNION
			SELECT path FROM dst_nodes WHERE parent_path = ?
		) AS all_children
		`
		if foldersOnly {
			totalQuery = `
			SELECT COUNT(*) FROM (
				SELECT path FROM src_nodes WHERE parent_path = ? AND type = 'folder'
				UNION
				SELECT path FROM dst_nodes WHERE parent_path = ? AND type = 'folder'
			) AS all_children
			`
		}

		err = duckdbConn.QueryRowContext(ctx, totalQuery, parentPath, parentPath).Scan(&total)
		if err != nil {
			logger.Warn().Err(err).Msg("failed to get total count from DuckDB")
			total = len(items) // Fallback to items count
		}
	}

	// Get folders and files count from stats if available, otherwise count from paginated items
	var foldersCount, filesCount int
	if stats != nil {
		foldersCount = stats.FoldersCount
		filesCount = stats.FilesCount
	} else {
		// Count folders and files from paginated items (fallback)
		for _, pn := range items {
			if pn.Src != nil {
				if pn.Src.Type == "folder" {
					foldersCount++
				} else {
					filesCount++
				}
			} else if pn.Dst != nil {
				if pn.Dst.Type == "folder" {
					foldersCount++
				} else {
					filesCount++
				}
			}
		}
	}

	pagination := PaginationInfo{
		Offset:       offset,
		Limit:        limit,
		Total:        total,
		TotalFolders: foldersCount,
		TotalFiles:   filesCount,
		HasMore:      (offset + limit) < total,
	}

	return items, pagination, stats, nil
}

// GetChildrenDiffsStatsFromDuckDB calculates statistics for children matching the query (without pagination)
// Uses the same base query as GetChildrenDiffsFromDuckDB but calculates stats instead of returning items
func GetChildrenDiffsStatsFromDuckDB(ctx context.Context, logger zerolog.Logger, duckdbConn *sql.DB, parentPath string, foldersOnly bool) (*PathReviewStats, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	// Query for counts and stats (same base structure as GetChildrenDiffsFromDuckDB)
	statsQuery := `
	WITH src_children AS (
		SELECT path, type, size, traversal_status
		FROM src_nodes
		WHERE parent_path = ?
	),
	dst_children AS (
		SELECT path, type, size, traversal_status
		FROM dst_nodes
		WHERE parent_path = ?
	),
	merged AS (
		SELECT 
			COALESCE(s.path, d.path) AS path,
			COALESCE(s.type, d.type) AS type,
			s.size AS src_size,
			d.size AS dst_size,
			COALESCE(s.traversal_status, d.traversal_status) AS traversal_status
		FROM src_children s
		FULL OUTER JOIN dst_children d ON s.path = d.path
	)
	`

	if foldersOnly {
		statsQuery += ` WHERE (type = 'folder' OR type IS NULL)`
	}

	statsQuery += `
	SELECT 
		COUNT(DISTINCT path) AS total_count,
		COUNT(DISTINCT CASE WHEN type = 'folder' THEN path END) AS folders_count,
		COUNT(DISTINCT CASE WHEN type = 'file' THEN path END) AS files_count,
		COUNT(DISTINCT CASE WHEN traversal_status = 'pending' THEN path END) AS pending_count,
		COUNT(DISTINCT CASE WHEN traversal_status = 'failed' THEN path END) AS failed_count,
		COUNT(DISTINCT CASE WHEN traversal_status IN ('exclusion_explicit', 'exclusion_inherited') THEN path END) AS excluded_count,
		COALESCE(SUM(CASE WHEN type = 'file' THEN src_size ELSE 0 END), 0) AS src_total_size,
		COALESCE(SUM(CASE WHEN type = 'file' THEN dst_size ELSE 0 END), 0) AS dst_total_size
	FROM merged
	`

	var totalCount, foldersCount, filesCount, pendingCount, failedCount, excludedCount int
	var srcTotalSize, dstTotalSize int64

	err := duckdbConn.QueryRowContext(ctx, statsQuery, parentPath, parentPath).Scan(
		&totalCount,
		&foldersCount,
		&filesCount,
		&pendingCount,
		&failedCount,
		&excludedCount,
		&srcTotalSize,
		&dstTotalSize,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to get stats from DuckDB: %w", err)
	}

	// Calculate ratios (rounded to 2 decimal places)
	var foldersRatio, filesRatio float64
	if totalCount > 0 {
		foldersRatio = math.Round(float64(foldersCount)/float64(totalCount)*10000) / 100
		filesRatio = math.Round(float64(filesCount)/float64(totalCount)*10000) / 100
	}

	return &PathReviewStats{
		PendingCount:  pendingCount,
		FailedCount:   failedCount,
		ExcludedCount: excludedCount,
		FoldersCount:  foldersCount,
		FilesCount:    filesCount,
		FoldersRatio:  foldersRatio,
		FilesRatio:    filesRatio,
		TotalFileSize: FileSizeStats{
			Src: srcTotalSize,
			Dst: dstTotalSize,
		},
	}, nil
}

// FindNodePathByIDDuckDB finds the path of a node by its ULID in DuckDB
func FindNodePathByIDDuckDB(ctx context.Context, logger zerolog.Logger, duckdbConn *sql.DB, nodeID string) (string, error) {
	// Try src_nodes first
	query := `SELECT path FROM src_nodes WHERE id = ? LIMIT 1`
	var path string
	err := duckdbConn.QueryRowContext(ctx, query, nodeID).Scan(&path)
	if err == nil {
		return path, nil
	}
	if err != sql.ErrNoRows {
		return "", fmt.Errorf("failed to query src_nodes: %w", err)
	}

	// Try dst_nodes
	query = `SELECT path FROM dst_nodes WHERE id = ? LIMIT 1`
	err = duckdbConn.QueryRowContext(ctx, query, nodeID).Scan(&path)
	if err != nil {
		if err == sql.ErrNoRows {
			return "", fmt.Errorf("node with ID %s not found", nodeID)
		}
		return "", fmt.Errorf("failed to query dst_nodes: %w", err)
	}

	return path, nil
}

// SetNodeExclusionDuckDB sets the exclusion status for a node in DuckDB
// Updates the immediate node's traversal_status, then triggers background propagation
// When excluding, sets immediate node to 'exclusion_explicit', children get 'exclusion_inherited' via propagation
func SetNodeExclusionDuckDB(ctx context.Context, logger zerolog.Logger, duckdbConn *sql.DB, nodePath string, excluded bool) error {
	// Determine the new status
	// For exclusion: immediate node gets 'exclusion_explicit', children get 'exclusion_inherited' via propagation
	// For unexclusion: immediate node and children get 'pending'
	newStatus := "pending"
	if excluded {
		newStatus = "exclusion_explicit"
	}

	// Update the immediate node's traversal_status
	// First, try to update in src_nodes
	updateSrcQuery := `
	UPDATE src_nodes 
	SET traversal_status = ?
	WHERE path = ?
	`
	result, err := duckdbConn.ExecContext(ctx, updateSrcQuery, newStatus, nodePath)
	if err != nil {
		return fmt.Errorf("failed to update src_nodes: %w", err)
	}

	rowsAffected, _ := result.RowsAffected()
	if rowsAffected == 0 {
		// Try dst_nodes
		updateDstQuery := `
		UPDATE dst_nodes 
		SET traversal_status = ?
		WHERE path = ?
		`
		_, err = duckdbConn.ExecContext(ctx, updateDstQuery, newStatus, nodePath)
		if err != nil {
			return fmt.Errorf("failed to update dst_nodes: %w", err)
		}
	}

	logger.Info().
		Str("node_path", nodePath).
		Bool("excluded", excluded).
		Str("new_status", newStatus).
		Msg("updated node exclusion status in DuckDB")

	// Note: Background propagation will be handled by a separate goroutine
	// This is called from the manager which will trigger the background task

	return nil
}

// PropagateExclusionDuckDB propagates exclusion status to all children in DuckDB
// This runs in a background goroutine
// Uses path prefix matching to update all descendants in a single query
func PropagateExclusionDuckDB(ctx context.Context, logger zerolog.Logger, duckdbConn *sql.DB, parentPath string, excluded bool) error {
	newStatus := "pending"
	if excluded {
		newStatus = "exclusion_inherited"
	}

	// Use path prefix matching to find all descendants
	// All children will have paths that start with "{parentPath}/"
	// The trailing '/' ensures we only match direct children and descendants, not siblings
	// Example: excluding "/root/folder_1" will match "/root/folder_1/child" but NOT "/root/folder_10"
	// DuckDB supports STARTS_WITH function for safer prefix matching
	updateQuery := `
	UPDATE src_nodes 
	SET traversal_status = ?
	WHERE STARTS_WITH(path, ? || '/')
	`

	result, err := duckdbConn.ExecContext(ctx, updateQuery, newStatus, parentPath)
	if err != nil {
		return fmt.Errorf("failed to propagate exclusion in src_nodes: %w", err)
	}

	srcRowsAffected, _ := result.RowsAffected()
	logger.Info().
		Str("parent_path", parentPath).
		Bool("excluded", excluded).
		Int64("src_rows_affected", srcRowsAffected).
		Msg("propagated exclusion to src_nodes children")

	// Also update dst_nodes
	updateDstQuery := `
	UPDATE dst_nodes 
	SET traversal_status = ?
	WHERE STARTS_WITH(path, ? || '/')
	`

	result, err = duckdbConn.ExecContext(ctx, updateDstQuery, newStatus, parentPath)
	if err != nil {
		return fmt.Errorf("failed to propagate exclusion in dst_nodes: %w", err)
	}

	dstRowsAffected, _ := result.RowsAffected()
	logger.Info().
		Str("parent_path", parentPath).
		Bool("excluded", excluded).
		Int64("dst_rows_affected", dstRowsAffected).
		Msg("propagated exclusion to dst_nodes children")

	return nil
}

// MarkNodeForRetryDuckDB marks a node for retry in DuckDB
// For src nodes: marks the src node as pending, and if it has a corresponding dst node,
// marks all dst children with NotOnSrc status as pending too.
// For dst nodes: just marks the dst node as pending.
func MarkNodeForRetryDuckDB(ctx context.Context, logger zerolog.Logger, duckdbConn *sql.DB, nodePath string) error {
	// First, determine if this is a src or dst node and get the corresponding node info
	var isSrc bool
	var dstID sql.NullString
	var srcID sql.NullString

	// Check src_nodes first
	checkSrcQuery := `SELECT id, dst_id FROM src_nodes WHERE path = ? LIMIT 1`
	err := duckdbConn.QueryRowContext(ctx, checkSrcQuery, nodePath).Scan(&srcID, &dstID)
	if err == nil {
		isSrc = true
	} else if err != sql.ErrNoRows {
		return fmt.Errorf("failed to check src_nodes: %w", err)
	} else {
		// Not in src_nodes, check dst_nodes
		checkDstQuery := `SELECT id, src_id FROM dst_nodes WHERE path = ? LIMIT 1`
		var dstIDForCheck sql.NullString
		err = duckdbConn.QueryRowContext(ctx, checkDstQuery, nodePath).Scan(&dstIDForCheck, &srcID)
		if err != nil {
			if err == sql.ErrNoRows {
				return fmt.Errorf("node with path %s not found in src_nodes or dst_nodes", nodePath)
			}
			return fmt.Errorf("failed to check dst_nodes: %w", err)
		}
		isSrc = false
	}

	if isSrc {
		// Update src node from "failed" to "pending"
		updateSrcQuery := `
		UPDATE src_nodes 
		SET traversal_status = 'pending'
		WHERE path = ? AND traversal_status = 'failed'
		`
		result, err := duckdbConn.ExecContext(ctx, updateSrcQuery, nodePath)
		if err != nil {
			return fmt.Errorf("failed to mark src node for retry: %w", err)
		}

		rowsAffected, _ := result.RowsAffected()
		if rowsAffected == 0 {
			logger.Warn().
				Str("node_path", nodePath).
				Msg("src node not found or not in 'failed' status")
		}

		// If src node has a corresponding dst node, check its children
		if dstID.Valid && dstID.String != "" {
			// Find the dst node path using dst_id
			var dstPath string
			getDstPathQuery := `SELECT path FROM dst_nodes WHERE id = ? LIMIT 1`
			err = duckdbConn.QueryRowContext(ctx, getDstPathQuery, dstID.String).Scan(&dstPath)
			if err == nil {
				// Mark all dst children with NotOnSrc status as pending
				updateDstChildrenQuery := `
				UPDATE dst_nodes 
				SET traversal_status = 'pending'
				WHERE parent_path = ? AND traversal_status = 'NotOnSrc'
				`
				result, err = duckdbConn.ExecContext(ctx, updateDstChildrenQuery, dstPath)
				if err != nil {
					logger.Warn().
						Err(err).
						Str("dst_path", dstPath).
						Msg("failed to update dst children for retry")
				} else {
					dstChildrenAffected, _ := result.RowsAffected()
					logger.Info().
						Str("src_path", nodePath).
						Str("dst_path", dstPath).
						Int64("dst_children_updated", dstChildrenAffected).
						Msg("marked dst children with NotOnSrc status as pending for retry")
				}
			} else if err != sql.ErrNoRows {
				logger.Warn().
					Err(err).
					Str("dst_id", dstID.String).
					Msg("failed to find corresponding dst node path")
			}
		}

		logger.Info().
			Str("node_path", nodePath).
			Bool("is_src", true).
			Msg("marked src node for retry in DuckDB")
	} else {
		// For dst nodes, just mark the node as pending
		updateDstQuery := `
		UPDATE dst_nodes 
		SET traversal_status = 'pending'
		WHERE path = ? AND traversal_status = 'failed'
		`
		result, err := duckdbConn.ExecContext(ctx, updateDstQuery, nodePath)
		if err != nil {
			return fmt.Errorf("failed to mark dst node for retry: %w", err)
		}

		rowsAffected, _ := result.RowsAffected()
		if rowsAffected == 0 {
			logger.Warn().
				Str("node_path", nodePath).
				Msg("dst node not found or not in 'failed' status")
		}

		logger.Info().
			Str("node_path", nodePath).
			Bool("is_src", false).
			Msg("marked dst node for retry in DuckDB")
	}

	return nil
}

// MarkAllFailedAsExcludedDuckDB marks all failed items as excluded in DuckDB
// Updates all items in src_nodes and dst_nodes where traversal_status = 'failed' to 'exclusion_explicit'
// Then triggers exclusion propagation for each path
func MarkAllFailedAsExcludedDuckDB(ctx context.Context, logger zerolog.Logger, duckdbConn *sql.DB) error {
	// Check for context cancellation
	if ctx.Err() != nil {
		return ctx.Err()
	}

	// Step 1: Get all failed paths from src_nodes
	getFailedSrcPathsQuery := `
	SELECT DISTINCT path FROM src_nodes WHERE traversal_status = 'failed'
	`
	rows, err := duckdbConn.QueryContext(ctx, getFailedSrcPathsQuery)
	if err != nil {
		return fmt.Errorf("failed to query failed src paths: %w", err)
	}
	defer rows.Close()

	var failedPaths []string
	for rows.Next() {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		var path string
		if err := rows.Scan(&path); err != nil {
			logger.Warn().Err(err).Msg("failed to scan failed src path")
			continue
		}
		failedPaths = append(failedPaths, path)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("error iterating failed src paths: %w", err)
	}

	// Step 2: Get all failed paths from dst_nodes
	getFailedDstPathsQuery := `
	SELECT DISTINCT path FROM dst_nodes WHERE traversal_status = 'failed'
	`
	rows, err = duckdbConn.QueryContext(ctx, getFailedDstPathsQuery)
	if err != nil {
		return fmt.Errorf("failed to query failed dst paths: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		var path string
		if err := rows.Scan(&path); err != nil {
			logger.Warn().Err(err).Msg("failed to scan failed dst path")
			continue
		}
		// Only add if not already in list (avoid duplicates)
		found := false
		for _, existingPath := range failedPaths {
			if existingPath == path {
				found = true
				break
			}
		}
		if !found {
			failedPaths = append(failedPaths, path)
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("error iterating failed dst paths: %w", err)
	}

	// Step 3: Mark all failed items as exclusion_explicit
	updateSrcQuery := `
	UPDATE src_nodes 
	SET traversal_status = 'exclusion_explicit'
	WHERE traversal_status = 'failed'
	`
	result, err := duckdbConn.ExecContext(ctx, updateSrcQuery)
	if err != nil {
		return fmt.Errorf("failed to mark failed src nodes as excluded: %w", err)
	}
	srcRowsAffected, _ := result.RowsAffected()

	updateDstQuery := `
	UPDATE dst_nodes 
	SET traversal_status = 'exclusion_explicit'
	WHERE traversal_status = 'failed'
	`
	result, err = duckdbConn.ExecContext(ctx, updateDstQuery)
	if err != nil {
		return fmt.Errorf("failed to mark failed dst nodes as excluded: %w", err)
	}
	dstRowsAffected, _ := result.RowsAffected()

	logger.Info().
		Int64("src_rows_affected", srcRowsAffected).
		Int64("dst_rows_affected", dstRowsAffected).
		Int("paths_to_propagate", len(failedPaths)).
		Msg("marked all failed items as exclusion_explicit")

	// Step 4: Propagate exclusion for each path (in background, caller will handle)
	// We return the paths so the caller can trigger propagation
	// For now, we'll propagate inline but this could be optimized
	for _, path := range failedPaths {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err := PropagateExclusionDuckDB(ctx, logger, duckdbConn, path, true); err != nil {
			logger.Warn().
				Err(err).
				Str("path", path).
				Msg("failed to propagate exclusion for path")
			// Continue with other paths
		}
	}

	return nil
}

// RetryAllFailedDuckDB marks all failed items for retry in DuckDB
// This function:
// 1. Marks all failed items in src_nodes from 'failed' → 'pending'
// 2. For each src node with a dst_id, finds the corresponding dst node path
// 3. Marks all dst children with NotOnSrc status as pending
func RetryAllFailedDuckDB(ctx context.Context, logger zerolog.Logger, duckdbConn *sql.DB) error {
	// Check for context cancellation
	if ctx.Err() != nil {
		return ctx.Err()
	}

	// Step 1: Mark all failed items in src_nodes as pending
	updateSrcQuery := `
	UPDATE src_nodes 
	SET traversal_status = 'pending'
	WHERE traversal_status = 'failed'
	`
	result, err := duckdbConn.ExecContext(ctx, updateSrcQuery)
	if err != nil {
		return fmt.Errorf("failed to mark all failed src nodes for retry: %w", err)
	}

	srcRowsAffected, _ := result.RowsAffected()
	logger.Info().
		Int64("src_rows_affected", srcRowsAffected).
		Msg("marked all failed src nodes as pending")

	// Step 2: For all src nodes that now have 'pending' status and have a dst_id,
	// find their corresponding dst paths and mark dst children with NotOnSrc as pending
	// Use a CTE to batch process this efficiently
	updateDstChildrenQuery := `
	WITH src_with_dst AS (
		SELECT DISTINCT d.path AS dst_path
		FROM src_nodes s
		JOIN dst_nodes d ON s.dst_id = d.id
		WHERE s.traversal_status = 'pending' 
		  AND s.dst_id IS NOT NULL
		  AND d.path IS NOT NULL
	)
	UPDATE dst_nodes
	SET traversal_status = 'pending'
	WHERE parent_path IN (SELECT dst_path FROM src_with_dst)
	  AND traversal_status = 'NotOnSrc'
	`

	result, err = duckdbConn.ExecContext(ctx, updateDstChildrenQuery)
	if err != nil {
		return fmt.Errorf("failed to mark dst children for retry: %w", err)
	}

	dstRowsAffected, _ := result.RowsAffected()
	logger.Info().
		Int64("dst_children_rows_affected", dstRowsAffected).
		Msg("marked dst children with NotOnSrc status as pending for retry")

	return nil
}

// UnmarkNodeForRetryDuckDB unmarks a node for retry in DuckDB
func UnmarkNodeForRetryDuckDB(ctx context.Context, logger zerolog.Logger, duckdbConn *sql.DB, nodePath string) error {
	// Update traversal_status from "pending" back to "failed"
	updateQuery := `
	UPDATE src_nodes 
	SET traversal_status = 'failed'
	WHERE path = ? AND traversal_status = 'pending'
	`

	result, err := duckdbConn.ExecContext(ctx, updateQuery, nodePath)
	if err != nil {
		return fmt.Errorf("failed to unmark node for retry in src_nodes: %w", err)
	}

	rowsAffected, _ := result.RowsAffected()
	if rowsAffected == 0 {
		// Try dst_nodes
		updateDstQuery := `
		UPDATE dst_nodes 
		SET traversal_status = 'failed'
		WHERE path = ? AND traversal_status = 'pending'
		`
		_, err = duckdbConn.ExecContext(ctx, updateDstQuery, nodePath)
		if err != nil {
			return fmt.Errorf("failed to unmark node for retry in dst_nodes: %w", err)
		}
	}

	logger.Info().
		Str("node_path", nodePath).
		Msg("unmarked node for retry in DuckDB")

	return nil
}

// Helper functions for null handling
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
// sortField: field name ("name", "path", "depth", "size", "type", "traversalStatus")
// sortDir: direction ("asc" or "desc", defaults to "asc")
func buildOrderByClause(sortField, sortDir string) string {
	// Default to name ascending if no sort field specified
	if sortField == "" {
		return "COALESCE(src_name, dst_name) ASC"
	}

	// Normalize direction
	direction := "ASC"
	if sortDir == "desc" || sortDir == "DESC" {
		direction = "DESC"
	}

	// Map field names to SQL expressions
	switch sortField {
	case "name":
		return "COALESCE(src_name, dst_name) " + direction
	case "path":
		return "COALESCE(src_path, dst_path) " + direction
	case "depth":
		// Use COALESCE to handle nulls, defaulting to 0
		return "COALESCE(src_depth, dst_depth, 0) " + direction
	case "size":
		// For size, use COALESCE to prefer src_size over dst_size, defaulting to 0
		return "COALESCE(src_size, dst_size, 0) " + direction
	case "type":
		return "COALESCE(src_type, dst_type) " + direction
	case "traversalStatus":
		// Use src_traversal_status if available, otherwise dst_traversal_status
		return "COALESCE(src_traversal_status, dst_traversal_status) " + direction
	case "copyStatus":
		return "COALESCE(src_copy_status, dst_copy_status) " + direction
	default:
		// Unknown field, default to name
		return "COALESCE(src_name, dst_name) " + direction
	}
}

// GetAllNodesByStatusDuckDB gets all node paths with a specific status from DuckDB
// Returns unique paths across both src_nodes and dst_nodes
func GetAllNodesByStatusDuckDB(ctx context.Context, logger zerolog.Logger, duckdbConn *sql.DB, status string) ([]string, error) {
	// Check for context cancellation
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	// Get all paths with the specified status from both tables
	query := `
	SELECT DISTINCT path 
	FROM (
		SELECT path FROM src_nodes WHERE traversal_status = ?
		UNION
		SELECT path FROM dst_nodes WHERE traversal_status = ?
	) AS all_paths
	`

	rows, err := duckdbConn.QueryContext(ctx, query, status, status)
	if err != nil {
		return nil, fmt.Errorf("failed to query nodes by status: %w", err)
	}
	defer rows.Close()

	var paths []string
	for rows.Next() {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		var path string
		if err := rows.Scan(&path); err != nil {
			logger.Warn().Err(err).Msg("failed to scan path")
			continue
		}
		paths = append(paths, path)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating paths: %w", err)
	}

	return paths, nil
}

// SearchCondition represents a single search condition (local type to avoid import cycles)
type SearchCondition struct {
	Field    string
	Operator string
	Value    any
}

// SearchPathReviewItemsDuckDB searches for path review items matching the given conditions
// Returns all nodes (not just children of a specific path) that match the conditions
// For path and name fields, uses LIKE '%value%' for substring matching
// Uses two-phase approach: first paginate paths (entities), then join data
func SearchPathReviewItemsDuckDB(ctx context.Context, logger zerolog.Logger, duckdbConn *sql.DB, conditions []SearchCondition, offset, limit int, sortField, sortDir string) (map[string]PathNodes, PaginationInfo, *PathReviewStats, error) {
	// Check for context cancellation
	if ctx.Err() != nil {
		return map[string]PathNodes{}, PaginationInfo{}, nil, ctx.Err()
	}

	// Build WHERE clause filters for path selection phase (using EXISTS)
	pathFilterClause, pathFilterArgs := buildPathFilterClause(conditions)

	// For path selection (Phase A), we can only sort by path itself
	// For other sort fields, we'll sort after the join in Phase B
	direction := "ASC"
	if sortDir == "desc" || sortDir == "DESC" {
		direction = "DESC"
	}
	pathOrderBy := "path " + direction

	// Phase A: Get unique paths with filters applied and pagination
	// This ensures correct pagination on entities (paths), not joined rows
	query := `
	WITH all_paths AS (
		SELECT DISTINCT path FROM src_nodes
		UNION
		SELECT DISTINCT path FROM dst_nodes
	),
	filtered_paths AS (
		SELECT path
		FROM all_paths
		WHERE path != '/'` + pathFilterClause + `
	),
	paged_paths AS (
		SELECT path
		FROM filtered_paths
		ORDER BY ` + pathOrderBy + `
		LIMIT ? OFFSET ?
	),
	merged AS (
		SELECT 
			COALESCE(s.path, d.path) AS path,
			s.path AS src_path,
			d.path AS dst_path,
			s.name AS src_name,
			d.name AS dst_name,
			s.type AS src_type,
			d.type AS dst_type,
			s.size AS src_size,
			d.size AS dst_size,
			s.mtime AS src_mtime,
			d.mtime AS dst_mtime,
			s.depth AS src_depth,
			d.depth AS dst_depth,
			s.traversal_status AS src_traversal_status,
			d.traversal_status AS dst_traversal_status,
			s.copy_status AS src_copy_status,
			d.copy_status AS dst_copy_status,
			s.id AS src_id,
			d.id AS dst_id,
			s.parent_id AS src_parent_id,
			d.parent_id AS dst_parent_id,
			s.service_id AS src_service_id,
			d.service_id AS dst_service_id,
			s.parent_service_id AS src_parent_service_id,
			d.parent_service_id AS dst_parent_service_id
		FROM src_nodes s
		FULL OUTER JOIN dst_nodes d ON s.path = d.path
		WHERE COALESCE(s.path, d.path) IN (SELECT path FROM paged_paths)
	)
	SELECT * FROM merged`

	// If sorting by a field other than path, apply final sort after join
	if sortField != "" && sortField != "path" {
		finalOrderBy := buildOrderByClause(sortField, sortDir)
		query += " ORDER BY " + finalOrderBy
	} else {
		query += " ORDER BY path"
	}

	// Build args: pathFilterArgs, then limit, offset
	args := append(pathFilterArgs, limit, offset)

	// Execute query
	rows, err := duckdbConn.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, PaginationInfo{}, nil, fmt.Errorf("failed to query DuckDB for search: %w", err)
	}
	defer rows.Close()

	// Build result map (same structure as GetChildrenDiffsFromDuckDB)
	items := make(map[string]PathNodes)

	for rows.Next() {
		if ctx.Err() != nil {
			return nil, PaginationInfo{}, nil, ctx.Err()
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
			&path,
			&srcPath, &dstPath,
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
			return nil, PaginationInfo{}, nil, fmt.Errorf("failed to scan row: %w", err)
		}

		// Build path key (use path from result)
		pathKey := path.String

		// Build PathNodes structure
		pn := PathNodes{}

		// Add SRC node if present
		if srcPath.Valid {
			pn.Src = &PathNodeItem{
				Queue:           "SRC",
				Id:              getStringValue(srcID),
				ParentId:        getStringValue(srcParentID),
				ParentPath:      "", // Not available in merged query
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

		// Add DST node if present
		if dstPath.Valid {
			pn.Dst = &PathNodeItem{
				Queue:           "DST",
				Id:              getStringValue(dstID),
				ParentId:        getStringValue(dstParentID),
				ParentPath:      "", // Not available in merged query
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
		return nil, PaginationInfo{}, nil, fmt.Errorf("error iterating DuckDB rows: %w", err)
	}

	// Get stats for all matching items (before pagination)
	stats, err := SearchPathReviewStatsDuckDB(ctx, logger, duckdbConn, conditions)
	if err != nil {
		logger.Warn().Err(err).Msg("failed to get stats from DuckDB search")
		stats = nil // Continue without stats
	}

	// Get total count for pagination
	var total int
	if stats != nil {
		total = stats.FoldersCount + stats.FilesCount
	} else {
		// Fallback: count items in current page (approximation)
		total = len(items)
	}

	// Count folders and files from items or stats
	var totalFolders, totalFiles int
	if stats != nil {
		totalFolders = stats.FoldersCount
		totalFiles = stats.FilesCount
	} else {
		// Count from items
		for _, pn := range items {
			typ := ""
			if pn.Src != nil {
				typ = pn.Src.Type
			} else if pn.Dst != nil {
				typ = pn.Dst.Type
			}
			if typ == "folder" {
				totalFolders++
			} else if typ == "file" {
				totalFiles++
			}
		}
	}

	hasMore := offset+limit < total

	pagination := PaginationInfo{
		Offset:       offset,
		Limit:        limit,
		Total:        total,
		TotalFolders: totalFolders,
		TotalFiles:   totalFiles,
		HasMore:      hasMore,
	}

	return items, pagination, stats, nil
}

// buildSearchWhereClause builds a WHERE clause from search conditions
// Returns the WHERE clause SQL and the arguments array
// For path and name fields, treats all operators as LIKE '%value%' for substring matching
// Uses the full merged query field names (src_path, dst_path, src_name, dst_name, etc.)
func buildSearchWhereClause(conditions []SearchCondition) (string, []any) {
	if len(conditions) == 0 {
		return "", nil
	}

	var clauses []string
	var args []any

	for _, cond := range conditions {
		clause, clauseArgs := buildConditionClause(cond, false)
		if clause != "" {
			clauses = append(clauses, clause)
			args = append(args, clauseArgs...)
		}
	}

	if len(clauses) == 0 {
		return "", nil
	}

	// Join all clauses with AND
	return strings.Join(clauses, " AND "), args
}

// buildStatsWhereClause builds a WHERE clause from search conditions for stats queries
// Uses the simplified merged query field names (path, type, traversal_status, etc.)
func buildStatsWhereClause(conditions []SearchCondition) (string, []any) {
	if len(conditions) == 0 {
		return "", nil
	}

	var clauses []string
	var args []any

	for _, cond := range conditions {
		clause, clauseArgs := buildConditionClause(cond, true)
		if clause != "" {
			clauses = append(clauses, clause)
			args = append(args, clauseArgs...)
		}
	}

	if len(clauses) == 0 {
		return "", nil
	}

	// Join all clauses with AND
	return strings.Join(clauses, " AND "), args
}

// buildPathFilterClause builds EXISTS-based WHERE clause filters for path selection phase
// Returns SQL clause and arguments for filtering paths before pagination
// Uses EXISTS to check conditions against src_nodes and/or dst_nodes
func buildPathFilterClause(conditions []SearchCondition) (string, []any) {
	if len(conditions) == 0 {
		return "", nil
	}

	var clauses []string
	var args []any

	for _, cond := range conditions {
		clause, clauseArgs := buildPathFilterCondition(cond)
		if clause != "" {
			clauses = append(clauses, clause)
			args = append(args, clauseArgs...)
		}
	}

	if len(clauses) == 0 {
		return "", nil
	}

	// Join all clauses with AND, each wrapped in parentheses for clarity
	return " AND (" + strings.Join(clauses, " AND ") + ")", args
}

// buildPathFilterCondition builds an EXISTS-based condition for path filtering
// Returns SQL that checks if a path exists in src_nodes OR dst_nodes matching the condition
func buildPathFilterCondition(cond SearchCondition) (string, []any) {
	valueStr := fmt.Sprintf("%v", cond.Value)

	switch cond.Field {
	case "path":
		// Path filter: check if path matches pattern in either table
		return `(
			EXISTS (SELECT 1 FROM src_nodes s WHERE s.path = all_paths.path AND s.path LIKE ?)
			OR EXISTS (SELECT 1 FROM dst_nodes d WHERE d.path = all_paths.path AND d.path LIKE ?)
		)`, []any{"%" + valueStr + "%", "%" + valueStr + "%"}

	case "name":
		// Name filter: check if name matches pattern in either table
		return `(
			EXISTS (SELECT 1 FROM src_nodes s WHERE s.path = all_paths.path AND s.name LIKE ?)
			OR EXISTS (SELECT 1 FROM dst_nodes d WHERE d.path = all_paths.path AND d.name LIKE ?)
		)`, []any{"%" + valueStr + "%", "%" + valueStr + "%"}

	case "type":
		// Type filter: case-insensitive exact match
		return `(
			EXISTS (SELECT 1 FROM src_nodes s WHERE s.path = all_paths.path AND UPPER(s.type) = UPPER(?))
			OR EXISTS (SELECT 1 FROM dst_nodes d WHERE d.path = all_paths.path AND UPPER(d.type) = UPPER(?))
		)`, []any{valueStr, valueStr}

	case "traversalStatus":
		// Traversal status filter: case-insensitive exact match
		// Special handling: "excluded" matches both "exclusion_explicit" and "exclusion_inherited"
		valueUpper := strings.ToUpper(valueStr)
		if valueUpper == "EXCLUDED" {
			// Match both exclusion types
			return `(
				EXISTS (SELECT 1 FROM src_nodes s WHERE s.path = all_paths.path AND UPPER(s.traversal_status) IN ('EXCLUSION_EXPLICIT', 'EXCLUSION_INHERITED'))
				OR EXISTS (SELECT 1 FROM dst_nodes d WHERE d.path = all_paths.path AND UPPER(d.traversal_status) IN ('EXCLUSION_EXPLICIT', 'EXCLUSION_INHERITED'))
			)`, []any{}
		}
		// Regular exact match for other statuses
		return `(
			EXISTS (SELECT 1 FROM src_nodes s WHERE s.path = all_paths.path AND UPPER(s.traversal_status) = UPPER(?))
			OR EXISTS (SELECT 1 FROM dst_nodes d WHERE d.path = all_paths.path AND UPPER(d.traversal_status) = UPPER(?))
		)`, []any{valueStr, valueStr}

	case "copyStatus":
		// Copy status filter: case-insensitive exact match
		// Special handling: "excluded" matches both "exclusion_explicit" and "exclusion_inherited"
		valueUpper := strings.ToUpper(valueStr)
		if valueUpper == "EXCLUDED" {
			// Match both exclusion types
			return `(
				EXISTS (SELECT 1 FROM src_nodes s WHERE s.path = all_paths.path AND UPPER(s.copy_status) IN ('EXCLUSION_EXPLICIT', 'EXCLUSION_INHERITED'))
				OR EXISTS (SELECT 1 FROM dst_nodes d WHERE d.path = all_paths.path AND UPPER(d.copy_status) IN ('EXCLUSION_EXPLICIT', 'EXCLUSION_INHERITED'))
			)`, []any{}
		}
		// Regular exact match for other statuses
		return `(
			EXISTS (SELECT 1 FROM src_nodes s WHERE s.path = all_paths.path AND UPPER(s.copy_status) = UPPER(?))
			OR EXISTS (SELECT 1 FROM dst_nodes d WHERE d.path = all_paths.path AND UPPER(d.copy_status) = UPPER(?))
		)`, []any{valueStr, valueStr}

	case "depth":
		// Depth filter: numeric comparison (only available in src_nodes/dst_nodes, use OR logic)
		op := "="
		if cond.Operator != "" {
			op = cond.Operator
		}
		switch op {
		case "equals", "=":
			return `(
				EXISTS (SELECT 1 FROM src_nodes s WHERE s.path = all_paths.path AND s.depth = ?)
				OR EXISTS (SELECT 1 FROM dst_nodes d WHERE d.path = all_paths.path AND d.depth = ?)
			)`, []any{cond.Value, cond.Value}
		case "gt", ">":
			return `(
				EXISTS (SELECT 1 FROM src_nodes s WHERE s.path = all_paths.path AND s.depth > ?)
				OR EXISTS (SELECT 1 FROM dst_nodes d WHERE d.path = all_paths.path AND d.depth > ?)
			)`, []any{cond.Value, cond.Value}
		case "gte", ">=":
			return `(
				EXISTS (SELECT 1 FROM src_nodes s WHERE s.path = all_paths.path AND s.depth >= ?)
				OR EXISTS (SELECT 1 FROM dst_nodes d WHERE d.path = all_paths.path AND d.depth >= ?)
			)`, []any{cond.Value, cond.Value}
		case "lt", "<":
			return `(
				EXISTS (SELECT 1 FROM src_nodes s WHERE s.path = all_paths.path AND s.depth < ?)
				OR EXISTS (SELECT 1 FROM dst_nodes d WHERE d.path = all_paths.path AND d.depth < ?)
			)`, []any{cond.Value, cond.Value}
		case "lte", "<=":
			return `(
				EXISTS (SELECT 1 FROM src_nodes s WHERE s.path = all_paths.path AND s.depth <= ?)
				OR EXISTS (SELECT 1 FROM dst_nodes d WHERE d.path = all_paths.path AND d.depth <= ?)
			)`, []any{cond.Value, cond.Value}
		}
		return "", nil

	case "size":
		// Size filter: numeric comparison (only available in src_nodes/dst_nodes, use OR logic)
		op := "="
		if cond.Operator != "" {
			op = cond.Operator
		}
		switch op {
		case "equals", "=":
			return `(
				EXISTS (SELECT 1 FROM src_nodes s WHERE s.path = all_paths.path AND s.size = ?)
				OR EXISTS (SELECT 1 FROM dst_nodes d WHERE d.path = all_paths.path AND d.size = ?)
			)`, []any{cond.Value, cond.Value}
		case "gt", ">":
			return `(
				EXISTS (SELECT 1 FROM src_nodes s WHERE s.path = all_paths.path AND s.size > ?)
				OR EXISTS (SELECT 1 FROM dst_nodes d WHERE d.path = all_paths.path AND d.size > ?)
			)`, []any{cond.Value, cond.Value}
		case "gte", ">=":
			return `(
				EXISTS (SELECT 1 FROM src_nodes s WHERE s.path = all_paths.path AND s.size >= ?)
				OR EXISTS (SELECT 1 FROM dst_nodes d WHERE d.path = all_paths.path AND d.size >= ?)
			)`, []any{cond.Value, cond.Value}
		case "lt", "<":
			return `(
				EXISTS (SELECT 1 FROM src_nodes s WHERE s.path = all_paths.path AND s.size < ?)
				OR EXISTS (SELECT 1 FROM dst_nodes d WHERE d.path = all_paths.path AND d.size < ?)
			)`, []any{cond.Value, cond.Value}
		case "lte", "<=":
			return `(
				EXISTS (SELECT 1 FROM src_nodes s WHERE s.path = all_paths.path AND s.size <= ?)
				OR EXISTS (SELECT 1 FROM dst_nodes d WHERE d.path = all_paths.path AND d.size <= ?)
			)`, []any{cond.Value, cond.Value}
		}
		return "", nil
	}

	return "", nil // Unknown field, skip
}

// buildConditionClause builds a SQL condition clause for a single SearchCondition
// isStatsQuery: if true, uses simplified field names (path, type, traversal_status) for stats query
//
//	if false, uses full field names (COALESCE(src_path, dst_path), etc.) for main query
func buildConditionClause(cond SearchCondition, isStatsQuery bool) (string, []any) {
	var fieldMap map[string]string

	if isStatsQuery {
		// For stats query: use simplified field names from merged CTE
		fieldMap = map[string]string{
			"name":            "name",
			"path":            "path",
			"depth":           "0",                               // Depth not available in stats query, skip these conditions
			"size":            "COALESCE(src_size, dst_size, 0)", // Use combined size
			"type":            "type",
			"traversalStatus": "traversal_status",
			"copyStatus":      "traversal_status", // Copy status not available in stats, skip
		}
	} else {
		// For main search query: use full COALESCE expressions
		fieldMap = map[string]string{
			"name":            "COALESCE(src_name, dst_name)",
			"path":            "COALESCE(src_path, dst_path)",
			"depth":           "COALESCE(src_depth, dst_depth, 0)",
			"size":            "COALESCE(src_size, dst_size, 0)",
			"type":            "COALESCE(src_type, dst_type)",
			"traversalStatus": "COALESCE(src_traversal_status, dst_traversal_status)",
			"copyStatus":      "COALESCE(src_copy_status, dst_copy_status)",
		}
	}

	sqlField, ok := fieldMap[cond.Field]
	if !ok {
		return "", nil // Unknown field, skip
	}

	// Skip fields that aren't available in stats query
	if isStatsQuery && (cond.Field == "depth" || cond.Field == "copyStatus") {
		return "", nil
	}

	// For path and name fields, always use LIKE '%value%' for substring matching
	// (operator is ignored for these fields)
	if cond.Field == "path" || cond.Field == "name" {
		valueStr := fmt.Sprintf("%v", cond.Value)
		return sqlField + " LIKE ?", []any{"%" + valueStr + "%"}
	}

	// For type: always use case-insensitive exact match
	// (operator is ignored for this field)
	if cond.Field == "type" {
		valueStr := fmt.Sprintf("%v", cond.Value)
		// Use UPPER() for case-insensitive comparison
		return "UPPER(" + sqlField + ") = UPPER(?)", []any{valueStr}
	}

	// For traversalStatus and copyStatus: always use case-insensitive exact match
	// Special handling: "excluded" matches both "exclusion_explicit" and "exclusion_inherited"
	// (operator is ignored for these fields)
	if cond.Field == "traversalStatus" || cond.Field == "copyStatus" {
		valueStr := fmt.Sprintf("%v", cond.Value)
		valueUpper := strings.ToUpper(valueStr)
		if valueUpper == "EXCLUDED" {
			// Match both exclusion types
			return "UPPER(" + sqlField + ") IN ('EXCLUSION_EXPLICIT', 'EXCLUSION_INHERITED')", []any{}
		}
		// Regular exact match for other statuses
		return "UPPER(" + sqlField + ") = UPPER(?)", []any{valueStr}
	}

	// For depth and size: use the operator (numeric comparison)
	switch cond.Operator {
	case "equals", "=", "":
		return sqlField + " = ?", []any{cond.Value}
	case "gt", ">":
		return sqlField + " > ?", []any{cond.Value}
	case "gte", ">=":
		return sqlField + " >= ?", []any{cond.Value}
	case "lt", "<":
		return sqlField + " < ?", []any{cond.Value}
	case "lte", "<=":
		return sqlField + " <= ?", []any{cond.Value}
	case "contains":
		valueStr := fmt.Sprintf("%v", cond.Value)
		return sqlField + " LIKE ?", []any{"%" + valueStr + "%"}
	default:
		return "", nil // Unknown operator, skip
	}
}

// SearchPathReviewStatsDuckDB calculates statistics for search results matching the conditions
func SearchPathReviewStatsDuckDB(ctx context.Context, logger zerolog.Logger, duckdbConn *sql.DB, conditions []SearchCondition) (*PathReviewStats, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	// Build the same base query as SearchPathReviewItemsDuckDB but for stats
	// Include name field so we can filter by name in stats as well
	statsQuery := `
	WITH merged AS (
		SELECT 
			COALESCE(s.path, d.path) AS path,
			COALESCE(s.name, d.name) AS name,
			COALESCE(s.type, d.type) AS type,
			s.size AS src_size,
			d.size AS dst_size,
			COALESCE(s.traversal_status, d.traversal_status) AS traversal_status
		FROM src_nodes s
		FULL OUTER JOIN dst_nodes d ON s.path = d.path
	)
	SELECT 
		COUNT(DISTINCT path) AS total_count,
		COUNT(DISTINCT CASE WHEN type = 'folder' THEN path END) AS folders_count,
		COUNT(DISTINCT CASE WHEN type = 'file' THEN path END) AS files_count,
		COUNT(DISTINCT CASE WHEN traversal_status = 'pending' THEN path END) AS pending_count,
		COUNT(DISTINCT CASE WHEN traversal_status = 'failed' THEN path END) AS failed_count,
		COUNT(DISTINCT CASE WHEN traversal_status IN ('exclusion_explicit', 'exclusion_inherited') THEN path END) AS excluded_count,
		COALESCE(SUM(CASE WHEN type = 'file' THEN src_size ELSE 0 END), 0) AS src_total_size,
		COALESCE(SUM(CASE WHEN type = 'file' THEN dst_size ELSE 0 END), 0) AS dst_total_size
	FROM merged
	`

	// Build WHERE clause from conditions (using stats-specific field names)
	whereClause, args := buildStatsWhereClause(conditions)

	// Always exclude root item (path = '/')
	whereConditions := []string{"path != '/'"}
	if whereClause != "" {
		whereConditions = append(whereConditions, whereClause)
	}
	statsQuery += " WHERE " + strings.Join(whereConditions, " AND ")

	var totalCount, foldersCount, filesCount, pendingCount, failedCount, excludedCount int
	var srcTotalSize, dstTotalSize int64

	err := duckdbConn.QueryRowContext(ctx, statsQuery, args...).Scan(
		&totalCount,
		&foldersCount,
		&filesCount,
		&pendingCount,
		&failedCount,
		&excludedCount,
		&srcTotalSize,
		&dstTotalSize,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to get stats from DuckDB: %w", err)
	}

	// Calculate ratios (rounded to 2 decimal places)
	var foldersRatio, filesRatio float64
	if totalCount > 0 {
		foldersRatio = math.Round(float64(foldersCount)/float64(totalCount)*10000) / 100
		filesRatio = math.Round(float64(filesCount)/float64(totalCount)*10000) / 100
	}

	return &PathReviewStats{
		PendingCount:  pendingCount,
		FailedCount:   failedCount,
		ExcludedCount: excludedCount,
		FoldersCount:  foldersCount,
		FilesCount:    filesCount,
		FoldersRatio:  foldersRatio,
		FilesRatio:    filesRatio,
		TotalFileSize: FileSizeStats{
			Src: srcTotalSize,
			Dst: dstTotalSize,
		},
	}, nil
}
