package database

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/rs/zerolog"
)

// GetChildrenDiffsFromDuckDB retrieves merged children from both SRC and DST using DuckDB
// Uses direct path-based joins instead of path hashing
// sortField: field to sort by ("name", "path", "depth", "size", "type", "traversalStatus", etc.)
// sortDir: sort direction ("asc" or "desc", defaults to "asc")
// reviewPhase: "traversal" or "copy" - determines how status is computed
// Returns items, pagination info, and stats (stats can be nil if calculation fails)
func GetChildrenDiffsFromDuckDB(ctx context.Context, logger zerolog.Logger, duckdbConn *sql.DB, parentPath string, offset, limit int, foldersOnly bool, sortField, sortDir string, reviewPhase string) (map[string]PathNodes, PaginationInfo, error) {
	// Check for context cancellation
	if ctx.Err() != nil {
		return map[string]PathNodes{}, PaginationInfo{}, ctx.Err()
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
		SELECT path, parent_path, name, type, size, mtime, depth, traversal_status, id, parent_id, service_id, parent_service_id
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
		return nil, PaginationInfo{}, fmt.Errorf("failed to query DuckDB for children: %w", err)
	}
	defer rows.Close()

	// Build result map
	items := make(map[string]PathNodes)

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
		var srcCopyStatus sql.NullString
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
			&srcCopyStatus,
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
			}
		}

		items[pathKey] = pn
	}

	if err := rows.Err(); err != nil {
		return nil, PaginationInfo{}, fmt.Errorf("error iterating DuckDB rows: %w", err)
	}

	total := 0
	// do a sql query to get the total number of items
	totalQuery := `SELECT COUNT(*) FROM (SELECT path FROM src_nodes WHERE parent_path = ? UNION SELECT path FROM dst_nodes WHERE parent_path = ?) AS all_children`
	err = duckdbConn.QueryRowContext(ctx, totalQuery, parentPath, parentPath).Scan(&total)
	if err != nil {
		return nil, PaginationInfo{}, fmt.Errorf("failed to get total count from DuckDB: %w", err)
	}

	// Get folders and files count from stats if available, otherwise count from paginated items
	var foldersCount, filesCount int

	pagination := PaginationInfo{
		Offset:       offset,
		Limit:        limit,
		Total:        total,
		TotalFolders: foldersCount,
		TotalFiles:   filesCount,
		HasMore:      (offset + limit) < total,
	}

	return items, pagination, nil
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
// For src nodes: updates both traversal_status and copy_status (exclusion primarily means "don't copy")
// For dst nodes: only updates traversal_status (dst nodes don't have copy_status and shouldn't be excluded)
// When excluding, sets immediate node to 'exclusion_explicit', children get 'exclusion_inherited' via propagation
// If src node has a corresponding dst node, also updates the dst node's traversal_status
func SetNodeExclusionDuckDB(ctx context.Context, logger zerolog.Logger, duckdbConn *sql.DB, nodePath string, excluded bool) error {
	// Determine the new status
	// For exclusion: immediate node gets 'exclusion_explicit', children get 'exclusion_inherited' via propagation
	// For unexclusion: immediate node and children get 'pending'
	newStatus := "pending"
	if excluded {
		newStatus = "exclusion_explicit"
	}

	// Update src_nodes: both traversal_status and copy_status
	// This is the primary use case - exclusion means "don't copy this src item to dst"
	updateSrcQuery := `
	UPDATE src_nodes 
	SET traversal_status = ?, copy_status = ?
	WHERE path = ?
	`
	_, err := duckdbConn.ExecContext(ctx, updateSrcQuery, newStatus, newStatus, nodePath)
	if err != nil {
		return fmt.Errorf("failed to update src_nodes: %w", err)
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
// For src_nodes: updates both traversal_status and copy_status (exclusion means "don't copy")
// For dst_nodes: only updates traversal_status (for consistency, though dst nodes don't need exclusion)
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
	// Update both traversal_status and copy_status for src_nodes
	updateSrcQuery := `
	UPDATE src_nodes 
	SET traversal_status = ?, copy_status = ?
	WHERE STARTS_WITH(path, ? || '/')
	`

	result, err := duckdbConn.ExecContext(ctx, updateSrcQuery, newStatus, newStatus, parentPath)
	if err != nil {
		return fmt.Errorf("failed to propagate exclusion in src_nodes: %w", err)
	}

	srcRowsAffected, _ := result.RowsAffected()
	logger.Info().
		Str("parent_path", parentPath).
		Bool("excluded", excluded).
		Int64("src_rows_affected", srcRowsAffected).
		Msg("propagated exclusion to src_nodes children (traversal_status and copy_status)")

	return nil
}

// MarkNodeForRetryDuckDB marks a node for retry in DuckDB
// For src nodes: marks the src node as pending, and if it has a corresponding dst node,
// marks all dst children with not_on_src status as pending too.
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
		WHERE path = ?
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
				// Mark all dst children with not_on_src status as pending
				updateDstChildrenQuery := `
				UPDATE dst_nodes 
				SET traversal_status = 'pending'
				WHERE parent_path = ? AND traversal_status = 'not_on_src'
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
						Msg("marked dst children with not_on_src status as pending for retry")
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
		WHERE path = ?
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

// MarkNodeForRetryCopyDuckDB marks a node for copy phase retry
// Only src nodes have copy_status field, so this only updates src_nodes
// Sets copy_status from 'failed' to 'pending' for copy phase retry
func MarkNodeForRetryCopyDuckDB(ctx context.Context, logger zerolog.Logger, duckdbConn *sql.DB, nodePath string) error {
	// Check if node exists in src_nodes (only src nodes have copy_status)
	checkSrcQuery := `SELECT id FROM src_nodes WHERE path = ? LIMIT 1`
	var srcID sql.NullString
	err := duckdbConn.QueryRowContext(ctx, checkSrcQuery, nodePath).Scan(&srcID)
	if err != nil {
		if err == sql.ErrNoRows {
			return fmt.Errorf("node with path %s not found in src_nodes (copy retry only applies to src nodes)", nodePath)
		}
		return fmt.Errorf("failed to check src_nodes: %w", err)
	}

	// Update src node copy_status from "failed" to "pending"
	updateSrcQuery := `
	UPDATE src_nodes 
	SET copy_status = 'pending'
	WHERE path = ? AND copy_status = 'failed'
	`
	result, err := duckdbConn.ExecContext(ctx, updateSrcQuery, nodePath)
	if err != nil {
		return fmt.Errorf("failed to mark src node for copy retry: %w", err)
	}

	rowsAffected, _ := result.RowsAffected()
	if rowsAffected == 0 {
		logger.Warn().
			Str("node_path", nodePath).
			Msg("src node not found or copy_status not in 'failed' status")
		return fmt.Errorf("node is not in failed copy status, cannot mark for copy retry")
	}

	logger.Info().
		Str("node_path", nodePath).
		Msg("marked src node for copy retry in DuckDB")

	return nil
}

// UnmarkNodeForRetryCopyDuckDB unmarks a node for copy phase retry
// Only src nodes have copy_status field, so this only updates src_nodes
// Sets copy_status from 'pending' back to 'failed'
func UnmarkNodeForRetryCopyDuckDB(ctx context.Context, logger zerolog.Logger, duckdbConn *sql.DB, nodePath string) error {
	// Check if node exists in src_nodes (only src nodes have copy_status)
	checkSrcQuery := `SELECT id FROM src_nodes WHERE path = ? LIMIT 1`
	var srcID sql.NullString
	err := duckdbConn.QueryRowContext(ctx, checkSrcQuery, nodePath).Scan(&srcID)
	if err != nil {
		if err == sql.ErrNoRows {
			return fmt.Errorf("node with path %s not found in src_nodes (copy retry only applies to src nodes)", nodePath)
		}
		return fmt.Errorf("failed to check src_nodes: %w", err)
	}

	// Update src node copy_status from "pending" back to "failed"
	updateSrcQuery := `
	UPDATE src_nodes 
	SET copy_status = 'failed'
	WHERE path = ? AND copy_status = 'pending'
	`
	result, err := duckdbConn.ExecContext(ctx, updateSrcQuery, nodePath)
	if err != nil {
		return fmt.Errorf("failed to unmark src node for copy retry: %w", err)
	}

	rowsAffected, _ := result.RowsAffected()
	if rowsAffected == 0 {
		logger.Warn().
			Str("node_path", nodePath).
			Msg("src node not found or copy_status not in 'pending' status")
		return fmt.Errorf("node is not in pending copy status, cannot unmark for copy retry")
	}

	logger.Info().
		Str("node_path", nodePath).
		Msg("unmarked src node for copy retry in DuckDB")

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
	// For src_nodes: update both traversal_status and copy_status (exclusion means "don't copy")
	updateSrcQuery := `
	UPDATE src_nodes 
	SET traversal_status = 'exclusion_explicit', copy_status = 'exclusion_explicit'
	WHERE traversal_status = 'failed'
	`
	result, err := duckdbConn.ExecContext(ctx, updateSrcQuery)
	if err != nil {
		return fmt.Errorf("failed to mark failed src nodes as excluded: %w", err)
	}
	srcRowsAffected, _ := result.RowsAffected()

	// For dst_nodes: only update traversal_status (dst nodes don't have copy_status)
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
// 3. Marks all dst children with not_on_src status as pending
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
	// find their corresponding dst paths and mark dst children with not_on_src as pending
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
	  AND traversal_status = 'not_on_src'
	`

	result, err = duckdbConn.ExecContext(ctx, updateDstChildrenQuery)
	if err != nil {
		return fmt.Errorf("failed to mark dst children for retry: %w", err)
	}

	dstRowsAffected, _ := result.RowsAffected()
	logger.Info().
		Int64("dst_children_rows_affected", dstRowsAffected).
		Msg("marked dst children with not_on_src status as pending for retry")

	return nil
}

// UnmarkNodeForRetryDuckDB unmarks a node for retry in DuckDB
// For src nodes: marks the src node as failed, and if it has a corresponding dst node,
// marks all dst children with pending status as not_on_src again.
// For dst nodes: just marks the dst node as failed
func UnmarkNodeForRetryDuckDB(ctx context.Context, logger zerolog.Logger, duckdbConn *sql.DB, nodePath string) error {
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
		// Update src node from "pending" to "failed"
		updateSrcQuery := `
		UPDATE src_nodes 
		SET traversal_status = 'failed'
		WHERE path = ?
		`
		result, err := duckdbConn.ExecContext(ctx, updateSrcQuery, nodePath)
		if err != nil {
			return fmt.Errorf("failed to unmark src node for retry: %w", err)
		}

		srcRowsAffected, _ := result.RowsAffected()
		if srcRowsAffected == 0 {
			logger.Warn().
				Str("node_path", nodePath).
				Msg("src node not found or not in 'pending' status")
		}

		// If src node has a corresponding dst node, mark its children (that are pending) as not_on_src again
		if dstID.Valid && dstID.String != "" {
			// Find the dst node path using dst_id
			var dstPath string
			getDstPathQuery := `SELECT path FROM dst_nodes WHERE id = ? LIMIT 1`
			err = duckdbConn.QueryRowContext(ctx, getDstPathQuery, dstID.String).Scan(&dstPath)
			if err == nil {
				// Mark all dst children with pending status as not_on_src again
				updateDstChildrenQuery := `
				UPDATE dst_nodes 
				SET traversal_status = 'not_on_src'
				WHERE parent_path = ? AND traversal_status = 'pending'
				`
				result, err = duckdbConn.ExecContext(ctx, updateDstChildrenQuery, dstPath)
				if err != nil {
					logger.Warn().
						Err(err).
						Str("dst_path", dstPath).
						Msg("failed to update dst children for unmark retry")
				} else {
					dstChildrenAffected, _ := result.RowsAffected()
					if dstChildrenAffected > 0 {
						logger.Info().
							Str("src_path", nodePath).
							Str("dst_path", dstPath).
							Int64("dst_children_updated", dstChildrenAffected).
							Msg("marked dst children back to not_on_src status")
					}
				}
			} else if err != sql.ErrNoRows {
				logger.Warn().
					Err(err).
					Str("dst_id", dstID.String).
					Msg("failed to find corresponding dst node path")
			}
		}

		// If no rows were affected in src_nodes, the node is not in pending status
		if srcRowsAffected == 0 {
			return fmt.Errorf("node is not in pending status, cannot unmark for retry")
		}

		logger.Info().
			Str("node_path", nodePath).
			Int64("src_rows_affected", srcRowsAffected).
			Bool("is_src", true).
			Msg("unmarked src node for retry in DuckDB")
	} else {
		// For dst nodes, just mark the node as failed
		updateDstQuery := `
		UPDATE dst_nodes 
		SET traversal_status = 'failed'
		WHERE path = ?
		`
		result, err := duckdbConn.ExecContext(ctx, updateDstQuery, nodePath)
		if err != nil {
			return fmt.Errorf("failed to unmark dst node for retry: %w", err)
		}

		dstRowsAffected, _ := result.RowsAffected()
		if dstRowsAffected == 0 {
			return fmt.Errorf("node is not in pending status, cannot unmark for retry")
		}

		logger.Info().
			Str("node_path", nodePath).
			Int64("dst_rows_affected", dstRowsAffected).
			Bool("is_src", false).
			Msg("unmarked dst node for retry in DuckDB")
	}

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
// statusSearchType specifies which status type(s) to search by when using status conditions: "traversal", "copy", or "both" (default: "both")
// reviewPhase: "traversal" or "copy" - determines how status filtering works (in copy phase, "status" field maps to copy_status)
func SearchPathReviewItemsDuckDB(ctx context.Context, logger zerolog.Logger, duckdbConn *sql.DB, conditions []SearchCondition, offset, limit int, sortField, sortDir string, statusSearchType string, reviewPhase string) (map[string]PathNodes, PaginationInfo, error) {
	// Check for context cancellation
	if ctx.Err() != nil {
		return map[string]PathNodes{}, PaginationInfo{}, ctx.Err()
	}

	// Normalize statusSearchType: default to "both" if empty or invalid
	normalizedStatusType := normalizeStatusSearchType(statusSearchType)

	// In copy phase, if statusSearchType is not explicitly set, default to "copy"
	if reviewPhase == "copy" && normalizedStatusType == "both" {
		normalizedStatusType = "copy"
	}

	// Build WHERE clause filters for path selection phase (using EXISTS)
	pathFilterClause, pathFilterArgs := buildPathFilterClause(conditions, normalizedStatusType, reviewPhase)

	// For path selection (Phase A), we can only sort by path itself
	// For other sort fields, we'll sort after the join in Phase B
	direction := "ASC"
	if sortDir == "desc" || sortDir == "DESC" {
		direction = "DESC"
	}
	pathOrderBy := "path " + direction

	// Build the core query (with pagination)
	mainQuery := `
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

	// Build the total count query (with SAME FILTERS as main, but without limit/offset)
	totalQuery := `
	WITH all_paths AS (
		SELECT DISTINCT path FROM src_nodes
		UNION
		SELECT DISTINCT path FROM dst_nodes
	),
	filtered_paths AS (
		SELECT path
		FROM all_paths
		WHERE path != '/'` + pathFilterClause + `
	)
	SELECT COUNT(*) FROM filtered_paths`

	// If sorting by a field other than path, apply final sort after join
	query := mainQuery
	if sortField != "" && sortField != "path" {
		finalOrderBy := buildOrderByClause(sortField, sortDir)
		query += " ORDER BY " + finalOrderBy
	} else {
		query += " ORDER BY path"
	}

	// Build args: pathFilterArgs, then limit, offset
	args := append(pathFilterArgs, limit, offset)

	// Execute query for results
	rows, err := duckdbConn.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, PaginationInfo{}, fmt.Errorf("failed to query DuckDB for search: %w", err)
	}
	defer rows.Close()

	// Build result map (same structure as GetChildrenDiffsFromDuckDB)
	items := make(map[string]PathNodes)

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
			return nil, PaginationInfo{}, fmt.Errorf("failed to scan row: %w", err)
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
		return nil, PaginationInfo{}, fmt.Errorf("error iterating DuckDB rows: %w", err)
	}

	total := 0

	// Get total count for pagination (same filters as the paged query)
	err = duckdbConn.QueryRowContext(ctx, totalQuery, pathFilterArgs...).Scan(&total)
	if err != nil {
		return nil, PaginationInfo{}, fmt.Errorf("failed to get total count from DuckDB: %w", err)
	}

	// Count folders and files from items or stats
	var totalFolders, totalFiles int

	hasMore := offset+limit < total

	pagination := PaginationInfo{
		Offset:       offset,
		Limit:        limit,
		Total:        total,
		TotalFolders: totalFolders,
		TotalFiles:   totalFiles,
		HasMore:      hasMore,
	}

	return items, pagination, nil
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

// normalizeStatusSearchType normalizes the status search type value
// Returns "traversal", "copy", or "both" (default: "both")
func normalizeStatusSearchType(statusType string) string {
	statusType = strings.ToLower(strings.TrimSpace(statusType))
	switch statusType {
	case "traversal":
		return "traversal"
	case "copy":
		return "copy"
	case "both":
		return "both"
	default:
		return "both" // Default to both if empty or invalid
	}
}

// buildPathFilterClause builds EXISTS-based WHERE clause filters for path selection phase
// Returns SQL clause and arguments for filtering paths before pagination
// Uses EXISTS to check conditions against src_nodes and/or dst_nodes
// statusSearchType specifies which status type(s) to check: "traversal", "copy", or "both"
// reviewPhase: "traversal" or "copy" - determines how "status" field is interpreted
func buildPathFilterClause(conditions []SearchCondition, statusSearchType string, reviewPhase string) (string, []any) {
	if len(conditions) == 0 {
		return "", nil
	}

	var clauses []string
	var args []any

	for _, cond := range conditions {
		clause, clauseArgs := buildPathFilterCondition(cond, statusSearchType, reviewPhase)
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
// statusSearchType specifies which status type(s) to check: "traversal", "copy", or "both"
// reviewPhase: "traversal" or "copy" - determines how "status" field is interpreted
// If statusSearchType is "traversal", only traversalStatus conditions are processed
// If statusSearchType is "copy", only copyStatus conditions are processed
// If statusSearchType is "both", both traversalStatus and copyStatus conditions are processed
// In copy phase, "status" field maps to copy_status; "successful" also includes dst-only nodes
func buildPathFilterCondition(cond SearchCondition, statusSearchType string, reviewPhase string) (string, []any) {
	valueStr := fmt.Sprintf("%v", cond.Value)

	// In copy phase, map "status" field to "copyStatus"
	if reviewPhase == "copy" && cond.Field == "status" {
		cond.Field = "copyStatus"
	}

	// Filter status conditions based on statusSearchType
	if cond.Field == "traversalStatus" && statusSearchType == "copy" {
		// Skip traversalStatus conditions when only searching by copy status
		return "", nil
	}
	if cond.Field == "copyStatus" && statusSearchType == "traversal" {
		// Skip copyStatus conditions when only searching by traversal status
		return "", nil
	}

	switch cond.Field {
	case "path":
		// Path filter: case-insensitive check if path matches pattern in either table
		return `(
			EXISTS (SELECT 1 FROM src_nodes s WHERE s.path = all_paths.path AND lower(s.path) LIKE '%' || lower(?) || '%')
			OR EXISTS (SELECT 1 FROM dst_nodes d WHERE d.path = all_paths.path AND lower(d.path) LIKE '%' || lower(?) || '%')
		)`, []any{valueStr, valueStr}

	case "name":
		// Name filter: case-insensitive check if name matches pattern in either table
		return `(
			EXISTS (SELECT 1 FROM src_nodes s WHERE s.path = all_paths.path AND lower(s.name) LIKE '%' || lower(?) || '%')
			OR EXISTS (SELECT 1 FROM dst_nodes d WHERE d.path = all_paths.path AND lower(d.name) LIKE '%' || lower(?) || '%')
		)`, []any{valueStr, valueStr}

	case "type":
		// Type filter: case-insensitive exact match
		return `(
			EXISTS (SELECT 1 FROM src_nodes s WHERE s.path = all_paths.path AND UPPER(s.type) = UPPER(?))
			OR EXISTS (SELECT 1 FROM dst_nodes d WHERE d.path = all_paths.path AND UPPER(d.type) = UPPER(?))
		)`, []any{valueStr, valueStr}

	case "traversalStatus":
		// Traversal status filter: case-insensitive exact match
		// Special handling: "excluded" matches both "exclusion_explicit" and "exclusion_inherited"
		return buildTraversalStatusFilter(valueStr)

	case "copyStatus":
		// Copy status filter: case-insensitive exact match
		// Special handling: "excluded" matches both "exclusion_explicit" and "exclusion_inherited"
		// In copy phase, "successful" also includes dst-only nodes
		return buildCopyStatusFilter(valueStr, reviewPhase)

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

// buildTraversalStatusFilter builds a filter for traversal status
func buildTraversalStatusFilter(valueStr string) (string, []any) {
	valueUpper := strings.ToUpper(valueStr)
	if valueUpper == "EXCLUDED" {
		// Match both exclusion types
		return `(
			EXISTS (SELECT 1 FROM src_nodes s WHERE s.path = all_paths.path AND UPPER(s.traversal_status) IN ('EXCLUSION_EXPLICIT', 'EXCLUSION_INHERITED'))
			OR EXISTS (SELECT 1 FROM dst_nodes d WHERE d.path = all_paths.path AND UPPER(d.traversal_status) IN ('EXCLUSION_EXPLICIT', 'EXCLUSION_INHERITED'))
		)`, []any{}
	}

	// Special handling for "not_on_src" - this status should not appear in traversal_status,
	if valueUpper == "NOT_ON_SRC" {
		// Items with "not_on_src" only exist in dst_nodes, not src_nodes
		return `EXISTS (SELECT 1 FROM dst_nodes d WHERE d.path = all_paths.path AND UPPER(d.traversal_status) = 'NOT_ON_SRC')`, []any{}
	}

	// Regular exact match for other statuses (can exist in either table)
	return `(
		EXISTS (SELECT 1 FROM src_nodes s WHERE s.path = all_paths.path AND UPPER(s.traversal_status) = UPPER(?))
		OR EXISTS (SELECT 1 FROM dst_nodes d WHERE d.path = all_paths.path AND UPPER(d.traversal_status) = UPPER(?))
	)`, []any{valueStr, valueStr}
}

// buildCopyStatusFilter builds a filter for copy status
// reviewPhase: "traversal" or "copy" - in copy phase, "successful" includes dst-only nodes
func buildCopyStatusFilter(valueStr string, reviewPhase string) (string, []any) {
	valueUpper := strings.ToUpper(valueStr)
	if valueUpper == "EXCLUDED" {
		// Match both exclusion types (but exclusion shouldn't be used in copy phase)
		return `(
			EXISTS (SELECT 1 FROM src_nodes s WHERE s.path = all_paths.path AND UPPER(s.copy_status) IN ('EXCLUSION_EXPLICIT', 'EXCLUSION_INHERITED'))
			OR EXISTS (SELECT 1 FROM dst_nodes d WHERE d.path = all_paths.path AND UPPER(d.copy_status) IN ('EXCLUSION_EXPLICIT', 'EXCLUSION_INHERITED'))
		)`, []any{}
	}

	// In copy phase, "successful" includes dst-only nodes (nodes that exist in dst but not in src)
	if reviewPhase == "copy" && valueUpper == "SUCCESSFUL" {
		return `(
			EXISTS (SELECT 1 FROM src_nodes s WHERE s.path = all_paths.path AND UPPER(s.copy_status) = 'SUCCESSFUL')
			OR EXISTS (SELECT 1 FROM dst_nodes d WHERE d.path = all_paths.path AND NOT EXISTS (SELECT 1 FROM src_nodes s WHERE s.path = d.path))
		)`, []any{}
	}

	// Special handling for "not_on_src" - this status should not appear in copy_status,
	// but handle them correctly if they do (legacy data or edge cases)
	if valueUpper == "NOT_ON_SRC" {
		// Items with "not_on_src" only exist in dst_nodes
		return `EXISTS (SELECT 1 FROM dst_nodes d WHERE d.path = all_paths.path AND UPPER(d.copy_status) = 'NOT_ON_SRC')`, []any{}
	}

	// Regular exact match for other statuses
	return `(
		EXISTS (SELECT 1 FROM src_nodes s WHERE s.path = all_paths.path AND UPPER(s.copy_status) = UPPER(?))
		OR EXISTS (SELECT 1 FROM dst_nodes d WHERE d.path = all_paths.path AND UPPER(d.copy_status) = UPPER(?))
	)`, []any{valueStr, valueStr}
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

// CountPendingRetriesDuckDB counts the number of pending items in DuckDB
// Returns the total count of items with traversal_status = 'pending' across both src_nodes and dst_nodes
func CountPendingRetriesDuckDB(ctx context.Context, logger zerolog.Logger, duckdbConn *sql.DB) (int, error) {
	// Check for context cancellation
	if ctx.Err() != nil {
		return 0, ctx.Err()
	}

	// Count items with 'pending' traversal_status from both src_nodes and dst_nodes
	// Use UNION to get unique paths, then count
	query := `
	SELECT COUNT(DISTINCT path) 
	FROM (
		SELECT path FROM src_nodes WHERE traversal_status = 'pending'
		UNION
		SELECT path FROM dst_nodes WHERE traversal_status = 'pending'
	) AS pending_paths
	`

	var count int
	err := duckdbConn.QueryRowContext(ctx, query).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to count pending retries: %w", err)
	}

	return count, nil
}
