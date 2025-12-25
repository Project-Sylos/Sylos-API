package database

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/rs/zerolog"
)

// GetChildrenDiffsFromDuckDB retrieves merged children from both SRC and DST using DuckDB
// Uses direct path-based joins instead of path hashing
func GetChildrenDiffsFromDuckDB(ctx context.Context, logger zerolog.Logger, duckdbConn *sql.DB, parentPath string, offset, limit int, foldersOnly bool) (map[string]PathNodes, PaginationInfo, error) {
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

	// Add ordering and pagination
	query += ` ORDER BY COALESCE(src_name, dst_name) LIMIT ? OFFSET ?`

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
				Name:     getStringValue(srcName),
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
		return nil, PaginationInfo{}, fmt.Errorf("error iterating DuckDB rows: %w", err)
	}

	// Get total count for pagination
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

	var total int
	err = duckdbConn.QueryRowContext(ctx, totalQuery, parentPath, parentPath).Scan(&total)
	if err != nil {
		logger.Warn().Err(err).Msg("failed to get total count from DuckDB")
		total = len(items) // Fallback to items count
	}

	// Count folders and files
	foldersCount := 0
	filesCount := 0
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
// Updates the immediate node's traversal_status, then triggers background propagation
func SetNodeExclusionDuckDB(ctx context.Context, logger zerolog.Logger, duckdbConn *sql.DB, nodePath string, excluded bool) error {
	// Determine the new status
	newStatus := "pending"
	if excluded {
		newStatus = "exclusion_inherited"
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
