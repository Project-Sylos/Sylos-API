package database

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/rs/zerolog"
)

// MarkNodeForRetryDuckDB marks a node for retry in DuckDB
func MarkNodeForRetryDuckDB(ctx context.Context, logger zerolog.Logger, duckdbConn *sql.DB, nodePath string) error {
	var exists int
	err := duckdbConn.QueryRowContext(ctx, `SELECT 1 FROM src_nodes WHERE path = ? LIMIT 1`, nodePath).Scan(&exists)
	if err == nil && exists == 1 {
		result, err := duckdbConn.ExecContext(ctx, `UPDATE src_nodes SET traversal_status = 'pending' WHERE path = ?`, nodePath)
		if err != nil {
			return fmt.Errorf("failed to mark src node for retry: %w", err)
		}
		if rows, _ := result.RowsAffected(); rows == 0 {
			logger.Warn().Str("node_path", nodePath).Msg("src node not found or not in failed status")
		}
		_, _ = duckdbConn.ExecContext(ctx, `UPDATE dst_nodes SET traversal_status = 'pending' WHERE parent_path = ? AND traversal_status = 'not_on_src'`, nodePath)
		logger.Info().Str("node_path", nodePath).Bool("is_src", true).Msg("marked src node for retry in DuckDB")
		return nil
	}
	if err != nil && err != sql.ErrNoRows {
		return fmt.Errorf("failed to check src_nodes: %w", err)
	}
	result, err := duckdbConn.ExecContext(ctx, `UPDATE dst_nodes SET traversal_status = 'pending' WHERE path = ?`, nodePath)
	if err != nil {
		return fmt.Errorf("failed to mark dst node for retry: %w", err)
	}
	if rows, _ := result.RowsAffected(); rows == 0 {
		return fmt.Errorf("node with path %s not found", nodePath)
	}
	logger.Info().Str("node_path", nodePath).Bool("is_src", false).Msg("marked dst node for retry in DuckDB")
	return nil
}

// MarkNodeForRetryCopyDuckDB marks a node for copy phase retry
func MarkNodeForRetryCopyDuckDB(ctx context.Context, logger zerolog.Logger, duckdbConn *sql.DB, nodePath string) error {
	result, err := duckdbConn.ExecContext(ctx, `UPDATE src_nodes SET copy_status = 'pending' WHERE path = ? AND copy_status = 'failed'`, nodePath)
	if err != nil {
		return fmt.Errorf("failed to mark src node for copy retry: %w", err)
	}
	rowsAffected, _ := result.RowsAffected()
	if rowsAffected == 0 {
		var exists int
		if err := duckdbConn.QueryRowContext(ctx, `SELECT 1 FROM src_nodes WHERE path = ? LIMIT 1`, nodePath).Scan(&exists); err != nil || exists != 1 {
			return fmt.Errorf("node with path %s not found (copy retry only applies to src nodes)", nodePath)
		}
		return fmt.Errorf("node is not in failed copy status, cannot mark for copy retry")
	}

	logger.Info().
		Str("node_path", nodePath).
		Msg("marked src node for copy retry in DuckDB")

	return nil
}

// UnmarkNodeForRetryCopyDuckDB unmarks a node for copy phase retry
func UnmarkNodeForRetryCopyDuckDB(ctx context.Context, logger zerolog.Logger, duckdbConn *sql.DB, nodePath string) error {
	result, err := duckdbConn.ExecContext(ctx, `UPDATE src_nodes SET copy_status = 'failed' WHERE path = ? AND copy_status = 'pending'`, nodePath)
	if err != nil {
		return fmt.Errorf("failed to unmark src node for copy retry: %w", err)
	}
	rowsAffected, _ := result.RowsAffected()
	if rowsAffected == 0 {
		var exists int
		if err := duckdbConn.QueryRowContext(ctx, `SELECT 1 FROM src_nodes WHERE path = ? LIMIT 1`, nodePath).Scan(&exists); err != nil || exists != 1 {
			return fmt.Errorf("node with path %s not found (copy retry only applies to src nodes)", nodePath)
		}
		return fmt.Errorf("node is not in pending copy status, cannot unmark for copy retry")
	}

	logger.Info().
		Str("node_path", nodePath).
		Msg("unmarked src node for copy retry in DuckDB")

	return nil
}

// MarkAllFailedAsExcludedDuckDB marks all failed items as excluded in DuckDB
func MarkAllFailedAsExcludedDuckDB(ctx context.Context, logger zerolog.Logger, duckdbConn *sql.DB) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}

	rows, err := duckdbConn.QueryContext(ctx, `
		SELECT DISTINCT path FROM (
			SELECT path FROM src_nodes WHERE traversal_status = 'failed'
			UNION
			SELECT path FROM dst_nodes WHERE traversal_status = 'failed'
		) failed_paths`)
	if err != nil {
		return fmt.Errorf("failed to query failed paths: %w", err)
	}
	var failedPaths []string
	for rows.Next() {
		if ctx.Err() != nil {
			rows.Close()
			return ctx.Err()
		}
		var path string
		if err := rows.Scan(&path); err != nil {
			logger.Warn().Err(err).Msg("failed to scan failed path")
			continue
		}
		failedPaths = append(failedPaths, path)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return fmt.Errorf("error iterating failed paths: %w", err)
	}

	result, err := duckdbConn.ExecContext(ctx, `UPDATE src_nodes SET excluded = true WHERE traversal_status = 'failed'`)
	if err != nil {
		return fmt.Errorf("failed to mark failed src as excluded: %w", err)
	}
	srcRows, _ := result.RowsAffected()
	result, err = duckdbConn.ExecContext(ctx, `UPDATE dst_nodes SET excluded = true WHERE traversal_status = 'failed'`)
	if err != nil {
		return fmt.Errorf("failed to mark failed dst as excluded: %w", err)
	}
	dstRows, _ := result.RowsAffected()

	logger.Info().
		Int64("src_rows_affected", srcRows).
		Int64("dst_rows_affected", dstRows).
		Int("paths_to_propagate", len(failedPaths)).
		Msg("marked all failed items as excluded")

	for _, path := range failedPaths {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err := PropagateExclusionDuckDB(ctx, logger, duckdbConn, path, true); err != nil {
			logger.Warn().
				Err(err).
				Str("path", path).
				Msg("failed to propagate exclusion for path")
		}
	}

	return nil
}

// RetryAllFailedDuckDB marks all failed items for retry in DuckDB
func RetryAllFailedDuckDB(ctx context.Context, logger zerolog.Logger, duckdbConn *sql.DB) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}

	result, err := duckdbConn.ExecContext(ctx, `UPDATE src_nodes SET traversal_status = 'pending' WHERE traversal_status = 'failed'`)
	if err != nil {
		return fmt.Errorf("failed to mark src failed for retry: %w", err)
	}
	srcRows, _ := result.RowsAffected()
	result, err = duckdbConn.ExecContext(ctx, `UPDATE dst_nodes SET traversal_status = 'pending' WHERE traversal_status = 'failed'`)
	if err != nil {
		return fmt.Errorf("failed to mark dst failed for retry: %w", err)
	}
	dstRows, _ := result.RowsAffected()
	logger.Info().Int64("src_rows_affected", srcRows).Int64("dst_rows_affected", dstRows).Msg("marked all failed as pending")

	_, err = duckdbConn.ExecContext(ctx, `
	UPDATE dst_nodes SET traversal_status = 'pending'
	WHERE traversal_status = 'not_on_src'
	  AND parent_path IN (SELECT path FROM src_nodes WHERE traversal_status = 'pending')`)
	if err != nil {
		return fmt.Errorf("failed to mark dst children for retry: %w", err)
	}
	return nil
}

// UnmarkNodeForRetryDuckDB unmarks a node for retry in DuckDB
func UnmarkNodeForRetryDuckDB(ctx context.Context, logger zerolog.Logger, duckdbConn *sql.DB, nodePath string) error {
	var exists int
	err := duckdbConn.QueryRowContext(ctx, `SELECT 1 FROM src_nodes WHERE path = ? LIMIT 1`, nodePath).Scan(&exists)
	if err == nil && exists == 1 {
		result, err := duckdbConn.ExecContext(ctx, `UPDATE src_nodes SET traversal_status = 'failed' WHERE path = ?`, nodePath)
		if err != nil {
			return fmt.Errorf("failed to unmark src node for retry: %w", err)
		}
		srcRows, _ := result.RowsAffected()
		if srcRows == 0 {
			return fmt.Errorf("node is not in pending status, cannot unmark for retry")
		}
		_, _ = duckdbConn.ExecContext(ctx, `UPDATE dst_nodes SET traversal_status = 'not_on_src' WHERE parent_path = ? AND traversal_status = 'pending'`, nodePath)
		logger.Info().Str("node_path", nodePath).Bool("is_src", true).Msg("unmarked src node for retry in DuckDB")
		return nil
	}
	if err != nil && err != sql.ErrNoRows {
		return fmt.Errorf("failed to check src_nodes: %w", err)
	}
	result, err := duckdbConn.ExecContext(ctx, `UPDATE dst_nodes SET traversal_status = 'failed' WHERE path = ?`, nodePath)
	if err != nil {
		return fmt.Errorf("failed to unmark dst node for retry: %w", err)
	}
	dstRows, _ := result.RowsAffected()
	if dstRows == 0 {
		return fmt.Errorf("node with path %s not found or not in pending status", nodePath)
	}
	logger.Info().Str("node_path", nodePath).Bool("is_src", false).Msg("unmarked dst node for retry in DuckDB")
	return nil
}

// CountPendingRetriesDuckDB counts the number of pending items in DuckDB
func CountPendingRetriesDuckDB(ctx context.Context, logger zerolog.Logger, duckdbConn *sql.DB) (int, error) {
	if ctx.Err() != nil {
		return 0, ctx.Err()
	}

	query := `
	SELECT COUNT(DISTINCT path) FROM (
		SELECT path FROM src_nodes WHERE traversal_status = 'pending'
		UNION
		SELECT path FROM dst_nodes WHERE traversal_status = 'pending'
	) pending_paths`

	var count int
	err := duckdbConn.QueryRowContext(ctx, query).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to count pending retries: %w", err)
	}

	return count, nil
}
