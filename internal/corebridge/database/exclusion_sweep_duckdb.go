package database

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/rs/zerolog"
)

// RunExclusionSweepDuckDB performs an exclusion sweep using DuckDB
// Finds all nodes with explicit exclusion and propagates to their children using PropagateExclusionDuckDB
func RunExclusionSweepDuckDB(ctx context.Context, logger zerolog.Logger, duckdbConn *sql.DB) error {
	logger.Info().Msg("starting exclusion sweep in DuckDB")

	// Find all nodes in src_nodes that have explicit exclusion (exclusion_inherited status)
	// These are the nodes that need their children to be propagated
	query := `SELECT DISTINCT path FROM src_nodes WHERE traversal_status = 'exclusion_inherited'`
	rows, err := duckdbConn.QueryContext(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to query excluded nodes in src_nodes: %w", err)
	}
	defer rows.Close()

	var excludedPaths []string
	for rows.Next() {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		var path string
		if err := rows.Scan(&path); err != nil {
			logger.Warn().Err(err).Msg("failed to scan excluded path")
			continue
		}
		excludedPaths = append(excludedPaths, path)
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("error iterating excluded paths: %w", err)
	}

	// Propagate exclusion for each excluded node
	for _, path := range excludedPaths {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		if err := PropagateExclusionDuckDB(ctx, logger, duckdbConn, path, true); err != nil {
			logger.Warn().
				Err(err).
				Str("path", path).
				Msg("failed to propagate exclusion for node, continuing with other nodes")
			// Continue with other nodes even if one fails
		}
	}

	// Also check dst_nodes
	query = `SELECT DISTINCT path FROM dst_nodes WHERE traversal_status = 'exclusion_inherited'`
	rows, err = duckdbConn.QueryContext(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to query excluded nodes in dst_nodes: %w", err)
	}
	defer rows.Close()

	excludedPaths = nil
	for rows.Next() {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		var path string
		if err := rows.Scan(&path); err != nil {
			logger.Warn().Err(err).Msg("failed to scan excluded path")
			continue
		}
		excludedPaths = append(excludedPaths, path)
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("error iterating excluded paths: %w", err)
	}

	// Propagate exclusion for each excluded node in dst_nodes
	for _, path := range excludedPaths {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		if err := PropagateExclusionDuckDB(ctx, logger, duckdbConn, path, true); err != nil {
			logger.Warn().
				Err(err).
				Str("path", path).
				Msg("failed to propagate exclusion for node, continuing with other nodes")
			// Continue with other nodes even if one fails
		}
	}

	logger.Info().Msg("exclusion sweep completed in DuckDB")

	return nil
}
