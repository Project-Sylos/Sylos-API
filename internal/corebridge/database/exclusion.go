package database

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/rs/zerolog"
)

// SetNodeExclusionDuckDB sets the exclusion status for a node in DuckDB
func SetNodeExclusionDuckDB(ctx context.Context, logger zerolog.Logger, duckdbConn *sql.DB, nodePath string, excluded bool) error {
	if _, err := duckdbConn.ExecContext(ctx, `UPDATE src_nodes SET excluded = ? WHERE path = ?`, excluded, nodePath); err != nil {
		return fmt.Errorf("failed to update src_nodes: %w", err)
	}
	if _, err := duckdbConn.ExecContext(ctx, `UPDATE dst_nodes SET excluded = ? WHERE path = ?`, excluded, nodePath); err != nil {
		return fmt.Errorf("failed to update dst_nodes: %w", err)
	}

	logger.Info().
		Str("node_path", nodePath).
		Bool("excluded", excluded).
		Msg("updated node exclusion status in DuckDB")

	return nil
}

// PropagateExclusionDuckDB propagates exclusion status to all children in DuckDB
func PropagateExclusionDuckDB(ctx context.Context, logger zerolog.Logger, duckdbConn *sql.DB, parentPath string, excluded bool) error {
	prefix := parentPath + "/"
	result, err := duckdbConn.ExecContext(ctx, `UPDATE src_nodes SET excluded = ? WHERE path LIKE ?`, excluded, prefix+"%")
	if err != nil {
		return fmt.Errorf("failed to propagate exclusion in src_nodes: %w", err)
	}
	rowsAffected, _ := result.RowsAffected()
	_, _ = duckdbConn.ExecContext(ctx, `UPDATE dst_nodes SET excluded = ? WHERE path LIKE ?`, excluded, prefix+"%")
	logger.Info().
		Str("parent_path", parentPath).
		Bool("excluded", excluded).
		Int64("rows_affected", rowsAffected).
		Msg("propagated exclusion to src_nodes children")

	return nil
}
