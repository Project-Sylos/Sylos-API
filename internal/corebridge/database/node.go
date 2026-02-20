package database

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/rs/zerolog"
)

// FindNodePathByIDDuckDB finds the path of a node by its node ID (deterministic ID from the engine) in DuckDB
func FindNodePathByIDDuckDB(ctx context.Context, logger zerolog.Logger, duckdbConn *sql.DB, nodeID string) (string, error) {
	var path string
	err := duckdbConn.QueryRowContext(ctx, `SELECT path FROM src_nodes WHERE id = ? LIMIT 1`, nodeID).Scan(&path)
	if err == nil {
		return path, nil
	}
	if err != sql.ErrNoRows {
		return "", fmt.Errorf("failed to query src_nodes: %w", err)
	}
	err = duckdbConn.QueryRowContext(ctx, `SELECT path FROM dst_nodes WHERE id = ? LIMIT 1`, nodeID).Scan(&path)
	if err != nil {
		if err == sql.ErrNoRows {
			return "", fmt.Errorf("node with ID %s not found", nodeID)
		}
		return "", fmt.Errorf("failed to query dst_nodes: %w", err)
	}
	return path, nil
}
