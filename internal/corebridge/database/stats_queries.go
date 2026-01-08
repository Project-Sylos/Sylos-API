package database

import (
	"context"
	"database/sql"
	"fmt"
	"math"

	"github.com/rs/zerolog"
)

// PathReviewStats represents statistics for path review
type PathReviewStats struct {
	PendingCount        int           `json:"pendingCount"`
	FailedCount         int           `json:"failedCount"`
	ExcludedCount       int           `json:"excludedCount"`
	PendingRetriesCount int           `json:"pendingRetriesCount"` // Count of items with traversal_status = 'pending'
	FoldersCount        int           `json:"foldersCount"`
	FilesCount          int           `json:"filesCount"`
	FoldersRatio        float64       `json:"foldersRatio"` // Rounded to 2 decimal places
	FilesRatio          float64       `json:"filesRatio"`   // Rounded to 2 decimal places
	TotalFileSize       FileSizeStats `json:"totalFileSize"`
}

// FileSizeStats represents file size statistics grouped by src/dst
type FileSizeStats struct {
	Src int64 `json:"src"`
	Dst int64 `json:"dst"`
}

// GetPathReviewStatsDuckDB retrieves comprehensive statistics for path review
// Counts are merged (unique paths across both src_nodes and dst_nodes)
func GetPathReviewStatsDuckDB(ctx context.Context, logger zerolog.Logger, duckdbConn *sql.DB) (*PathReviewStats, error) {
	// Check for context cancellation
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	stats := &PathReviewStats{}

	// Query for pending items (merged count of unique paths)
	pendingQuery := `
	SELECT COUNT(DISTINCT path) 
	FROM (
		SELECT path FROM src_nodes WHERE copy_status = 'pending'
		UNION
		SELECT path FROM dst_nodes WHERE copy_status = 'pending'
	) AS pending_paths
	`
	err := duckdbConn.QueryRowContext(ctx, pendingQuery).Scan(&stats.PendingCount)
	if err != nil {
		return nil, fmt.Errorf("failed to query pending count: %w", err)
	}

	// Query for failed items (merged count of unique paths)
	// This is the only one that should be traversal status actually.
	failedQuery := `
	SELECT COUNT(DISTINCT path) 
	FROM (
		SELECT path FROM src_nodes WHERE traversal_status = 'failed'
		UNION
		SELECT path FROM dst_nodes WHERE traversal_status = 'failed'
	) AS failed_paths
	`
	err = duckdbConn.QueryRowContext(ctx, failedQuery).Scan(&stats.FailedCount)
	if err != nil {
		return nil, fmt.Errorf("failed to query failed count: %w", err)
	}

	// Query for excluded items (merged count of unique paths)
	excludedQuery := `
	SELECT COUNT(DISTINCT path) 
	FROM (
		SELECT path FROM src_nodes WHERE copy_status IN ('exclusion_explicit', 'exclusion_inherited')
		UNION
		SELECT path FROM dst_nodes WHERE copy_status IN ('exclusion_explicit', 'exclusion_inherited')
	) AS excluded_paths
	`
	err = duckdbConn.QueryRowContext(ctx, excludedQuery).Scan(&stats.ExcludedCount)
	if err != nil {
		return nil, fmt.Errorf("failed to query excluded count: %w", err)
	}

	// Query for pending retries (items with traversal_status = 'pending')
	pendingRetriesQuery := `
	SELECT COUNT(DISTINCT path) 
	FROM (
		SELECT path FROM src_nodes WHERE traversal_status = 'pending'
		UNION
		SELECT path FROM dst_nodes WHERE traversal_status = 'pending'
	) AS pending_retries_paths
	`
	err = duckdbConn.QueryRowContext(ctx, pendingRetriesQuery).Scan(&stats.PendingRetriesCount)
	if err != nil {
		return nil, fmt.Errorf("failed to query pending retries count: %w", err)
	}

	// Query for folders and files count (merged, unique paths)
	typeQuery := `
	WITH merged_nodes AS (
		SELECT DISTINCT 
			COALESCE(s.path, d.path) AS path,
			COALESCE(s.type, d.type) AS type
		FROM src_nodes s
		FULL OUTER JOIN dst_nodes d ON s.path = d.path
	)
	SELECT 
		COUNT(*) FILTER (WHERE type = 'folder') AS folders_count,
		COUNT(*) FILTER (WHERE type = 'file') AS files_count
	FROM merged_nodes
	WHERE type IS NOT NULL
	`
	err = duckdbConn.QueryRowContext(ctx, typeQuery).Scan(&stats.FoldersCount, &stats.FilesCount)
	if err != nil {
		return nil, fmt.Errorf("failed to query type counts: %w", err)
	}

	// Calculate ratios (rounded to 2 decimal places)
	totalItems := stats.FoldersCount + stats.FilesCount
	if totalItems > 0 {
		stats.FoldersRatio = math.Round(float64(stats.FoldersCount)/float64(totalItems)*10000) / 100
		stats.FilesRatio = math.Round(float64(stats.FilesCount)/float64(totalItems)*10000) / 100
	}

	// Query for total file size grouped by src/dst (only files, not folders)
	sizeQuery := `
	SELECT 
		COALESCE(SUM(CASE WHEN s.type = 'file' THEN s.size ELSE 0 END), 0) AS src_size,
		COALESCE(SUM(CASE WHEN d.type = 'file' THEN d.size ELSE 0 END), 0) AS dst_size
	FROM src_nodes s
	FULL OUTER JOIN dst_nodes d ON s.path = d.path
	WHERE (s.type = 'file' OR d.type = 'file')
	`
	err = duckdbConn.QueryRowContext(ctx, sizeQuery).Scan(&stats.TotalFileSize.Src, &stats.TotalFileSize.Dst)
	if err != nil {
		return nil, fmt.Errorf("failed to query file sizes: %w", err)
	}

	return stats, nil
}
