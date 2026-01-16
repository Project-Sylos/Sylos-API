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
	TraversalStatusCounts map[string]int `json:"traversalStatusCounts"` // Counts by traversal_status (pending, failed, successful, exclusion_explicit, exclusion_inherited, not_on_src, not_on_dst)
	CopyStatusCounts      map[string]int `json:"copyStatusCounts"`      // Counts by copy_status (pending, failed, successful, exclusion_explicit, exclusion_inherited)
	FoldersCount          int            `json:"foldersCount"`
	FilesCount            int            `json:"filesCount"`
	FoldersRatio          float64        `json:"foldersRatio"` // Rounded to 2 decimal places
	FilesRatio            float64        `json:"filesRatio"`   // Rounded to 2 decimal places
	TotalFileSize         FileSizeStats  `json:"totalFileSize"`
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

	// Initialize all expected status values to 0 so they always appear in the response
	stats := &PathReviewStats{
		TraversalStatusCounts: map[string]int{
			"pending":             0,
			"failed":              0,
			"successful":          0,
			"exclusion_explicit":  0,
			"exclusion_inherited": 0,
			"not_on_src":          0,
			"not_on_dst":          0,
		},
		CopyStatusCounts: map[string]int{
			"pending":             0,
			"failed":              0,
			"successful":          0,
			"exclusion_explicit":  0,
			"exclusion_inherited": 0,
		},
	}

	// Query for traversal status counts (merged counts from src_nodes and dst_nodes)
	// Filter out NULL and empty strings
	traversalStatusQuery := `
	SELECT 
		COALESCE(NULLIF(traversal_status, ''), 'unknown') AS status,
		COUNT(DISTINCT path) AS count
	FROM (
		SELECT path, traversal_status FROM src_nodes 
		WHERE traversal_status IS NOT NULL AND traversal_status != ''
		UNION
		SELECT path, traversal_status FROM dst_nodes 
		WHERE traversal_status IS NOT NULL AND traversal_status != ''
	) AS all_traversal_statuses
	GROUP BY COALESCE(NULLIF(traversal_status, ''), 'unknown')
	`
	rows, err := duckdbConn.QueryContext(ctx, traversalStatusQuery)
	if err != nil {
		return nil, fmt.Errorf("failed to query traversal status counts: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var status string
		var count int
		if err := rows.Scan(&status, &count); err != nil {
			return nil, fmt.Errorf("failed to scan traversal status row: %w", err)
		}
		// Only update if status is a known value (ignore 'unknown' which shouldn't happen)
		if status != "unknown" {
			stats.TraversalStatusCounts[status] = count
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating traversal status rows: %w", err)
	}

	// Query for copy status counts (only from src_nodes - dst_nodes don't have copy_status field)
	// Filter out NULL and empty strings
	copyStatusQuery := `
	SELECT 
		COALESCE(NULLIF(copy_status, ''), 'unknown') AS status,
		COUNT(DISTINCT path) AS count
	FROM src_nodes
	WHERE copy_status IS NOT NULL AND copy_status != ''
	GROUP BY COALESCE(NULLIF(copy_status, ''), 'unknown')
	`
	rows, err = duckdbConn.QueryContext(ctx, copyStatusQuery)
	if err != nil {
		return nil, fmt.Errorf("failed to query copy status counts: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var status string
		var count int
		if err := rows.Scan(&status, &count); err != nil {
			return nil, fmt.Errorf("failed to scan copy status row: %w", err)
		}
		// Only update if status is a known value (ignore 'unknown' and empty strings)
		if status != "unknown" && status != "" {
			stats.CopyStatusCounts[status] = count
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating copy status rows: %w", err)
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
