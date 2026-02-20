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
	TraversalStatusCounts map[string]int `json:"traversalStatusCounts"` // Counts by traversal_status (pending, successful, failed, not_on_src)
	CopyStatusCounts      map[string]int `json:"copyStatusCounts"`       // Counts by copy_status (pending, in_progress, successful, failed, skipped)
	ExcludedCount         int            `json:"excludedCount"`           // Count where excluded = true (engine schema)
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
func GetPathReviewStatsDuckDB(ctx context.Context, logger zerolog.Logger, duckdbConn *sql.DB) (*PathReviewStats, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	stats := &PathReviewStats{
		TraversalStatusCounts: map[string]int{
			"pending":    0,
			"failed":     0,
			"successful": 0,
			"not_on_src": 0,
			"not_on_dst": 0,
		},
		CopyStatusCounts: map[string]int{
			"pending":     0,
			"failed":      0,
			"successful":  0,
			"in_progress": 0,
			"skipped":     0,
		},
	}

	// UNION both node tables for traversal status counts (engine schema: src/dst_nodes)
	traversalStatusQuery := `
	SELECT COALESCE(NULLIF(traversal_status, ''), 'unknown') AS status, COUNT(DISTINCT path) AS count
	FROM (
		SELECT path, traversal_status FROM src_nodes WHERE traversal_status IS NOT NULL AND traversal_status != ''
		UNION
		SELECT path, traversal_status FROM dst_nodes WHERE traversal_status IS NOT NULL AND traversal_status != ''
	) all_statuses
	GROUP BY COALESCE(NULLIF(traversal_status, ''), 'unknown')
	`
	rows, err := duckdbConn.QueryContext(ctx, traversalStatusQuery)
	if err != nil {
		return nil, fmt.Errorf("failed to query traversal status counts: %w", err)
	}
	for rows.Next() {
		var status string
		var count int
		if err := rows.Scan(&status, &count); err != nil {
			rows.Close()
			return nil, fmt.Errorf("failed to scan traversal status row: %w", err)
		}
		if status != "unknown" {
			stats.TraversalStatusCounts[status] = count
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating traversal status rows: %w", err)
	}

	// Excluded count (engine schema: excluded column)
	if err := duckdbConn.QueryRowContext(ctx, `SELECT COUNT(*) FROM (SELECT path FROM src_nodes WHERE excluded = true UNION SELECT path FROM dst_nodes WHERE excluded = true) t`).Scan(&stats.ExcludedCount); err != nil {
		// excluded column may not exist in older schema
		stats.ExcludedCount = 0
	}

	// Copy status only from src_nodes
	copyStatusQuery := `
	SELECT COALESCE(NULLIF(copy_status, ''), 'unknown') AS status, COUNT(DISTINCT path) AS count
	FROM src_nodes
	WHERE copy_status IS NOT NULL AND copy_status != ''
	GROUP BY COALESCE(NULLIF(copy_status, ''), 'unknown')
	`
	rows, err = duckdbConn.QueryContext(ctx, copyStatusQuery)
	if err != nil {
		return nil, fmt.Errorf("failed to query copy status counts: %w", err)
	}
	for rows.Next() {
		var status string
		var count int
		if err := rows.Scan(&status, &count); err != nil {
			rows.Close()
			return nil, fmt.Errorf("failed to scan copy status row: %w", err)
		}
		if status != "unknown" && status != "" {
			stats.CopyStatusCounts[status] = count
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating copy status rows: %w", err)
	}

	// Type is in src/dst_nodes (engine schema)
	typeQuery := `
	WITH all_types AS (
		SELECT path, type FROM src_nodes WHERE type IS NOT NULL
		UNION
		SELECT path, type FROM dst_nodes WHERE type IS NOT NULL
	),
	path_type AS (
		SELECT path, MAX(type) AS type FROM all_types GROUP BY path
	)
	SELECT
		COUNT(*) FILTER (WHERE type = 'folder') AS folders_count,
		COUNT(*) FILTER (WHERE type = 'file') AS files_count
	FROM path_type
	`
	err = duckdbConn.QueryRowContext(ctx, typeQuery).Scan(&stats.FoldersCount, &stats.FilesCount)
	if err != nil {
		return nil, fmt.Errorf("failed to query type counts: %w", err)
	}

	totalItems := stats.FoldersCount + stats.FilesCount
	if totalItems > 0 {
		stats.FoldersRatio = math.Round(float64(stats.FoldersCount)/float64(totalItems)*10000) / 100
		stats.FilesRatio = math.Round(float64(stats.FilesCount)/float64(totalItems)*10000) / 100
	}

	// Type and size are in src/dst_nodes (engine schema)
	sizeQuery := `
	SELECT
		COALESCE((SELECT SUM(size) FROM src_nodes WHERE type = 'file'), 0) AS src_size,
		COALESCE((SELECT SUM(size) FROM dst_nodes WHERE type = 'file'), 0) AS dst_size
	`
	err = duckdbConn.QueryRowContext(ctx, sizeQuery).Scan(&stats.TotalFileSize.Src, &stats.TotalFileSize.Dst)
	if err != nil {
		return nil, fmt.Errorf("failed to query file sizes: %w", err)
	}

	return stats, nil
}
