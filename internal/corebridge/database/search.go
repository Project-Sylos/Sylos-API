package database

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/rs/zerolog"
)

// SearchCondition represents a single search condition
type SearchCondition struct {
	Field    string
	Operator string
	Value    any
}

// GetAllNodesByStatusDuckDB gets all node paths with a specific status from DuckDB
func GetAllNodesByStatusDuckDB(ctx context.Context, logger zerolog.Logger, duckdbConn *sql.DB, status string) ([]string, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	query := `
	SELECT DISTINCT path FROM (
		SELECT path FROM src_nodes WHERE traversal_status = ?
		UNION
		SELECT path FROM dst_nodes WHERE traversal_status = ?
	) all_statuses`

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

// SearchPathReviewItemsDuckDB searches for path review items matching the given conditions
func SearchPathReviewItemsDuckDB(ctx context.Context, logger zerolog.Logger, duckdbConn *sql.DB, conditions []SearchCondition, offset, limit int, sortField, sortDir string, statusSearchType string, reviewPhase string) (map[string]PathNodes, PaginationInfo, error) {
	if ctx.Err() != nil {
		return map[string]PathNodes{}, PaginationInfo{}, ctx.Err()
	}

	normalizedStatusType := normalizeStatusSearchType(statusSearchType)

	if reviewPhase == "copy" && normalizedStatusType == "both" {
		normalizedStatusType = "copy"
	}

	pathFilterClause, pathFilterArgs := buildPathFilterClause(conditions, normalizedStatusType, reviewPhase, "all_paths")

	direction := "ASC"
	if sortDir == "desc" || sortDir == "DESC" {
		direction = "DESC"
	}
	pathOrderBy := "path " + direction

	var mainQuery, totalQuery string
	if pathFilterClause == "" {
		mainQuery = `
	WITH paged_paths AS (
		SELECT path FROM (
			SELECT path FROM src_nodes WHERE path != '/'
			UNION
			SELECT path FROM dst_nodes WHERE path != '/'
		) u
		ORDER BY ` + pathOrderBy + `
		LIMIT ? OFFSET ?
	)
	SELECT * FROM (` + buildEnrichmentFromPaths("paged_paths") + `) enriched`
		totalQuery = `
	SELECT COUNT(*) FROM (
		SELECT path FROM src_nodes WHERE path != '/'
		UNION
		SELECT path FROM dst_nodes WHERE path != '/'
	) u`
	} else {
		mainQuery = `
	WITH all_paths AS (
		SELECT path FROM src_nodes
		UNION
		SELECT path FROM dst_nodes
	),
	candidate_paths AS (
		SELECT DISTINCT path
		FROM all_paths
		WHERE path != '/'` + pathFilterClause + `
	),
	paged_paths AS (
		SELECT path
		FROM candidate_paths
		ORDER BY ` + pathOrderBy + `
		LIMIT ? OFFSET ?
	)
	SELECT * FROM (` + buildEnrichmentFromPaths("paged_paths") + `) enriched`

		totalQuery = `
	WITH all_paths AS (
		SELECT path FROM src_nodes
		UNION
		SELECT path FROM dst_nodes
	),
	candidate_paths AS (
		SELECT DISTINCT path
		FROM all_paths
		WHERE path != '/'` + pathFilterClause + `
	)
	SELECT COUNT(*) FROM candidate_paths`
	}

	query := mainQuery
	if sortField != "" && sortField != "path" {
		finalOrderBy := buildOrderByClause(sortField, sortDir)
		query += " ORDER BY " + finalOrderBy
	} else {
		query += " ORDER BY path"
	}

	args := append(pathFilterArgs, limit, offset)

	rows, err := duckdbConn.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, PaginationInfo{}, fmt.Errorf("failed to query DuckDB for search: %w", err)
	}
	defer rows.Close()

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

		pathKey := path.String

		pn := PathNodes{}

		if srcPath.Valid {
			pn.Src = &PathNodeItem{
				Queue:           "SRC",
				Id:              getStringValue(srcID),
				ParentId:        getStringValue(srcParentID),
				ParentPath:      "",
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

		if dstPath.Valid {
			pn.Dst = &PathNodeItem{
				Queue:           "DST",
				Id:              getStringValue(dstID),
				ParentId:        getStringValue(dstParentID),
				ParentPath:      "",
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

	err = duckdbConn.QueryRowContext(ctx, totalQuery, pathFilterArgs...).Scan(&total)
	if err != nil {
		return nil, PaginationInfo{}, fmt.Errorf("failed to get total count from DuckDB: %w", err)
	}

	hasMore := offset+limit < total

	pagination := PaginationInfo{
		Offset:       offset,
		Limit:        limit,
		Total:        total,
		TotalFolders: 0,
		TotalFiles:   0,
		HasMore:      hasMore,
	}

	return items, pagination, nil
}

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
		return "both"
	}
}

func buildPathFilterClause(conditions []SearchCondition, statusSearchType string, reviewPhase string, pathTableName string) (string, []any) {
	if len(conditions) == 0 {
		return "", nil
	}
	if pathTableName == "" {
		pathTableName = "candidate_paths"
	}

	var clauses []string
	var args []any

	for _, cond := range conditions {
		clause, clauseArgs := buildPathFilterCondition(cond, statusSearchType, reviewPhase, pathTableName)
		if clause != "" {
			clauses = append(clauses, clause)
			args = append(args, clauseArgs...)
		}
	}

	if len(clauses) == 0 {
		return "", nil
	}

	return " AND (" + strings.Join(clauses, " AND ") + ")", args
}

func buildPathFilterCondition(cond SearchCondition, statusSearchType string, reviewPhase string, pathTableName string) (string, []any) {
	valueStr := fmt.Sprintf("%v", cond.Value)
	if pathTableName == "" {
		pathTableName = "candidate_paths"
	}
	pathCol := pathTableName + ".path"

	if reviewPhase == "copy" && cond.Field == "status" {
		cond.Field = "copyStatus"
	}

	if cond.Field == "traversalStatus" && statusSearchType == "copy" {
		return "", nil
	}
	if cond.Field == "copyStatus" && statusSearchType == "traversal" {
		return "", nil
	}

	switch cond.Field {
	case "path":
		return `(
			EXISTS (SELECT 1 FROM src_nodes WHERE path = ` + pathCol + ` AND (lower(path) LIKE '%' || lower(?) || '%' OR lower(regexp_replace(path, '^.*/', '')) LIKE '%' || lower(?) || '%'))
			OR EXISTS (SELECT 1 FROM dst_nodes WHERE path = ` + pathCol + ` AND (lower(path) LIKE '%' || lower(?) || '%' OR lower(regexp_replace(path, '^.*/', '')) LIKE '%' || lower(?) || '%'))
		)`, []any{valueStr, valueStr, valueStr, valueStr}

	case "name":
		return `(
			EXISTS (SELECT 1 FROM src_nodes WHERE path = ` + pathCol + ` AND lower(regexp_replace(path, '^.*/', '')) LIKE '%' || lower(?) || '%')
			OR EXISTS (SELECT 1 FROM dst_nodes WHERE path = ` + pathCol + ` AND lower(regexp_replace(path, '^.*/', '')) LIKE '%' || lower(?) || '%')
		)`, []any{valueStr, valueStr}

	case "type":
		return `(
			EXISTS (SELECT 1 FROM src_nodes WHERE path = ` + pathCol + ` AND UPPER(type) = UPPER(?))
			OR EXISTS (SELECT 1 FROM dst_nodes WHERE path = ` + pathCol + ` AND UPPER(type) = UPPER(?))
		)`, []any{valueStr, valueStr}

	case "traversalStatus":
		return buildTraversalStatusFilter(valueStr, pathTableName)

	case "copyStatus":
		return buildCopyStatusFilter(valueStr, reviewPhase, pathTableName)

	case "depth":
		op := "="
		if cond.Operator != "" {
			op = cond.Operator
		}
		switch op {
		case "equals", "=":
			return `(EXISTS (SELECT 1 FROM src_nodes WHERE path = ` + pathCol + ` AND depth = ?) OR EXISTS (SELECT 1 FROM dst_nodes WHERE path = ` + pathCol + ` AND depth = ?))`, []any{cond.Value, cond.Value}
		case "gt", ">":
			return `(EXISTS (SELECT 1 FROM src_nodes WHERE path = ` + pathCol + ` AND depth > ?) OR EXISTS (SELECT 1 FROM dst_nodes WHERE path = ` + pathCol + ` AND depth > ?))`, []any{cond.Value, cond.Value}
		case "gte", ">=":
			return `(EXISTS (SELECT 1 FROM src_nodes WHERE path = ` + pathCol + ` AND depth >= ?) OR EXISTS (SELECT 1 FROM dst_nodes WHERE path = ` + pathCol + ` AND depth >= ?))`, []any{cond.Value, cond.Value}
		case "lt", "<":
			return `(EXISTS (SELECT 1 FROM src_nodes WHERE path = ` + pathCol + ` AND depth < ?) OR EXISTS (SELECT 1 FROM dst_nodes WHERE path = ` + pathCol + ` AND depth < ?))`, []any{cond.Value, cond.Value}
		case "lte", "<=":
			return `(EXISTS (SELECT 1 FROM src_nodes WHERE path = ` + pathCol + ` AND depth <= ?) OR EXISTS (SELECT 1 FROM dst_nodes WHERE path = ` + pathCol + ` AND depth <= ?))`, []any{cond.Value, cond.Value}
		}
		return "", nil

	case "size":
		op := "="
		if cond.Operator != "" {
			op = cond.Operator
		}
		switch op {
		case "equals", "=":
			return `(EXISTS (SELECT 1 FROM src_nodes WHERE path = ` + pathCol + ` AND size = ?) OR EXISTS (SELECT 1 FROM dst_nodes WHERE path = ` + pathCol + ` AND size = ?))`, []any{cond.Value, cond.Value}
		case "gt", ">":
			return `(EXISTS (SELECT 1 FROM src_nodes WHERE path = ` + pathCol + ` AND size > ?) OR EXISTS (SELECT 1 FROM dst_nodes WHERE path = ` + pathCol + ` AND size > ?))`, []any{cond.Value, cond.Value}
		case "gte", ">=":
			return `(EXISTS (SELECT 1 FROM src_nodes WHERE path = ` + pathCol + ` AND size >= ?) OR EXISTS (SELECT 1 FROM dst_nodes WHERE path = ` + pathCol + ` AND size >= ?))`, []any{cond.Value, cond.Value}
		case "lt", "<":
			return `(EXISTS (SELECT 1 FROM src_nodes WHERE path = ` + pathCol + ` AND size < ?) OR EXISTS (SELECT 1 FROM dst_nodes WHERE path = ` + pathCol + ` AND size < ?))`, []any{cond.Value, cond.Value}
		case "lte", "<=":
			return `(EXISTS (SELECT 1 FROM src_nodes WHERE path = ` + pathCol + ` AND size <= ?) OR EXISTS (SELECT 1 FROM dst_nodes WHERE path = ` + pathCol + ` AND size <= ?))`, []any{cond.Value, cond.Value}
		}
		return "", nil
	}

	return "", nil
}

func buildTraversalStatusFilter(valueStr string, pathTableName string) (string, []any) {
	if pathTableName == "" {
		pathTableName = "candidate_paths"
	}
	pathCol := pathTableName + ".path"
	valueUpper := strings.ToUpper(valueStr)
	if valueUpper == "EXCLUDED" {
		return `(
			EXISTS (SELECT 1 FROM src_nodes WHERE path = ` + pathCol + ` AND excluded = true)
			OR EXISTS (SELECT 1 FROM dst_nodes WHERE path = ` + pathCol + ` AND excluded = true)
		)`, []any{}
	}
	if valueUpper == "NOT_ON_SRC" {
		return `EXISTS (SELECT 1 FROM dst_nodes WHERE path = ` + pathCol + ` AND UPPER(traversal_status) = 'NOT_ON_SRC')`, []any{}
	}
	return `(
		EXISTS (SELECT 1 FROM src_nodes WHERE path = ` + pathCol + ` AND UPPER(traversal_status) = UPPER(?))
		OR EXISTS (SELECT 1 FROM dst_nodes WHERE path = ` + pathCol + ` AND UPPER(traversal_status) = UPPER(?))
	)`, []any{valueStr, valueStr}
}

func buildCopyStatusFilter(valueStr string, reviewPhase string, pathTableName string) (string, []any) {
	if pathTableName == "" {
		pathTableName = "candidate_paths"
	}
	pathCol := pathTableName + ".path"
	valueUpper := strings.ToUpper(valueStr)
	if valueUpper == "EXCLUDED" {
		return `EXISTS (SELECT 1 FROM src_nodes WHERE path = ` + pathCol + ` AND excluded = true)`, []any{}
	}
	if reviewPhase == "copy" && valueUpper == "SUCCESSFUL" {
		return `(
			EXISTS (SELECT 1 FROM src_nodes WHERE path = ` + pathCol + ` AND UPPER(copy_status) = 'SUCCESSFUL')
			OR (EXISTS (SELECT 1 FROM dst_nodes WHERE path = ` + pathCol + `) AND NOT EXISTS (SELECT 1 FROM src_nodes WHERE path = ` + pathCol + `))
		)`, []any{}
	}
	if valueUpper == "NOT_ON_SRC" {
		return `EXISTS (SELECT 1 FROM dst_nodes WHERE path = ` + pathCol + ` AND UPPER(COALESCE(copy_status, '')) = 'NOT_ON_SRC')`, []any{}
	}
	return `EXISTS (SELECT 1 FROM src_nodes WHERE path = ` + pathCol + ` AND UPPER(copy_status) = UPPER(?))`, []any{valueStr}
}
