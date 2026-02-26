package manager

import (
	"context"
	"fmt"
	"strings"

	"codeberg.org/Sylos/Migration-Engine/pkg/migration"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge"
)

func diffItemToPathNodes(item migration.DiffItem) corebridge.PathNodes {
	pathNodes := corebridge.PathNodes{}
	if !item.MissingOnSource {
		pathNodes.Src = &corebridge.PathNodeItem{
			Queue:           "SRC",
			Id:              item.SrcNodeID,
			Name:            item.Name,
			LocationPath:    item.Path,
			DepthLevel:      item.Depth,
			Type:            item.Type,
			Size:            item.Size,
			TraversalStatus: item.SrcTraversalStatus,
			CopyStatus:      item.CopyStatus,
		}
	}
	if !item.MissingOnDest {
		pathNodes.Dst = &corebridge.PathNodeItem{
			Queue:           "DST",
			Id:              item.DstNodeID,
			Name:            item.Name,
			LocationPath:    item.Path,
			DepthLevel:      item.Depth,
			Type:            item.Type,
			Size:            item.Size,
			TraversalStatus: item.DstTraversalStatus,
			CopyStatus:      item.CopyStatus,
		}
	}
	return pathNodes
}

func (m *Manager) ListChildrenDiffs(_ context.Context, req corebridge.ListChildrenDiffsRequest) (corebridge.ListChildrenDiffsResponse, error) {
	mig, err := m.engineMgr.GetMigration(req.MigrationID)
	if err != nil {
		return corebridge.ListChildrenDiffsResponse{}, err
	}
	if mig == nil {
		return corebridge.ListChildrenDiffsResponse{}, corebridge.ErrMigrationNotFound
	}

	sortBy := ""
	sortDirection := "asc"
	if req.Sort != nil {
		sortBy = req.Sort.Field
		if req.Sort.Direction != "" {
			sortDirection = strings.ToLower(req.Sort.Direction)
		}
	}
	result, err := mig.ListChildrenDiffs(migration.ListChildrenDiffsRequest{
		Path:          req.Path,
		Limit:       req.Limit,
		Offset:      req.Offset,
		SortBy:        sortBy,
		SortDirection: sortDirection,
		FoldersOnly:   req.FoldersOnly,
	})
	if err != nil {
		return corebridge.ListChildrenDiffsResponse{}, fmt.Errorf("failed to list children diffs: %w", err)
	}

	items := make(map[string]corebridge.PathNodes)
	for _, item := range result.Items {
		items[item.Path] = diffItemToPathNodes(item)
	}

	hasMore := result.Offset+result.Limit < result.Total
	return corebridge.ListChildrenDiffsResponse{
		Items: items,
		Pagination: corebridge.PaginationInfo{
			Offset:  result.Offset,
			Limit:   result.Limit,
			Total:   result.Total,
			HasMore: hasMore,
		},
	}, nil
}

func (m *Manager) GetChildrenDiffsStats(_ context.Context, migrationID, path string, foldersOnly bool) (corebridge.DiffsStatsResponse, error) {
	mig, err := m.engineMgr.GetMigration(migrationID)
	if err != nil {
		return corebridge.DiffsStatsResponse{}, err
	}
	if mig == nil {
		return corebridge.DiffsStatsResponse{}, corebridge.ErrMigrationNotFound
	}
	stats, err := mig.GetChildrenDiffsStats(path, foldersOnly)
	if err != nil {
		return corebridge.DiffsStatsResponse{}, fmt.Errorf("failed to get diffs stats: %w", err)
	}
	folders := stats.Folders
	files := stats.Files
	if foldersOnly {
		files = 0
	}
	return corebridge.DiffsStatsResponse{
		Total:        stats.Total,
		TotalFolders: folders,
		TotalFiles:   files,
	}, nil
}

func (m *Manager) SearchPathReviewItems(_ context.Context, migrationID string, req corebridge.SearchRequest, offset, limit int) (corebridge.ListChildrenDiffsResponse, error) {
	mig, err := m.engineMgr.GetMigration(migrationID)
	if err != nil {
		return corebridge.ListChildrenDiffsResponse{}, err
	}
	if mig == nil {
		return corebridge.ListChildrenDiffsResponse{}, corebridge.ErrMigrationNotFound
	}

	query := ""
	for _, cond := range req.Conditions {
		if cond.Field == "path" || cond.Field == "name" {
			query = fmt.Sprintf("%v", cond.Value)
			break
		}
	}
	sortBy := ""
	sortDirection := "asc"
	if req.Sort != nil {
		sortBy = req.Sort.Field
		if req.Sort.Direction != "" {
			sortDirection = strings.ToLower(req.Sort.Direction)
		}
	}
	result, err := mig.SearchPathReviewItems(migration.SearchRequest{
		Query:         query,
		Path:          "",
		Limit:       limit,
		Offset:      offset,
		SortBy:        sortBy,
		SortDirection: sortDirection,
	})
	if err != nil {
		return corebridge.ListChildrenDiffsResponse{}, fmt.Errorf("failed to search path review items: %w", err)
	}

	items := make(map[string]corebridge.PathNodes)
	for _, item := range result.Items {
		items[item.Path] = diffItemToPathNodes(item)
	}
	return corebridge.ListChildrenDiffsResponse{
		Items: items,
		Pagination: corebridge.PaginationInfo{
			Offset:  result.Offset,
			Limit:   result.Limit,
			Total:   result.Total,
			HasMore: result.Offset+result.Limit < result.Total,
		},
	}, nil
}
