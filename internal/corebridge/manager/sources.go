package manager

import (
	"context"

	"codeberg.org/Sylos/Sylos-API/internal/corebridge"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge/services"
)

func (m *Manager) ListSources(ctx context.Context) ([]corebridge.Source, error) {
	sources, err := m.serviceMgr.ListSources(ctx)
	if err != nil {
		return nil, err
	}
	result := make([]corebridge.Source, len(sources))
	for i, s := range sources {
		result[i] = corebridge.Source{
			ID:          s.ID,
			DisplayName: s.DisplayName,
			Type:        s.Type,
			Metadata:    s.Metadata,
		}
	}
	return result, nil
}

func (m *Manager) ListChildren(ctx context.Context, req corebridge.ListChildrenRequest) (corebridge.ListChildrenResponse, error) {
	svcReq := services.ListChildrenRequest{
		ServiceID:   req.ServiceID,
		Identifier:  req.Identifier,
		Role:        req.Role,
		Offset:      req.Offset,
		Limit:       req.Limit,
		FoldersOnly: req.FoldersOnly,
	}
	result, pagination, err := m.serviceMgr.ListChildren(ctx, svcReq)
	if err != nil {
		return corebridge.ListChildrenResponse{}, err
	}
	return corebridge.ListChildrenResponse{
		Folders: result.Folders,
		Files:   result.Files,
		Pagination: corebridge.PaginationInfo{
			Offset:       pagination.Offset,
			Limit:        pagination.Limit,
			Total:        pagination.Total,
			TotalFolders: pagination.TotalFolders,
			TotalFiles:   pagination.TotalFiles,
			HasMore:      pagination.HasMore,
		},
	}, nil
}

func (m *Manager) ListDrives(ctx context.Context, serviceID string) ([]corebridge.DriveInfo, error) {
	return m.serviceMgr.ListDrives(ctx, serviceID)
}

func (m *Manager) MountDrive(ctx context.Context, serviceID string, req corebridge.MountDriveRequest) (corebridge.DriveInfo, error) {
	return m.serviceMgr.MountDrive(ctx, serviceID, req)
}
