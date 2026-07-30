package services

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"

	"codeberg.org/Sylos/Sylos-API/internal/corebridge"
	"codeberg.org/Sylos/Sylos-API/pkg/config"
	fslib "codeberg.org/Sylos/Sylos-FS/pkg/fs"
	fstypes "codeberg.org/Sylos/Sylos-FS/pkg/types"
)

// ServiceType and ServiceDefinition are Sylos-FS runtime types (single source of truth).
type (
	ServiceType       = fstypes.ServiceType
	ServiceDefinition = fstypes.ServiceDefinition
)

const (
	ServiceTypeLocal   = fstypes.ServiceTypeLocal
	ServiceTypeSpectra = fstypes.ServiceTypeSpectra
	ServiceTypeCloud   = fstypes.ServiceTypeCloud
)

// CloudProviderID returns the provider id for a cloud service definition.
func CloudProviderID(def ServiceDefinition) string {
	if def.Cloud != nil {
		return def.Cloud.ProviderID
	}
	return ""
}

// Source represents a source service
type Source struct {
	ID          string            `json:"id"`
	DisplayName string            `json:"displayName"`
	Type        ServiceType       `json:"type"`
	Metadata    map[string]string `json:"metadata,omitempty"`
}

// ListChildrenRequest represents a request to list children
type ListChildrenRequest struct {
	ServiceID    string
	Identifier   string
	Role         string // "source" or "destination" - used to map "spectra" to the correct world
	ConnectionID string // Cloud/Spectra session connection ID
	RootType     string // Cloud browse root type when listing a virtual root
	DriveID      string // Cloud namespace metadata (Dropbox team_folder, shared_folder)
	Offset       int    // Pagination offset (default: 0)
	Limit        int    // Pagination limit (default: 100, max: 1000)
	FoldersOnly  bool   // If true, only return folders and apply limit to folders only
}

// PaginationInfo provides pagination metadata
type PaginationInfo struct {
	Offset       int  `json:"offset"`
	Limit        int  `json:"limit"`
	Total        int  `json:"total"`
	TotalFolders int  `json:"totalFolders"`
	TotalFiles   int  `json:"totalFiles"`
	HasMore      bool `json:"hasMore"`
}

// ServiceManager handles service-related operations.
// FS is the Sylos-FS runtime; methods on ServiceManager add API-layer validation and type mapping.
type ServiceManager struct {
	FS       *fslib.ServiceManager
	services map[string]ServiceDefinition
}

var ErrServiceNotFound = fmt.Errorf("service not found")

func NewServiceManager() *ServiceManager {
	return &ServiceManager{
		FS:       fslib.NewServiceManager(),
		services: make(map[string]ServiceDefinition),
	}
}

func (m *ServiceManager) LoadServices(cfg config.Config) error {
	localServices := make([]fstypes.LocalServiceConfig, 0, len(cfg.Services.Local))
	for _, svc := range cfg.Services.Local {
		if svc.ID == "" {
			return fmt.Errorf("local service missing id")
		}
		name := svc.Name
		if name == "" {
			name = svc.ID
		}
		rootPath := svc.RootPath
		if rootPath != "" {
			abs, err := filepath.Abs(rootPath)
			if err != nil {
				return fmt.Errorf("local service %s: %w", svc.ID, err)
			}
			rootPath = filepath.Clean(abs)
		}
		localCfg := fstypes.LocalServiceConfig{
			ID:       svc.ID,
			Name:     name,
			RootPath: rootPath,
		}
		localServices = append(localServices, localCfg)
		m.services[localCfg.ID] = ServiceDefinition{
			ID:    localCfg.ID,
			Name:  localCfg.Name,
			Type:  ServiceTypeLocal,
			Local: cloneLocalConfig(localCfg),
		}
	}

	spectraServices := make([]fstypes.SpectraServiceConfig, 0, len(cfg.Services.Spectra))
	for _, svc := range cfg.Services.Spectra {
		if svc.ID == "" {
			return fmt.Errorf("spectra service missing id")
		}
		name := svc.Name
		if name == "" {
			name = svc.ID
		}
		world := svc.World
		if world == "" {
			world = "primary"
		}
		rootID := svc.RootID
		if rootID == "" {
			rootID = "root"
		}
		if svc.ConfigPath == "" {
			return fmt.Errorf("spectra service %s missing config_path", svc.ID)
		}
		configPath, err := filepath.Abs(svc.ConfigPath)
		if err != nil {
			return fmt.Errorf("spectra service %s: %w", svc.ID, err)
		}
		configPath = filepath.Clean(configPath)

		spectraCfg := fstypes.SpectraServiceConfig{
			ID:         svc.ID,
			Name:       name,
			World:      world,
			RootID:     rootID,
			ConfigPath: configPath,
		}
		spectraServices = append(spectraServices, spectraCfg)
		m.services[spectraCfg.ID] = ServiceDefinition{
			ID:      spectraCfg.ID,
			Name:    spectraCfg.Name,
			Type:    ServiceTypeSpectra,
			Spectra: cloneSpectraConfig(spectraCfg),
		}
	}

	cloudServices := make([]fstypes.CloudServiceConfig, 0)
	seenCloud := make(map[string]struct{})
	for _, svc := range cfg.Services.Cloud {
		if svc.ID == "" || svc.ProviderID == "" {
			return fmt.Errorf("cloud service requires id and provider_id")
		}
		name := svc.Name
		if name == "" {
			name = svc.ID
		}
		cloudCfg := fstypes.CloudServiceConfig{
			ID:         svc.ID,
			Name:       name,
			ProviderID: svc.ProviderID,
		}
		cloudServices = append(cloudServices, cloudCfg)
		m.services[cloudCfg.ID] = ServiceDefinition{
			ID:    cloudCfg.ID,
			Name:  cloudCfg.Name,
			Type:  ServiceTypeCloud,
			Cloud: cloneCloudConfig(cloudCfg),
		}
		seenCloud[cloudCfg.ProviderID] = struct{}{}
	}
	for providerID, pcfg := range cfg.Providers {
		if !pcfg.Enabled {
			continue
		}
		if _, ok := seenCloud[providerID]; ok {
			continue
		}
		svcID := pcfg.ServiceID
		if svcID == "" {
			svcID = strings.ReplaceAll(providerID, "_", "-")
		}
		cloudCfg := fstypes.CloudServiceConfig{
			ID:         svcID,
			Name:       pcfg.DisplayName,
			ProviderID: providerID,
		}
		if cloudCfg.Name == "" {
			cloudCfg.Name = providerID
		}
		cloudServices = append(cloudServices, cloudCfg)
		m.services[cloudCfg.ID] = ServiceDefinition{
			ID:    cloudCfg.ID,
			Name:  cloudCfg.Name,
			Type:  ServiceTypeCloud,
			Cloud: cloneCloudConfig(cloudCfg),
		}
	}

	return m.FS.LoadServices(localServices, spectraServices, cloudServices)
}

func cloneLocalConfig(c fstypes.LocalServiceConfig) *fstypes.LocalServiceConfig {
	out := c
	return &out
}

func cloneSpectraConfig(c fstypes.SpectraServiceConfig) *fstypes.SpectraServiceConfig {
	out := c
	return &out
}

func cloneCloudConfig(c fstypes.CloudServiceConfig) *fstypes.CloudServiceConfig {
	out := c
	return &out
}

func (m *ServiceManager) ListSources(ctx context.Context) ([]Source, error) {
	sources, err := m.FS.ListSources(ctx)
	if err != nil {
		return nil, err
	}

	result := make([]Source, len(sources))
	for i, s := range sources {
		result[i] = Source{
			ID:          s.ID,
			DisplayName: s.DisplayName,
			Type:        s.Type,
			Metadata:    s.Metadata,
		}
	}
	return result, nil
}

func (m *ServiceManager) ListCloudChildren(ctx context.Context, connectionID, identifier, rootType, driveID string, offset, limit int, foldersOnly bool) (fstypes.ListResult, PaginationInfo, error) {
	result, pagination, err := m.FS.ListCloudChildren(ctx, connectionID, identifier, rootType, driveID, offset, limit, foldersOnly)
	if err != nil {
		return fstypes.ListResult{}, PaginationInfo{}, err
	}
	return result, PaginationInfo{
		Offset:       pagination.Offset,
		Limit:        pagination.Limit,
		Total:        pagination.Total,
		TotalFolders: pagination.TotalFolders,
		TotalFiles:   pagination.TotalFiles,
		HasMore:      pagination.HasMore,
	}, nil
}

func (m *ServiceManager) GetServiceDefinitionByProvider(providerID string) (ServiceDefinition, error) {
	for _, def := range m.services {
		if def.Type == ServiceTypeCloud && CloudProviderID(def) == providerID {
			return def, nil
		}
	}
	return ServiceDefinition{}, ErrServiceNotFound
}

func (m *ServiceManager) ListChildren(ctx context.Context, req ListChildrenRequest) (fstypes.ListResult, PaginationInfo, error) {
	fsReq := fstypes.ListChildrenRequest{
		ServiceID:   req.ServiceID,
		Identifier:  req.Identifier,
		Role:        req.Role,
		SessionID:   req.ConnectionID,
		RootType:    req.RootType,
		DriveID:     req.DriveID,
		Offset:      req.Offset,
		Limit:       req.Limit,
		FoldersOnly: req.FoldersOnly,
	}

	result, pagination, err := m.FS.ListChildren(ctx, fsReq)
	if err != nil {
		return fstypes.ListResult{}, PaginationInfo{}, err
	}

	return result, PaginationInfo{
		Offset:       pagination.Offset,
		Limit:        pagination.Limit,
		Total:        pagination.Total,
		TotalFolders: pagination.TotalFolders,
		TotalFiles:   pagination.TotalFiles,
		HasMore:      pagination.HasMore,
	}, nil
}

func (m *ServiceManager) MountDrive(ctx context.Context, serviceID string, req corebridge.MountDriveRequest) (fstypes.DriveInfo, error) {
	if strings.TrimSpace(req.Device) == "" {
		return fstypes.DriveInfo{}, fmt.Errorf("device is required")
	}
	return m.FS.MountDrive(ctx, serviceID, req.Device)
}

func (m *ServiceManager) GetStorageInfo(ctx context.Context, req corebridge.GetStorageInfoRequest) (fstypes.StorageInfo, error) {
	return m.FS.GetStorageInfo(ctx, fslib.GetStorageInfoRequest{
		ServiceID:    req.ServiceID,
		Path:         req.Path,
		ConnectionID: req.ConnectionID,
		RootType:     req.RootType,
		DriveID:      req.DriveID,
		Role:         req.Role,
	})
}

func (m *ServiceManager) CreateBrowseFolder(ctx context.Context, serviceID string, req corebridge.CreateBrowseFolderRequest) (fstypes.Folder, error) {
	mutation := fstypes.BrowseMutationRequest{
		ServiceID:    serviceID,
		ConnectionID: req.ConnectionID,
		Role:         req.Role,
		RootType:     req.RootType,
		DriveID:      req.DriveID,
		ContextID:    req.ParentID,
	}
	return m.FS.CreateFolder(ctx, mutation, req.ParentID, req.Name)
}

func (m *ServiceManager) DeleteBrowseNodes(ctx context.Context, serviceID string, req corebridge.DeleteBrowseNodesRequest) (corebridge.DeleteBrowseNodesResponse, error) {
	if strings.TrimSpace(req.ContextID) == "" {
		return corebridge.DeleteBrowseNodesResponse{}, fmt.Errorf("contextId is required")
	}
	nodes := make([]fstypes.NodeRef, len(req.Nodes))
	for i, n := range req.Nodes {
		nodes[i] = fstypes.NodeRef{ID: n.ID, Type: n.Type}
	}
	mutation := fstypes.BrowseMutationRequest{
		ServiceID:    serviceID,
		ConnectionID: req.ConnectionID,
		Role:         req.Role,
		RootType:     req.RootType,
		DriveID:      req.DriveID,
		ContextID:    req.ContextID,
	}
	result, err := m.FS.DeleteNodes(ctx, mutation, nodes)
	if err != nil {
		return corebridge.DeleteBrowseNodesResponse{}, err
	}
	response := corebridge.DeleteBrowseNodesResponse{
		Deleted: result.Deleted,
		Errors:  make([]corebridge.DeleteBrowseNodeError, len(result.Errors)),
	}
	for i, delErr := range result.Errors {
		response.Errors[i] = corebridge.DeleteBrowseNodeError{
			ID:      delErr.ID,
			Message: delErr.Message,
		}
	}
	return response, nil
}

func (m *ServiceManager) CountChildren(ctx context.Context, req ListChildrenRequest) (int, error) {
	fsReq := fstypes.ListChildrenRequest{
		ServiceID:   req.ServiceID,
		Identifier:  req.Identifier,
		Role:        req.Role,
		SessionID:   req.ConnectionID,
		RootType:    req.RootType,
		DriveID:     req.DriveID,
		Limit:       1,
		Offset:      0,
		FoldersOnly: false,
	}
	return m.FS.CountChildren(ctx, fsReq)
}

// ValidateMigrationRoot delegates provider-owned forbidden-root policy to
// Sylos-FS. Non-cloud services (including the virtual "spectra" catalog id)
// have no cloud virtual-root policy.
func (m *ServiceManager) ValidateMigrationRoot(serviceID string, folder fstypes.Folder) error {
	// Catalog lists a single virtual "spectra" entry; concrete worlds are
	// spectra-primary / spectra-s1 and are resolved later by role.
	if serviceID == "" || serviceID == "spectra" {
		return nil
	}
	def, err := m.GetServiceDefinition(serviceID)
	if err != nil {
		return err
	}
	if def.Type != ServiceTypeCloud {
		return nil
	}
	return m.FS.ValidateMigrationRoot(def.ID, folder)
}

func (m *ServiceManager) GetServiceDefinition(id string) (ServiceDefinition, error) {
	def, ok := m.services[id]
	if !ok {
		return ServiceDefinition{}, ErrServiceNotFound
	}
	return def, nil
}

func (m *ServiceManager) GetServiceDefinitionByWorld(world string) (ServiceDefinition, error) {
	for _, svc := range m.services {
		if svc.Type == ServiceTypeSpectra && svc.Spectra != nil && svc.Spectra.World == world {
			return svc, nil
		}
	}
	return ServiceDefinition{}, ErrServiceNotFound
}
