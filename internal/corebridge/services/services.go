package services

import (
	"context"
	"fmt"
	"path/filepath"

	"github.com/Project-Sylos/Sylos-API/pkg/config"
	fslib "github.com/Project-Sylos/Sylos-FS/pkg/fs"
	fstypes "github.com/Project-Sylos/Sylos-FS/pkg/types"
)

// ServiceType represents the type of service
type ServiceType string

const (
	ServiceTypeLocal   ServiceType = "local"
	ServiceTypeSpectra ServiceType = "spectra"
)

// Source represents a source service
type Source struct {
	ID          string            `json:"id"`
	DisplayName string            `json:"displayName"`
	Type        ServiceType       `json:"type"`
	Metadata    map[string]string `json:"metadata,omitempty"`
}

// ListChildrenRequest represents a request to list children
type ListChildrenRequest struct {
	ServiceID   string
	Identifier  string
	Role        string // "source" or "destination" - used to map "spectra" to the correct world
	Offset      int    // Pagination offset (default: 0)
	Limit       int    // Pagination limit (default: 100, max: 1000)
	FoldersOnly bool   // If true, only return folders and apply limit to folders only
}

// ServiceDefinition represents a service definition
type ServiceDefinition struct {
	ID      string
	Name    string
	Type    ServiceType
	Local   *config.LocalServiceConfig
	Spectra *config.SpectraServiceConfig
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

// driveInfo represents information about a drive/volume (internal type)
type driveInfo struct {
	Path        string `json:"path"`        // Absolute path to the drive (e.g., "C:\" on Windows, "/" on Unix)
	DisplayName string `json:"displayName"` // Display name (e.g., "C:" or "Local Disk (C:)")
	Type        string `json:"type"`        // Drive type (e.g., "fixed", "removable", "network")
}

// ServiceManager handles service-related operations
type ServiceManager struct {
	fsManager *fslib.ServiceManager
	services  map[string]ServiceDefinition
}

var ErrServiceNotFound = fmt.Errorf("service not found")

func NewServiceManager() *ServiceManager {
	return &ServiceManager{
		fsManager: fslib.NewServiceManager(),
		services:  make(map[string]ServiceDefinition),
	}
}

func (m *ServiceManager) LoadServices(cfg config.Config) error {
	// Convert config to Sylos-FS types
	localServices := make([]fstypes.LocalServiceConfig, 0, len(cfg.Services.Local))
	for _, svc := range cfg.Services.Local {
		if svc.ID == "" {
			return fmt.Errorf("local service missing id")
		}
		normalized := svc
		if normalized.Name == "" {
			normalized.Name = normalized.ID
		}
		if normalized.RootPath != "" {
			abs, err := filepath.Abs(normalized.RootPath)
			if err != nil {
				return fmt.Errorf("local service %s: %w", normalized.ID, err)
			}
			normalized.RootPath = filepath.Clean(abs)
		}
		localServices = append(localServices, fstypes.LocalServiceConfig{
			ID:       normalized.ID,
			Name:     normalized.Name,
			RootPath: normalized.RootPath,
		})
		// Store in our internal map for later use
		m.services[normalized.ID] = ServiceDefinition{
			ID:    normalized.ID,
			Name:  normalized.Name,
			Type:  ServiceTypeLocal,
			Local: &normalized,
		}
	}

	spectraServices := make([]fstypes.SpectraServiceConfig, 0, len(cfg.Services.Spectra))
	for _, svc := range cfg.Services.Spectra {
		if svc.ID == "" {
			return fmt.Errorf("spectra service missing id")
		}
		normalized := svc
		if normalized.Name == "" {
			normalized.Name = normalized.ID
		}
		if normalized.World == "" {
			normalized.World = "primary"
		}
		if normalized.RootID == "" {
			normalized.RootID = "root"
		}
		if normalized.ConfigPath == "" {
			return fmt.Errorf("spectra service %s missing config_path", normalized.ID)
		}
		abs, err := filepath.Abs(normalized.ConfigPath)
		if err != nil {
			return fmt.Errorf("spectra service %s: %w", normalized.ID, err)
		}
		normalized.ConfigPath = filepath.Clean(abs)

		spectraServices = append(spectraServices, fstypes.SpectraServiceConfig{
			ID:         normalized.ID,
			Name:       normalized.Name,
			World:      normalized.World,
			RootID:     normalized.RootID,
			ConfigPath: normalized.ConfigPath,
		})
		// Store in our internal map for later use
		m.services[normalized.ID] = ServiceDefinition{
			ID:      normalized.ID,
			Name:    normalized.Name,
			Type:    ServiceTypeSpectra,
			Spectra: &normalized,
		}
	}

	// Load services into Sylos-FS manager
	return m.fsManager.LoadServices(localServices, spectraServices)
}

func (m *ServiceManager) ListSources(ctx context.Context) ([]Source, error) {
	sources, err := m.fsManager.ListSources(ctx)
	if err != nil {
		return nil, err
	}

	result := make([]Source, len(sources))
	for i, s := range sources {
		result[i] = Source{
			ID:          s.ID,
			DisplayName: s.DisplayName,
			Type:        ServiceType(s.Type),
			Metadata:    s.Metadata,
		}
	}
	return result, nil
}

func (m *ServiceManager) ListChildren(ctx context.Context, req ListChildrenRequest) (fstypes.ListResult, PaginationInfo, error) {
	// Convert to Sylos-FS request
	fsReq := fstypes.ListChildrenRequest{
		ServiceID:   req.ServiceID,
		Identifier:  req.Identifier,
		Offset:      req.Offset,
		Limit:       req.Limit,
		FoldersOnly: req.FoldersOnly,
	}

	// Handle role-based mapping for "spectra" virtual service
	// The Sylos-FS library handles virtual service mapping internally based on role
	// We pass the role through the request context or handle it here
	// For now, if serviceID is "spectra", the library should handle role mapping
	// If the library doesn't support role in the request, we may need to adjust the serviceID

	result, pagination, err := m.fsManager.ListChildren(ctx, fsReq)
	if err != nil {
		return fstypes.ListResult{}, PaginationInfo{}, err
	}

	// Return Sylos-FS types directly (no conversion needed)
	return result, PaginationInfo{
		Offset:       pagination.Offset,
		Limit:        pagination.Limit,
		Total:        pagination.Total,
		TotalFolders: pagination.TotalFolders,
		TotalFiles:   pagination.TotalFiles,
		HasMore:      pagination.HasMore,
	}, nil
}

func (m *ServiceManager) ListDrives(ctx context.Context, serviceID string) ([]driveInfo, error) {
	drives, err := m.fsManager.ListDrives(ctx, serviceID)
	if err != nil {
		return nil, err
	}

	result := make([]driveInfo, len(drives))
	for i, d := range drives {
		result[i] = driveInfo{
			Path:        d.Path,
			DisplayName: d.DisplayName,
			Type:        d.Type,
		}
	}
	return result, nil
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

func (m *ServiceManager) AcquireAdapter(def ServiceDefinition, rootID, connectionID string) (fstypes.FSAdapter, func(), error) {
	return m.AcquireAdapterWithOverride(def, rootID, connectionID, "")
}

// AcquireAdapterWithOverride acquires an adapter, with optional Spectra config override path
func (m *ServiceManager) AcquireAdapterWithOverride(def ServiceDefinition, rootID, connectionID, spectraConfigOverridePath string) (fstypes.FSAdapter, func(), error) {
	// Convert ServiceDefinition to Sylos-FS types
	var fsDef fstypes.ServiceDefinition
	switch def.Type {
	case ServiceTypeLocal:
		if def.Local == nil {
			return nil, nil, fmt.Errorf("local service configuration missing")
		}
		fsDef = fstypes.ServiceDefinition{
			ID:   def.ID,
			Name: def.Name,
			Type: fstypes.ServiceTypeLocal,
			Local: &fstypes.LocalServiceConfig{
				ID:       def.Local.ID,
				Name:     def.Local.Name,
				RootPath: def.Local.RootPath,
			},
		}
	case ServiceTypeSpectra:
		if def.Spectra == nil {
			return nil, nil, fmt.Errorf("spectra service configuration missing")
		}
		configPath := def.Spectra.ConfigPath
		if spectraConfigOverridePath != "" {
			configPath = spectraConfigOverridePath
		}
		fsDef = fstypes.ServiceDefinition{
			ID:   def.ID,
			Name: def.Name,
			Type: fstypes.ServiceTypeSpectra,
			Spectra: &fstypes.SpectraServiceConfig{
				ID:         def.Spectra.ID,
				Name:       def.Spectra.Name,
				World:      def.Spectra.World,
				RootID:     def.Spectra.RootID,
				ConfigPath: configPath,
			},
		}
	default:
		return nil, nil, fmt.Errorf("unsupported service type: %s", def.Type)
	}

	// Acquire adapter from Sylos-FS
	adapter, release, err := m.fsManager.AcquireAdapter(fsDef, rootID, connectionID)
	if err != nil {
		return nil, nil, err
	}

	// Convert Sylos-FS adapter to Migration-Engine adapter
	// Note: This assumes the adapters implement the same interface
	// If they don't, we'll need to create a wrapper adapter
	// For now, we assume compatibility and return the adapter directly
	return adapter, release, nil
}
