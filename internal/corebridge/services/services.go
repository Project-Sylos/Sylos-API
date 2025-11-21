package services

import (
	"context"
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"github.com/Project-Sylos/Migration-Engine/pkg/fsservices"
	"github.com/Project-Sylos/Spectra/sdk"
	"github.com/Project-Sylos/Sylos-API/pkg/config"
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
	ServiceID  string
	Identifier string
	Role       string // "source" or "destination" - used to map "spectra" to the correct world
}

// ServiceManager handles service-related operations
type ServiceManager struct {
	services    map[string]serviceDefinition
	connections map[string]*serviceConnection
	mu          sync.RWMutex
}

type ServiceDefinition struct {
	ID      string
	Name    string
	Type    ServiceType
	Local   *config.LocalServiceConfig
	Spectra *config.SpectraServiceConfig
}

type serviceDefinition = ServiceDefinition // alias for internal use

type serviceConnection struct {
	typ      ServiceType
	spectra  *sdk.SpectraFS
	refCount int
}

func NewServiceManager() *ServiceManager {
	return &ServiceManager{
		services:    make(map[string]serviceDefinition),
		connections: make(map[string]*serviceConnection),
	}
}

func (m *ServiceManager) LoadServices(cfg config.Config) error {
	m.mu.Lock()
	defer m.mu.Unlock()

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
		def := serviceDefinition{
			ID:    normalized.ID,
			Name:  normalized.Name,
			Type:  ServiceTypeLocal,
			Local: &normalized,
		}
		m.services[def.ID] = def
	}

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

		def := serviceDefinition{
			ID:      normalized.ID,
			Name:    normalized.Name,
			Type:    ServiceTypeSpectra,
			Spectra: &normalized,
		}
		m.services[def.ID] = def
	}

	return nil
}

func (m *ServiceManager) ListSources(ctx context.Context) ([]Source, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	sources := make([]Source, 0)
	hasSpectra := false
	var spectraConfigPath string

	for _, svc := range m.services {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		switch svc.Type {
		case ServiceTypeLocal:
			// Add all local services individually
			metadata := map[string]string{
				"name": svc.Name,
			}
			if svc.Local != nil && svc.Local.RootPath != "" {
				metadata["rootPath"] = svc.Local.RootPath
			}
			sources = append(sources, Source{
				ID:          svc.ID,
				DisplayName: svc.Name,
				Type:        svc.Type,
				Metadata:    metadata,
			})
		case ServiceTypeSpectra:
			// Track that we have Spectra services, but don't add them individually
			if !hasSpectra {
				if svc.Spectra != nil {
					spectraConfigPath = svc.Spectra.ConfigPath
				}
				hasSpectra = true
			}
		}
	}

	// Add a single "spectra" service entry if any Spectra services exist
	if hasSpectra {
		metadata := map[string]string{
			"name": "Spectra",
		}
		if spectraConfigPath != "" {
			metadata["configPath"] = spectraConfigPath
		}
		sources = append(sources, Source{
			ID:          "spectra",
			DisplayName: "Spectra",
			Type:        ServiceTypeSpectra,
			Metadata:    metadata,
		})
	}

	sort.Slice(sources, func(i, j int) bool {
		return sources[i].DisplayName < sources[j].DisplayName
	})

	return sources, nil
}

func (m *ServiceManager) ListChildren(ctx context.Context, req ListChildrenRequest) (fsservices.ListResult, error) {
	serviceID := req.ServiceID

	// Map "spectra" virtual service to the appropriate world based on role
	if serviceID == "spectra" {
		role := strings.ToLower(strings.TrimSpace(req.Role))
		var world string
		switch role {
		case "source":
			world = "primary"
		case "destination":
			world = "s1"
		default:
			// Default to primary if no role specified
			world = "primary"
		}

		def, err := m.findSpectraServiceByWorld(world)
		if err != nil {
			return fsservices.ListResult{}, fmt.Errorf("spectra service with world %s not found: %w", world, err)
		}
		return m.listSpectraChildren(def, req.Identifier)
	}

	def, err := m.serviceDefinition(serviceID)
	if err != nil {
		return fsservices.ListResult{}, err
	}

	switch def.Type {
	case ServiceTypeLocal:
		return m.listLocalChildren(def, req.Identifier)
	case ServiceTypeSpectra:
		return m.listSpectraChildren(def, req.Identifier)
	default:
		return fsservices.ListResult{}, fmt.Errorf("unsupported service type: %s", def.Type)
	}
}

func (m *ServiceManager) listLocalChildren(def serviceDefinition, identifier string) (fsservices.ListResult, error) {
	if def.Local == nil || def.Local.RootPath == "" {
		return fsservices.ListResult{}, fmt.Errorf("local service %s missing root_path", def.ID)
	}

	root := def.Local.RootPath
	adapter, err := fsservices.NewLocalFS(root)
	if err != nil {
		return fsservices.ListResult{}, err
	}

	target := identifier
	if target == "" {
		target = root
	}

	cleanTarget, err := filepath.Abs(target)
	if err != nil {
		return fsservices.ListResult{}, err
	}
	cleanTarget = filepath.Clean(cleanTarget)
	if !hasPathPrefix(cleanTarget, root) {
		return fsservices.ListResult{}, fmt.Errorf("path %s is outside allowed root %s", cleanTarget, root)
	}

	return adapter.ListChildren(cleanTarget)
}

func (m *ServiceManager) listSpectraChildren(def serviceDefinition, identifier string) (fsservices.ListResult, error) {
	if def.Spectra == nil {
		return fsservices.ListResult{}, fmt.Errorf("spectra service %s missing configuration", def.ID)
	}

	spectraFS, err := sdk.New(def.Spectra.ConfigPath)
	if err != nil {
		return fsservices.ListResult{}, err
	}
	defer spectraFS.Close()

	root := def.Spectra.RootID
	if root == "" {
		root = "root"
	}

	adapter, err := fsservices.NewSpectraFS(spectraFS, root, def.Spectra.World)
	if err != nil {
		return fsservices.ListResult{}, err
	}

	target := identifier
	if target == "" {
		target = root
	}

	return adapter.ListChildren(target)
}

var ErrServiceNotFound = fmt.Errorf("service not found")

func (m *ServiceManager) serviceDefinition(id string) (serviceDefinition, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	def, ok := m.services[id]
	if !ok {
		return serviceDefinition{}, ErrServiceNotFound
	}
	return def, nil
}

// findSpectraServiceByWorld finds the first Spectra service with the given world.
func (m *ServiceManager) findSpectraServiceByWorld(world string) (serviceDefinition, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	for _, svc := range m.services {
		if svc.Type == ServiceTypeSpectra && svc.Spectra != nil && svc.Spectra.World == world {
			return svc, nil
		}
	}
	return serviceDefinition{}, ErrServiceNotFound
}

// GetServiceDefinitionByWorld finds the first Spectra service with the given world.
func (m *ServiceManager) GetServiceDefinitionByWorld(world string) (ServiceDefinition, error) {
	return m.findSpectraServiceByWorld(world)
}

func (m *ServiceManager) AcquireAdapter(def serviceDefinition, rootID, connectionID string) (fsservices.FSAdapter, func(), error) {
	return m.AcquireAdapterWithOverride(def, rootID, connectionID, "")
}

// AcquireAdapterWithOverride acquires an adapter, with optional Spectra config override path
func (m *ServiceManager) AcquireAdapterWithOverride(def serviceDefinition, rootID, connectionID, spectraConfigOverridePath string) (fsservices.FSAdapter, func(), error) {
	switch def.Type {
	case ServiceTypeLocal:
		if rootID == "" {
			return nil, nil, fmt.Errorf("local root path cannot be empty")
		}
		adapter, err := fsservices.NewLocalFS(rootID)
		if err != nil {
			return nil, nil, err
		}
		return adapter, func() {}, nil
	case ServiceTypeSpectra:
		if def.Spectra == nil {
			return nil, nil, fmt.Errorf("spectra configuration missing")
		}
		return m.acquireSpectraAdapter(def, rootID, connectionID, spectraConfigOverridePath)
	default:
		return nil, nil, fmt.Errorf("unsupported service type: %s", def.Type)
	}
}

func (m *ServiceManager) acquireSpectraAdapter(def serviceDefinition, rootID, connectionID, spectraConfigOverridePath string) (fsservices.FSAdapter, func(), error) {
	root := def.Spectra.RootID
	if rootID != "" {
		root = rootID
	}

	// Use override config path if provided, otherwise use original
	configPath := def.Spectra.ConfigPath
	if spectraConfigOverridePath != "" {
		configPath = spectraConfigOverridePath
	}

	if connectionID == "" {
		spectraFS, err := sdk.New(configPath)
		if err != nil {
			return nil, nil, err
		}

		adapter, err := fsservices.NewSpectraFS(spectraFS, root, def.Spectra.World)
		if err != nil {
			_ = spectraFS.Close()
			return nil, nil, err
		}
		return adapter, func() {
			_ = spectraFS.Close()
		}, nil
	}

	m.mu.Lock()
	conn, ok := m.connections[connectionID]
	if ok {
		if conn.typ != ServiceTypeSpectra {
			m.mu.Unlock()
			return nil, nil, fmt.Errorf("connection %s is already in use by a different service type", connectionID)
		}
		conn.refCount++
		spectraFS := conn.spectra
		m.mu.Unlock()

		adapter, err := fsservices.NewSpectraFS(spectraFS, root, def.Spectra.World)
		if err != nil {
			m.ReleaseConnection(connectionID)
			return nil, nil, err
		}

		return adapter, func() {
			m.ReleaseConnection(connectionID)
		}, nil
	}
	m.mu.Unlock()

	spectraFS, err := sdk.New(configPath)
	if err != nil {
		return nil, nil, err
	}

	adapter, err := fsservices.NewSpectraFS(spectraFS, root, def.Spectra.World)
	if err != nil {
		_ = spectraFS.Close()
		return nil, nil, err
	}

	m.mu.Lock()
	if existing, exists := m.connections[connectionID]; exists {
		existing.refCount++
		spectraShared := existing.spectra
		m.mu.Unlock()

		_ = spectraFS.Close()

		adapter, err := fsservices.NewSpectraFS(spectraShared, root, def.Spectra.World)
		if err != nil {
			m.ReleaseConnection(connectionID)
			return nil, nil, err
		}
		return adapter, func() {
			m.ReleaseConnection(connectionID)
		}, nil
	}

	m.connections[connectionID] = &serviceConnection{
		typ:      ServiceTypeSpectra,
		spectra:  spectraFS,
		refCount: 1,
	}
	m.mu.Unlock()

	return adapter, func() {
		m.ReleaseConnection(connectionID)
	}, nil
}

func (m *ServiceManager) ReleaseConnection(connectionID string) {
	if connectionID == "" {
		return
	}

	m.mu.Lock()
	conn, ok := m.connections[connectionID]
	if !ok {
		m.mu.Unlock()
		return
	}

	conn.refCount--
	if conn.refCount <= 0 {
		delete(m.connections, connectionID)
		switch conn.typ {
		case ServiceTypeSpectra:
			if conn.spectra != nil {
				_ = conn.spectra.Close()
			}
		}
	}
	m.mu.Unlock()
}

func (m *ServiceManager) GetServiceDefinition(id string) (serviceDefinition, error) {
	return m.serviceDefinition(id)
}

func hasPathPrefix(path, root string) bool {
	if path == root {
		return true
	}

	sep := string(filepath.Separator)
	if strings.HasPrefix(path, root+sep) {
		return true
	}

	if sep != "/" && strings.HasPrefix(path, root+"/") {
		return true
	}

	return false
}
