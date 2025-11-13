package corebridge

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/Project-Sylos/Migration-Engine/pkg/fsservices"
	"github.com/Project-Sylos/Migration-Engine/pkg/logservice"
	"github.com/Project-Sylos/Migration-Engine/pkg/migration"
	"github.com/Project-Sylos/Migration-Engine/pkg/queue"
	"github.com/Project-Sylos/Spectra/sdk"
	"github.com/Project-Sylos/Sylos-API/pkg/config"
	"github.com/rs/xid"
	"github.com/rs/zerolog"
)

var (
	ErrMigrationNotFound = errors.New("migration not found")
	ErrServiceNotFound   = errors.New("service not found")
)

type ServiceType string

const (
	ServiceTypeLocal   ServiceType = "local"
	ServiceTypeSpectra ServiceType = "spectra"
)

type Source struct {
	ID          string            `json:"id"`
	DisplayName string            `json:"displayName"`
	Type        ServiceType       `json:"type"`
	Metadata    map[string]string `json:"metadata,omitempty"`
}

type ListChildrenRequest struct {
	ServiceID  string
	Identifier string
}

type FolderDescriptor struct {
	ID           string `json:"id"`
	ParentID     string `json:"parentId,omitempty"`
	ParentPath   string `json:"parentPath,omitempty"`
	DisplayName  string `json:"displayName,omitempty"`
	LocationPath string `json:"locationPath,omitempty"`
	LastUpdated  string `json:"lastUpdated,omitempty"`
	DepthLevel   int    `json:"depthLevel,omitempty"`
	Type         string `json:"type,omitempty"`
}

type ServiceSelection struct {
	ServiceID string           `json:"serviceId"`
	Root      FolderDescriptor `json:"root"`
}

type VerificationOptions struct {
	AllowPending  bool `json:"allowPending"`
	AllowFailed   bool `json:"allowFailed"`
	AllowNotOnSrc bool `json:"allowNotOnSrc"`
}

type MigrationOptions struct {
	MigrationID        string              `json:"migrationId,omitempty"`
	DatabasePath       string              `json:"databasePath,omitempty"`
	RemoveExistingDB   bool                `json:"removeExistingDatabase,omitempty"`
	WorkerCount        int                 `json:"workerCount,omitempty"`
	MaxRetries         int                 `json:"maxRetries,omitempty"`
	CoordinatorLead    int                 `json:"coordinatorLead,omitempty"`
	LogAddress         string              `json:"logAddress,omitempty"`
	LogLevel           string              `json:"logLevel,omitempty"`
	SkipLogListener    bool                `json:"skipLogListener,omitempty"`
	StartupDelaySec    int                 `json:"startupDelaySeconds,omitempty"`
	ProgressTickMillis int                 `json:"progressTickMillis,omitempty"`
	Verification       VerificationOptions `json:"verification,omitempty"`
}

type StartMigrationRequest struct {
	Source      ServiceSelection `json:"source"`
	Destination ServiceSelection `json:"destination"`
	Options     MigrationOptions `json:"options"`
}

type Migration struct {
	ID            string    `json:"id"`
	SourceID      string    `json:"sourceId"`
	DestinationID string    `json:"destinationId"`
	StartedAt     time.Time `json:"startedAt"`
	Status        string    `json:"status"`
}

type Status struct {
	Migration
	CompletedAt *time.Time  `json:"completedAt,omitempty"`
	Error       string      `json:"error,omitempty"`
	Result      *ResultView `json:"result,omitempty"`
}

type ResultView struct {
	RootSummary  RootSummaryView  `json:"rootSummary"`
	Runtime      RuntimeStatsView `json:"runtime"`
	Verification VerificationView `json:"verification"`
}

type RootSummaryView struct {
	SrcRoots int `json:"srcRoots"`
	DstRoots int `json:"dstRoots"`
}

type QueueStatsView struct {
	Name         string `json:"name"`
	Round        int    `json:"round"`
	Pending      int    `json:"pending"`
	InProgress   int    `json:"inProgress"`
	TotalTracked int    `json:"totalTracked"`
	Workers      int    `json:"workers"`
}

type RuntimeStatsView struct {
	Duration string         `json:"duration"`
	Src      QueueStatsView `json:"src"`
	Dst      QueueStatsView `json:"dst"`
}

type VerificationView struct {
	SrcTotal    int `json:"srcTotal"`
	DstTotal    int `json:"dstTotal"`
	SrcPending  int `json:"srcPending"`
	DstPending  int `json:"dstPending"`
	SrcFailed   int `json:"srcFailed"`
	DstFailed   int `json:"dstFailed"`
	DstNotOnSrc int `json:"dstNotOnSrc"`
}

type Bridge interface {
	ListSources(ctx context.Context) ([]Source, error)
	ListChildren(ctx context.Context, req ListChildrenRequest) (fsservices.ListResult, error)
	StartMigration(ctx context.Context, req StartMigrationRequest) (Migration, error)
	GetMigrationStatus(ctx context.Context, id string) (Status, error)
}

type Manager struct {
	logger     zerolog.Logger
	cfg        config.Config
	services   map[string]serviceDefinition
	migrations map[string]*migrationRecord
	mu         sync.RWMutex
}

type serviceDefinition struct {
	ID      string
	Name    string
	Type    ServiceType
	Local   *config.LocalServiceConfig
	Spectra *config.SpectraServiceConfig
}

type migrationRecord struct {
	ID            string
	SourceID      string
	DestinationID string
	Status        string
	StartedAt     time.Time
	CompletedAt   *time.Time
	Result        *migration.Result
	Error         string
}

const (
	migrationStatusRunning   = "running"
	migrationStatusCompleted = "completed"
	migrationStatusFailed    = "failed"
)

func NewManager(logger zerolog.Logger, cfg config.Config) (*Manager, error) {
	manager := &Manager{
		logger:     logger,
		cfg:        cfg,
		services:   make(map[string]serviceDefinition),
		migrations: make(map[string]*migrationRecord),
	}

	if err := manager.loadServices(); err != nil {
		return nil, err
	}

	return manager, nil
}

func (m *Manager) loadServices() error {
	for _, svc := range m.cfg.Services.Local {
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

	for _, svc := range m.cfg.Services.Spectra {
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

func (m *Manager) ListSources(ctx context.Context) ([]Source, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	sources := make([]Source, 0, len(m.services))
	for _, svc := range m.services {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		metadata := map[string]string{
			"name": svc.Name,
		}
		switch svc.Type {
		case ServiceTypeLocal:
			if svc.Local != nil && svc.Local.RootPath != "" {
				metadata["rootPath"] = svc.Local.RootPath
			}
		case ServiceTypeSpectra:
			if svc.Spectra != nil {
				metadata["world"] = svc.Spectra.World
				metadata["configPath"] = svc.Spectra.ConfigPath
			}
		}

		sources = append(sources, Source{
			ID:          svc.ID,
			DisplayName: svc.Name,
			Type:        svc.Type,
			Metadata:    metadata,
		})
	}

	sort.Slice(sources, func(i, j int) bool {
		return sources[i].DisplayName < sources[j].DisplayName
	})

	return sources, nil
}

func (m *Manager) ListChildren(ctx context.Context, req ListChildrenRequest) (fsservices.ListResult, error) {
	def, err := m.serviceDefinition(req.ServiceID)
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

func (m *Manager) StartMigration(ctx context.Context, req StartMigrationRequest) (Migration, error) {
	srcDef, err := m.serviceDefinition(req.Source.ServiceID)
	if err != nil {
		return Migration{}, err
	}
	dstDef, err := m.serviceDefinition(req.Destination.ServiceID)
	if err != nil {
		return Migration{}, err
	}

	srcFolder, err := folderFromDescriptor(req.Source.Root)
	if err != nil {
		return Migration{}, fmt.Errorf("invalid source root: %w", err)
	}
	dstFolder, err := folderFromDescriptor(req.Destination.Root)
	if err != nil {
		return Migration{}, fmt.Errorf("invalid destination root: %w", err)
	}

	migrationID := req.Options.MigrationID
	if migrationID == "" {
		migrationID = xid.New().String()
	}

	record := &migrationRecord{
		ID:            migrationID,
		SourceID:      srcDef.ID,
		DestinationID: dstDef.ID,
		Status:        migrationStatusRunning,
		StartedAt:     time.Now().UTC(),
	}

	m.mu.Lock()
	if _, exists := m.migrations[migrationID]; exists {
		m.mu.Unlock()
		return Migration{}, fmt.Errorf("migration %s already exists", migrationID)
	}
	m.migrations[migrationID] = record
	m.mu.Unlock()

	go m.runMigration(record, srcDef, dstDef, srcFolder, dstFolder, req.Options)

	return Migration{
		ID:            record.ID,
		SourceID:      record.SourceID,
		DestinationID: record.DestinationID,
		StartedAt:     record.StartedAt,
		Status:        record.Status,
	}, nil
}

func (m *Manager) GetMigrationStatus(ctx context.Context, id string) (Status, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	record, ok := m.migrations[id]
	if !ok {
		return Status{}, ErrMigrationNotFound
	}

	return record.toStatus(), nil
}

func (m *Manager) runMigration(record *migrationRecord, srcDef, dstDef serviceDefinition, srcFolder, dstFolder fsservices.Folder, opts MigrationOptions) {
	m.logger.Info().
		Str("migration_id", record.ID).
		Str("source", srcDef.ID).
		Str("destination", dstDef.ID).
		Msg("starting migration")

	result, err := m.executeMigration(record.ID, srcDef, dstDef, srcFolder, dstFolder, opts)

	m.mu.Lock()
	defer m.mu.Unlock()

	if err != nil {
		record.Status = migrationStatusFailed
		record.Error = err.Error()
		finished := time.Now().UTC()
		record.CompletedAt = &finished
		m.logger.Error().
			Err(err).
			Str("migration_id", record.ID).
			Msg("migration failed")
		return
	}

	finished := time.Now().UTC()
	record.Status = migrationStatusCompleted
	record.CompletedAt = &finished
	record.Result = result
	m.logger.Info().
		Str("migration_id", record.ID).
		Msg("migration completed successfully")
}

func (m *Manager) executeMigration(migrationID string, srcDef, dstDef serviceDefinition, srcFolder, dstFolder fsservices.Folder, opts MigrationOptions) (*migration.Result, error) {
	dbPath, err := m.resolveDatabasePath(opts.DatabasePath, migrationID)
	if err != nil {
		return nil, err
	}

	srcAdapter, srcCleanup, err := m.instantiateAdapterForMigration(srcDef, srcFolder.Id)
	if err != nil {
		return nil, fmt.Errorf("source adapter: %w", err)
	}
	defer srcCleanup()

	dstAdapter, dstCleanup, err := m.instantiateAdapterForMigration(dstDef, dstFolder.Id)
	if err != nil {
		return nil, fmt.Errorf("destination adapter: %w", err)
	}
	defer dstCleanup()

	cfg := migration.Config{
		Database: migration.DatabaseConfig{
			Path:           dbPath,
			RemoveExisting: opts.RemoveExistingDB,
		},
		Source: migration.Service{
			Name:    srcDef.Name,
			Adapter: srcAdapter,
		},
		Destination: migration.Service{
			Name:    dstDef.Name,
			Adapter: dstAdapter,
		},
		WorkerCount:     m.selectWorkerCount(opts.WorkerCount),
		MaxRetries:      m.selectMaxRetries(opts.MaxRetries),
		CoordinatorLead: m.selectCoordinatorLead(opts.CoordinatorLead),
		LogAddress:      m.selectLogAddress(opts.LogAddress),
		LogLevel:        m.selectLogLevel(opts.LogLevel),
		SkipListener:    opts.SkipLogListener || m.cfg.Runtime.SkipLogListener,
		StartupDelay:    time.Duration(opts.StartupDelaySec) * time.Second,
		ProgressTick:    time.Duration(opts.ProgressTickMillis) * time.Millisecond,
		Verification: migration.VerifyOptions{
			AllowPending:  opts.Verification.AllowPending,
			AllowFailed:   opts.Verification.AllowFailed,
			AllowNotOnSrc: opts.Verification.AllowNotOnSrc,
		},
	}

	if cfg.StartupDelay == 0 {
		cfg.StartupDelay = 3 * time.Second
	}
	if cfg.ProgressTick == 0 {
		cfg.ProgressTick = 500 * time.Millisecond
	}

	if err := cfg.SetRootFolders(srcFolder, dstFolder); err != nil {
		return nil, err
	}

	result, err := migration.LetsMigrate(cfg)
	if logservice.LS != nil {
		_ = logservice.LS.Close()
	}
	if err != nil {
		return nil, err
	}

	return &result, nil
}

func (m *Manager) selectWorkerCount(value int) int {
	if value > 0 {
		return value
	}
	if m.cfg.Runtime.DefaultWorkerCount > 0 {
		return m.cfg.Runtime.DefaultWorkerCount
	}
	return 10
}

func (m *Manager) selectMaxRetries(value int) int {
	if value > 0 {
		return value
	}
	if m.cfg.Runtime.DefaultMaxRetries > 0 {
		return m.cfg.Runtime.DefaultMaxRetries
	}
	return 3
}

func (m *Manager) selectCoordinatorLead(value int) int {
	if value > 0 {
		return value
	}
	if m.cfg.Runtime.DefaultCoordinatorLead > 0 {
		return m.cfg.Runtime.DefaultCoordinatorLead
	}
	return 4
}

func (m *Manager) selectLogAddress(value string) string {
	if value != "" {
		return value
	}
	return m.cfg.Runtime.LogAddress
}

func (m *Manager) selectLogLevel(value string) string {
	if value != "" {
		return value
	}
	if m.cfg.Runtime.LogLevel != "" {
		return m.cfg.Runtime.LogLevel
	}
	return "info"
}

func (m *Manager) resolveDatabasePath(path, migrationID string) (string, error) {
	if path == "" {
		filename := fmt.Sprintf("%s.db", migrationID)
		path = filepath.Join(m.cfg.Runtime.DataDir, filename)
	}

	abs, err := filepath.Abs(path)
	if err != nil {
		return "", fmt.Errorf("failed to resolve database path: %w", err)
	}

	if err := os.MkdirAll(filepath.Dir(abs), 0o755); err != nil {
		return "", fmt.Errorf("failed to create database directory: %w", err)
	}

	return filepath.Clean(abs), nil
}

func (m *Manager) instantiateAdapterForMigration(def serviceDefinition, rootID string) (fsservices.FSAdapter, func(), error) {
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
		spectraFS, err := sdk.New(def.Spectra.ConfigPath)
		if err != nil {
			return nil, nil, err
		}
		root := def.Spectra.RootID
		if rootID != "" {
			root = rootID
		}
		adapter, err := fsservices.NewSpectraFS(spectraFS, root, def.Spectra.World)
		if err != nil {
			_ = spectraFS.Close()
			return nil, nil, err
		}
		cleanup := func() {
			_ = spectraFS.Close()
		}
		return adapter, cleanup, nil
	default:
		return nil, nil, fmt.Errorf("unsupported service type: %s", def.Type)
	}
}

func (m *Manager) listLocalChildren(def serviceDefinition, identifier string) (fsservices.ListResult, error) {
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

func (m *Manager) listSpectraChildren(def serviceDefinition, identifier string) (fsservices.ListResult, error) {
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

func (m *Manager) serviceDefinition(id string) (serviceDefinition, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	def, ok := m.services[id]
	if !ok {
		return serviceDefinition{}, ErrServiceNotFound
	}
	return def, nil
}

func folderFromDescriptor(desc FolderDescriptor) (fsservices.Folder, error) {
	if desc.ID == "" {
		return fsservices.Folder{}, fmt.Errorf("folder id cannot be empty")
	}

	folder := fsservices.Folder{
		Id:           desc.ID,
		ParentId:     desc.ParentID,
		ParentPath:   desc.ParentPath,
		DisplayName:  desc.DisplayName,
		LocationPath: desc.LocationPath,
		LastUpdated:  desc.LastUpdated,
		DepthLevel:   desc.DepthLevel,
		Type:         desc.Type,
	}

	if folder.DisplayName == "" {
		folder.DisplayName = folder.Id
	}
	if folder.LocationPath == "" {
		folder.LocationPath = "/"
	}
	if folder.Type == "" {
		folder.Type = fsservices.NodeTypeFolder
	}
	if folder.LastUpdated == "" {
		folder.LastUpdated = time.Now().UTC().Format(time.RFC3339)
	}

	return folder, nil
}

func (r *migrationRecord) toStatus() Status {
	status := Status{
		Migration: Migration{
			ID:            r.ID,
			SourceID:      r.SourceID,
			DestinationID: r.DestinationID,
			StartedAt:     r.StartedAt,
			Status:        r.Status,
		},
	}

	if r.CompletedAt != nil {
		status.CompletedAt = r.CompletedAt
	}
	if r.Error != "" {
		status.Error = r.Error
	}
	if r.Result != nil {
		status.Result = resultToView(r.Result)
	}

	return status
}

func resultToView(res *migration.Result) *ResultView {
	if res == nil {
		return nil
	}

	return &ResultView{
		RootSummary: RootSummaryView{
			SrcRoots: res.RootSummary.SrcRoots,
			DstRoots: res.RootSummary.DstRoots,
		},
		Runtime: RuntimeStatsView{
			Duration: res.Runtime.Duration.String(),
			Src:      queueStatsToView(res.Runtime.Src),
			Dst:      queueStatsToView(res.Runtime.Dst),
		},
		Verification: VerificationView{
			SrcTotal:    res.Verification.SrcTotal,
			DstTotal:    res.Verification.DstTotal,
			SrcPending:  res.Verification.SrcPending,
			DstPending:  res.Verification.DstPending,
			SrcFailed:   res.Verification.SrcFailed,
			DstFailed:   res.Verification.DstFailed,
			DstNotOnSrc: res.Verification.DstNotOnSrc,
		},
	}
}

func queueStatsToView(stats queue.QueueStats) QueueStatsView {
	return QueueStatsView{
		Name:         stats.Name,
		Round:        stats.Round,
		Pending:      stats.Pending,
		InProgress:   stats.InProgress,
		TotalTracked: stats.TotalTracked,
		Workers:      stats.Workers,
	}
}

func hasPathPrefix(path, root string) bool {
	if path == root {
		return true
	}

	sep := string(os.PathSeparator)
	if strings.HasPrefix(path, root+sep) {
		return true
	}

	if sep != "/" && strings.HasPrefix(path, root+"/") {
		return true
	}

	return false
}
