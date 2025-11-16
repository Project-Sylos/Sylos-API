package corebridge

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/Project-Sylos/Migration-Engine/pkg/db"
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

const (
	rootRoleSource      = "source"
	rootRoleDestination = "destination"
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
	Role       string // "source" or "destination" - used to map "spectra" to the correct world
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
	ServiceID    string           `json:"serviceId"`
	ConnectionID string           `json:"connectionId,omitempty"`
	Root         FolderDescriptor `json:"root"`
}

type VerificationOptions struct {
	AllowPending  bool `json:"allowPending"`
	AllowFailed   bool `json:"allowFailed"`
	AllowNotOnSrc bool `json:"allowNotOnSrc"`
}

type MigrationOptions struct {
	MigrationID             string              `json:"migrationId,omitempty"`
	DatabasePath            string              `json:"databasePath,omitempty"`
	RemoveExistingDB        bool                `json:"removeExistingDatabase,omitempty"`
	UsePreseededDB          bool                `json:"usePreseededDatabase,omitempty"`
	SourceConnectionID      string              `json:"sourceConnectionId,omitempty"`
	DestinationConnectionID string              `json:"destinationConnectionId,omitempty"`
	WorkerCount             int                 `json:"workerCount,omitempty"`
	MaxRetries              int                 `json:"maxRetries,omitempty"`
	CoordinatorLead         int                 `json:"coordinatorLead,omitempty"`
	LogAddress              string              `json:"logAddress,omitempty"`
	LogLevel                string              `json:"logLevel,omitempty"`
	EnableLoggingTerminal   bool                `json:"enableLoggingTerminal,omitempty"`
	StartupDelaySec         int                 `json:"startupDelaySeconds,omitempty"`
	ProgressTickMillis      int                 `json:"progressTickMillis,omitempty"`
	Verification            VerificationOptions `json:"verification,omitempty"`
}

type StartMigrationRequest struct {
	MigrationID string           `json:"migrationId,omitempty"`
	Options     MigrationOptions `json:"options"`
}

type SetRootRequest struct {
	MigrationID  string           `json:"migrationId,omitempty"`
	Role         string           `json:"role"`
	ServiceID    string           `json:"serviceId"`
	ConnectionID string           `json:"connectionId,omitempty"`
	Root         FolderDescriptor `json:"root"`
}

type SetRootResponse struct {
	MigrationID             string                     `json:"migrationId"`
	Role                    string                     `json:"role"`
	Ready                   bool                       `json:"ready"`
	DatabasePath            string                     `json:"databasePath,omitempty"`
	RootSummary             *migration.RootSeedSummary `json:"rootSummary,omitempty"`
	SourceConnectionID      string                     `json:"sourceConnectionId,omitempty"`
	DestinationConnectionID string                     `json:"destinationConnectionId,omitempty"`
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

type QueueStatsSnapshot struct {
	Round        int `json:"round"`
	Pending      int `json:"pending"`
	InProgress   int `json:"inProgress"`
	TotalTracked int `json:"totalTracked"`
	Workers      int `json:"workers"`
}

type ProgressEvent struct {
	Event       string             `json:"event"`
	Timestamp   time.Time          `json:"timestamp"`
	Migration   Status             `json:"migration"`
	Source      QueueStatsSnapshot `json:"source"`
	Destination QueueStatsSnapshot `json:"destination"`
}

type UploadMigrationDBRequest struct {
	Filename  string `json:"filename"`
	Overwrite bool   `json:"overwrite,omitempty"`
}

type UploadMigrationDBResponse struct {
	Success bool   `json:"success"`
	Error   string `json:"error,omitempty"`
	Path    string `json:"path,omitempty"`
}

type MigrationDBInfo struct {
	Filename   string    `json:"filename"`
	Path       string    `json:"path"`
	Size       int64     `json:"size"`
	ModifiedAt time.Time `json:"modifiedAt"`
}

type Bridge interface {
	ListSources(ctx context.Context) ([]Source, error)
	ListChildren(ctx context.Context, req ListChildrenRequest) (fsservices.ListResult, error)
	SetRoot(ctx context.Context, req SetRootRequest) (SetRootResponse, error)
	StartMigration(ctx context.Context, req StartMigrationRequest) (Migration, error)
	GetMigrationStatus(ctx context.Context, id string) (Status, error)
	InspectMigrationStatus(ctx context.Context, migrationID string) (migration.MigrationStatus, error)
	InspectMigrationStatusFromDB(ctx context.Context, dbPath string) (migration.MigrationStatus, error)
	UploadMigrationDB(ctx context.Context, filename string, data []byte, overwrite bool) (UploadMigrationDBResponse, error)
	ListMigrationDBs(ctx context.Context) ([]MigrationDBInfo, error)
	SubscribeProgress(ctx context.Context, id string) (<-chan ProgressEvent, func(), error)
	ToggleLogTerminal(ctx context.Context, enable bool, logAddress string) error
}

type Manager struct {
	logger        zerolog.Logger
	cfg           config.Config
	services      map[string]serviceDefinition
	migrations    map[string]*migrationRecord
	plans         map[string]*rootPlan
	subscribers   map[string]map[string]chan ProgressEvent
	connections   map[string]*serviceConnection
	logTerminal   *exec.Cmd  // Currently running log terminal process
	logTerminalMu sync.Mutex // Protects logTerminal
	mu            sync.RWMutex
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

type rootPlan struct {
	HasSource               bool
	SourceDefinition        serviceDefinition
	SourceRoot              fsservices.Folder
	SourceConnectionID      string
	HasDestination          bool
	DestinationDefinition   serviceDefinition
	DestinationRoot         fsservices.Folder
	DestinationConnectionID string
	DatabasePath            string
	RootSummary             migration.RootSeedSummary
	Seeded                  bool
	Seeding                 bool
}

type serviceConnection struct {
	typ      ServiceType
	spectra  *sdk.SpectraFS
	refCount int
}

const (
	migrationStatusRunning   = "running"
	migrationStatusCompleted = "completed"
	migrationStatusFailed    = "failed"
)

func NewManager(logger zerolog.Logger, cfg config.Config) (*Manager, error) {
	manager := &Manager{
		logger:      logger,
		cfg:         cfg,
		services:    make(map[string]serviceDefinition),
		migrations:  make(map[string]*migrationRecord),
		plans:       make(map[string]*rootPlan),
		subscribers: make(map[string]map[string]chan ProgressEvent),
		connections: make(map[string]*serviceConnection),
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

func (m *Manager) ListChildren(ctx context.Context, req ListChildrenRequest) (fsservices.ListResult, error) {
	serviceID := req.ServiceID

	// Map "spectra" virtual service to the appropriate world based on role
	if serviceID == "spectra" {
		role := strings.ToLower(strings.TrimSpace(req.Role))
		var world string
		switch role {
		case rootRoleSource:
			world = "primary"
		case rootRoleDestination:
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

func (m *Manager) SetRoot(ctx context.Context, req SetRootRequest) (SetRootResponse, error) {
	role := strings.ToLower(strings.TrimSpace(req.Role))
	if role != rootRoleSource && role != rootRoleDestination {
		return SetRootResponse{}, fmt.Errorf("role must be 'source' or 'destination'")
	}

	if req.ServiceID == "" {
		return SetRootResponse{}, fmt.Errorf("serviceId is required")
	}

	// Map "spectra" virtual service to the appropriate world based on role
	serviceID := req.ServiceID
	var serviceDef serviceDefinition
	var err error

	if serviceID == "spectra" {
		var world string
		switch role {
		case rootRoleSource:
			world = "primary"
		case rootRoleDestination:
			world = "s1"
		default:
			return SetRootResponse{}, fmt.Errorf("role must be 'source' or 'destination' when using 'spectra' service")
		}

		serviceDef, err = m.findSpectraServiceByWorld(world)
		if err != nil {
			return SetRootResponse{}, fmt.Errorf("spectra service with world %s not found: %w", world, err)
		}
	} else {
		serviceDef, err = m.serviceDefinition(serviceID)
		if err != nil {
			return SetRootResponse{}, err
		}
	}

	folder, err := folderFromDescriptor(req.Root)
	if err != nil {
		return SetRootResponse{}, fmt.Errorf("invalid %s root: %w", role, err)
	}

	migrationID := req.MigrationID
	if migrationID == "" {
		migrationID = xid.New().String()
	}

	m.mu.Lock()
	if _, exists := m.migrations[migrationID]; exists {
		m.mu.Unlock()
		return SetRootResponse{}, fmt.Errorf("migration %s already exists", migrationID)
	}

	plan := m.plans[migrationID]
	if plan == nil {
		plan = &rootPlan{}
		m.plans[migrationID] = plan
	}

	plan.Seeded = false
	plan.Seeding = false
	plan.DatabasePath = ""
	plan.RootSummary = migration.RootSeedSummary{}

	if role == rootRoleSource {
		plan.HasSource = true
		plan.SourceDefinition = serviceDef
		plan.SourceRoot = folder
		plan.SourceConnectionID = req.ConnectionID
	} else {
		plan.HasDestination = true
		plan.DestinationDefinition = serviceDef
		plan.DestinationRoot = folder
		plan.DestinationConnectionID = req.ConnectionID
	}

	planReady := plan.HasSource && plan.HasDestination
	m.mu.Unlock()

	if planReady {
		if _, _, _, err := m.seedPlanIfReady(migrationID); err != nil {
			return SetRootResponse{}, err
		}
	}

	m.mu.RLock()
	plan = m.plans[migrationID]
	var (
		ready      bool
		dbPath     string
		summary    *migration.RootSeedSummary
		sourceConn string
		destConn   string
	)
	if plan != nil {
		ready = plan.Seeded
		dbPath = plan.DatabasePath
		sourceConn = plan.SourceConnectionID
		destConn = plan.DestinationConnectionID
		if plan.Seeded {
			s := plan.RootSummary
			summary = &s
		}
	}
	m.mu.RUnlock()

	return SetRootResponse{
		MigrationID:             migrationID,
		Role:                    role,
		Ready:                   ready,
		DatabasePath:            dbPath,
		RootSummary:             summary,
		SourceConnectionID:      sourceConn,
		DestinationConnectionID: destConn,
	}, nil
}

func (m *Manager) seedPlanIfReady(migrationID string) (bool, migration.RootSeedSummary, string, error) {
	m.mu.Lock()
	plan, ok := m.plans[migrationID]
	if !ok || !plan.HasSource || !plan.HasDestination {
		m.mu.Unlock()
		return false, migration.RootSeedSummary{}, "", nil
	}
	if plan.Seeding {
		seeded := plan.Seeded
		summary := plan.RootSummary
		dbPath := plan.DatabasePath
		m.mu.Unlock()
		return seeded, summary, dbPath, nil
	}

	plan.Seeding = true
	sourceDef := plan.SourceDefinition
	destDef := plan.DestinationDefinition
	sourceRoot := plan.SourceRoot
	destRoot := plan.DestinationRoot
	sourceConn := plan.SourceConnectionID
	destConn := plan.DestinationConnectionID
	dbPath := plan.DatabasePath
	m.mu.Unlock()

	if dbPath == "" {
		var err error
		dbPath, err = m.resolveDatabasePath("", migrationID)
		if err != nil {
			m.mu.Lock()
			if plan, ok := m.plans[migrationID]; ok {
				plan.Seeding = false
			}
			m.mu.Unlock()
			return false, migration.RootSeedSummary{}, "", err
		}
	}

	// Check if database file already exists
	if _, err := os.Stat(dbPath); err == nil {
		// Database exists - validate schema to see if we should skip seeding
		database, err := db.NewDB(dbPath)
		if err == nil {
			// Validate schema - if valid, skip seeding and let LetsMigrate handle resume
			if err := database.ValidateCoreSchema(); err == nil {
				// Valid schema exists - check if it has roots already
				// If it does, we can skip seeding and let LetsMigrate resume
				status, inspectErr := migration.InspectMigrationStatus(database)
				closeErr := database.Close()
				if closeErr != nil {
					m.logger.Warn().Err(closeErr).Msg("failed to close database after inspection")
				}
				if inspectErr == nil && !status.IsEmpty() {
					// Database has valid schema and is not empty - skip seeding
					// LetsMigrate will handle resume automatically
					m.logger.Info().
						Str("migration_id", migrationID).
						Str("db_path", dbPath).
						Msg("database exists with valid schema, skipping seeding (will resume)")

					// Create a summary from the existing database state
					summary := migration.RootSeedSummary{
						SrcRoots: status.SrcTotal,
						DstRoots: status.DstTotal,
					}

					m.mu.Lock()
					plan, ok = m.plans[migrationID]
					if !ok {
						m.mu.Unlock()
						return false, migration.RootSeedSummary{}, "", fmt.Errorf("migration %s plan removed during seeding", migrationID)
					}

					plan.Seeding = false
					if !plan.HasSource ||
						plan.SourceDefinition.ID != sourceDef.ID ||
						plan.SourceRoot.Id != sourceRoot.Id ||
						!plan.HasDestination ||
						plan.DestinationDefinition.ID != destDef.ID ||
						plan.DestinationRoot.Id != destRoot.Id {
						plan.Seeded = false
						plan.DatabasePath = ""
						plan.RootSummary = migration.RootSeedSummary{}
						m.mu.Unlock()
						return false, migration.RootSeedSummary{}, "", nil
					}

					plan.Seeded = true
					plan.DatabasePath = dbPath
					plan.RootSummary = summary
					plan.SourceConnectionID = sourceConn
					plan.DestinationConnectionID = destConn
					m.mu.Unlock()

					return true, summary, dbPath, nil
				}
				// If inspection failed or DB is empty, fall through to seeding
			}
			// If schema validation failed, fall through to create fresh DB
		}
		// If opening DB failed, fall through to create fresh DB
	}

	// Database doesn't exist or has invalid schema - create fresh and seed roots
	database, err := migration.SetupDatabase(migration.DatabaseConfig{
		Path:           dbPath,
		RemoveExisting: true,
	})
	if err != nil {
		m.mu.Lock()
		if plan, ok := m.plans[migrationID]; ok {
			plan.Seeding = false
		}
		m.mu.Unlock()
		return false, migration.RootSeedSummary{}, "", err
	}

	summary, seedErr := migration.SeedRootTasks(database, sourceRoot, destRoot)
	closeErr := database.Close()
	if closeErr != nil {
		m.logger.Warn().Err(closeErr).Msg("failed to close database after seeding roots")
	}
	if seedErr != nil {
		m.mu.Lock()
		if plan, ok := m.plans[migrationID]; ok {
			plan.Seeding = false
		}
		m.mu.Unlock()
		return false, migration.RootSeedSummary{}, "", seedErr
	}

	m.mu.Lock()
	plan, ok = m.plans[migrationID]
	if !ok {
		m.mu.Unlock()
		return false, migration.RootSeedSummary{}, "", fmt.Errorf("migration %s plan removed during seeding", migrationID)
	}

	plan.Seeding = false
	if !plan.HasSource ||
		plan.SourceDefinition.ID != sourceDef.ID ||
		plan.SourceRoot.Id != sourceRoot.Id ||
		!plan.HasDestination ||
		plan.DestinationDefinition.ID != destDef.ID ||
		plan.DestinationRoot.Id != destRoot.Id {
		plan.Seeded = false
		plan.DatabasePath = ""
		plan.RootSummary = migration.RootSeedSummary{}
		m.mu.Unlock()
		return false, migration.RootSeedSummary{}, "", nil
	}

	plan.Seeded = true
	plan.DatabasePath = dbPath
	plan.RootSummary = summary
	plan.SourceConnectionID = sourceConn
	plan.DestinationConnectionID = destConn
	m.mu.Unlock()

	return true, summary, dbPath, nil
}

func (m *Manager) StartMigration(ctx context.Context, req StartMigrationRequest) (Migration, error) {
	migrationID := req.MigrationID
	if migrationID == "" {
		migrationID = req.Options.MigrationID
	}
	if migrationID == "" {
		return Migration{}, fmt.Errorf("migration id is required")
	}

	opts := req.Options
	opts.MigrationID = migrationID

	// If databasePath is provided, we can start migration directly from uploaded DB
	// Otherwise, we need roots to be set
	if opts.DatabasePath != "" {
		// Validate that the DB file exists
		if _, err := os.Stat(opts.DatabasePath); os.IsNotExist(err) {
			return Migration{}, fmt.Errorf("database file not found: %s", opts.DatabasePath)
		}

		// Check if DB has valid schema and get roots from it
		database, err := db.NewDB(opts.DatabasePath)
		if err != nil {
			return Migration{}, fmt.Errorf("failed to open database: %w", err)
		}
		defer database.Close()

		if err := database.ValidateCoreSchema(); err != nil {
			return Migration{}, fmt.Errorf("database schema invalid: %w", err)
		}

		// Get status to check if DB has roots
		status, err := migration.InspectMigrationStatus(database)
		if err != nil {
			return Migration{}, fmt.Errorf("failed to inspect database: %w", err)
		}

		if status.IsEmpty() {
			return Migration{}, fmt.Errorf("database is empty, roots must be set first")
		}

		// For uploaded DBs, we need to infer source/destination from the DB or require them in options
		// For now, use generic values - the migration will work with whatever is in the DB
		srcDef := serviceDefinition{
			ID:   "uploaded-db-source",
			Name: "Uploaded DB Source",
			Type: ServiceTypeLocal,
		}
		dstDef := serviceDefinition{
			ID:   "uploaded-db-destination",
			Name: "Uploaded DB Destination",
			Type: ServiceTypeLocal,
		}

		// Create minimal folder descriptors (not used when resuming from existing DB)
		srcFolder := fsservices.Folder{Id: "root", LocationPath: "/"}
		dstFolder := fsservices.Folder{Id: "root", LocationPath: "/"}

		opts.UsePreseededDB = true
		opts.RemoveExistingDB = false
		if opts.LogAddress == "" {
			opts.LogAddress = m.cfg.Runtime.LogAddress
		}
		if opts.LogLevel == "" {
			opts.LogLevel = m.cfg.Runtime.LogLevel
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
		if _, ok := m.subscribers[migrationID]; !ok {
			m.subscribers[migrationID] = make(map[string]chan ProgressEvent)
		}
		m.mu.Unlock()

		m.publishProgress(record.ID, "started", nil, nil)

		// Spawn log terminal if enabled
		if m.shouldEnableLoggingTerminal(opts) {
			logAddr := m.selectLogAddress(opts.LogAddress)
			if logAddr != "" {
				if err := m.spawnLogTerminal(logAddr); err != nil {
					m.logger.Warn().
						Err(err).
						Str("migration_id", migrationID).
						Msg("failed to spawn log terminal, continuing without it")
				}
			}
		}

		go m.runMigration(
			record,
			srcDef,
			dstDef,
			srcFolder,
			dstFolder,
			opts,
		)

		return Migration{
			ID:            record.ID,
			SourceID:      record.SourceID,
			DestinationID: record.DestinationID,
			StartedAt:     record.StartedAt,
			Status:        record.Status,
		}, nil
	}

	// Original flow: require roots to be set
	m.mu.RLock()
	plan, ok := m.plans[migrationID]
	if ok {
		planCopy := *plan
		m.mu.RUnlock()

		if !planCopy.HasSource || !planCopy.HasDestination {
			return Migration{}, fmt.Errorf("roots not fully configured for migration %s", migrationID)
		}

		if !planCopy.Seeded {
			if _, _, _, err := m.seedPlanIfReady(migrationID); err != nil {
				return Migration{}, err
			}
			m.mu.RLock()
			plan, ok = m.plans[migrationID]
			if ok {
				planCopy = *plan
			}
			m.mu.RUnlock()
			if !ok || !planCopy.Seeded {
				return Migration{}, fmt.Errorf("roots not fully configured for migration %s", migrationID)
			}
		}

		opts.DatabasePath = planCopy.DatabasePath
		opts.UsePreseededDB = true
		opts.RemoveExistingDB = false
		if opts.SourceConnectionID == "" {
			opts.SourceConnectionID = planCopy.SourceConnectionID
		}
		if opts.DestinationConnectionID == "" {
			opts.DestinationConnectionID = planCopy.DestinationConnectionID
		}
		if opts.LogAddress == "" {
			opts.LogAddress = m.cfg.Runtime.LogAddress
		}
		if opts.LogLevel == "" {
			opts.LogLevel = m.cfg.Runtime.LogLevel
		}

		record := &migrationRecord{
			ID:            migrationID,
			SourceID:      planCopy.SourceDefinition.ID,
			DestinationID: planCopy.DestinationDefinition.ID,
			Status:        migrationStatusRunning,
			StartedAt:     time.Now().UTC(),
		}

		m.mu.Lock()
		if _, exists := m.migrations[migrationID]; exists {
			m.mu.Unlock()
			return Migration{}, fmt.Errorf("migration %s already exists", migrationID)
		}
		m.migrations[migrationID] = record
		if _, ok := m.subscribers[migrationID]; !ok {
			m.subscribers[migrationID] = make(map[string]chan ProgressEvent)
		}
		m.mu.Unlock()

		m.publishProgress(record.ID, "started", nil, nil)

		// Spawn log terminal if enabled
		if m.shouldEnableLoggingTerminal(opts) {
			logAddr := m.selectLogAddress(opts.LogAddress)
			if logAddr != "" {
				if err := m.spawnLogTerminal(logAddr); err != nil {
					m.logger.Warn().
						Err(err).
						Str("migration_id", migrationID).
						Msg("failed to spawn log terminal, continuing without it")
				}
			}
		}

		go m.runMigration(
			record,
			planCopy.SourceDefinition,
			planCopy.DestinationDefinition,
			planCopy.SourceRoot,
			planCopy.DestinationRoot,
			opts,
		)

		return Migration{
			ID:            record.ID,
			SourceID:      record.SourceID,
			DestinationID: record.DestinationID,
			StartedAt:     record.StartedAt,
			Status:        record.Status,
		}, nil
	}
	m.mu.RUnlock()

	return Migration{}, fmt.Errorf("roots not set for migration %s and no databasePath provided", migrationID)
}

func (m *Manager) GetMigrationStatus(ctx context.Context, id string) (Status, error) {
	m.mu.RLock()
	record, ok := m.migrations[id]
	m.mu.RUnlock()

	if ok {
		return record.toStatus(), nil
	}

	// In-memory record not found - try to query database directly
	// This allows checking status of migrations that were started before server restart
	dbPath, err := m.resolveDatabasePath("", id)
	if err != nil {
		return Status{}, ErrMigrationNotFound
	}

	// Check if database file exists
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		return Status{}, ErrMigrationNotFound
	}

	// Open and inspect database
	dbStatus, err := m.InspectMigrationStatusFromDB(ctx, dbPath)
	if err != nil {
		return Status{}, ErrMigrationNotFound
	}

	// Convert migration.MigrationStatus to API Status format
	// We need to infer some fields that aren't in MigrationStatus
	status := Status{
		Migration: Migration{
			ID:        id,
			StartedAt: time.Now().UTC(), // We don't have this in DB, use current time as fallback
			Status:    migrationStatusCompleted,
		},
	}

	if dbStatus.HasPending() {
		status.Status = migrationStatusRunning
	} else if dbStatus.HasFailures() {
		status.Status = migrationStatusFailed
	}

	// Create result view from migration status
	if !dbStatus.IsEmpty() {
		status.Result = &ResultView{
			Verification: VerificationView{
				SrcTotal:   dbStatus.SrcTotal,
				DstTotal:   dbStatus.DstTotal,
				SrcPending: dbStatus.SrcPending,
				DstPending: dbStatus.DstPending,
				SrcFailed:  dbStatus.SrcFailed,
				DstFailed:  dbStatus.DstFailed,
			},
		}
	}

	return status, nil
}

func (m *Manager) InspectMigrationStatus(ctx context.Context, migrationID string) (migration.MigrationStatus, error) {
	// Resolve database path from migration ID
	dbPath, err := m.resolveDatabasePath("", migrationID)
	if err != nil {
		return migration.MigrationStatus{}, ErrMigrationNotFound
	}

	// Check if database file exists
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		return migration.MigrationStatus{}, ErrMigrationNotFound
	}

	return m.InspectMigrationStatusFromDB(ctx, dbPath)
}

func (m *Manager) InspectMigrationStatusFromDB(ctx context.Context, dbPath string) (migration.MigrationStatus, error) {
	// Open database
	database, err := db.NewDB(dbPath)
	if err != nil {
		return migration.MigrationStatus{}, fmt.Errorf("failed to open database: %w", err)
	}
	defer func() {
		if err := database.Close(); err != nil {
			m.logger.Warn().Err(err).Str("db_path", dbPath).Msg("failed to close database after inspection")
		}
	}()

	// Validate schema
	if err := database.ValidateCoreSchema(); err != nil {
		return migration.MigrationStatus{}, fmt.Errorf("database schema invalid: %w", err)
	}

	// Inspect status
	status, err := migration.InspectMigrationStatus(database)
	if err != nil {
		return migration.MigrationStatus{}, fmt.Errorf("failed to inspect migration status: %w", err)
	}

	return status, nil
}

func (m *Manager) UploadMigrationDB(ctx context.Context, filename string, data []byte, overwrite bool) (UploadMigrationDBResponse, error) {
	// Validate filename
	if filename == "" {
		return UploadMigrationDBResponse{
			Success: false,
			Error:   "filename is required",
		}, nil
	}

	// Ensure filename ends with .db
	if !strings.HasSuffix(filename, ".db") {
		filename = filename + ".db"
	}

	// Sanitize filename to prevent path traversal
	filename = filepath.Base(filename)
	if filename == "." || filename == ".." {
		return UploadMigrationDBResponse{
			Success: false,
			Error:   "invalid filename",
		}, nil
	}

	// Construct full path
	dbPath := filepath.Join(m.cfg.Runtime.MigrationDBStorageDir, filename)

	// Check if file already exists
	if _, err := os.Stat(dbPath); err == nil {
		if !overwrite {
			return UploadMigrationDBResponse{
				Success: false,
				Error:   "file already present on API",
			}, nil
		}
	}

	// Write file
	if err := os.WriteFile(dbPath, data, 0o644); err != nil {
		m.logger.Error().Err(err).Str("filename", filename).Msg("failed to write migration DB file")
		return UploadMigrationDBResponse{
			Success: false,
			Error:   fmt.Sprintf("failed to save file: %v", err),
		}, nil
	}

	m.logger.Info().
		Str("filename", filename).
		Str("path", dbPath).
		Int("size", len(data)).
		Bool("overwrite", overwrite).
		Msg("uploaded migration DB file")

	return UploadMigrationDBResponse{
		Success: true,
		Path:    dbPath,
	}, nil
}

func (m *Manager) ListMigrationDBs(ctx context.Context) ([]MigrationDBInfo, error) {
	dir := m.cfg.Runtime.MigrationDBStorageDir

	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return []MigrationDBInfo{}, nil
		}
		return []MigrationDBInfo{}, fmt.Errorf("failed to read migration DB storage directory: %w", err)
	}

	// Initialize as empty slice (not nil) to ensure JSON serializes as [] instead of null
	dbs := make([]MigrationDBInfo, 0)
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		// Only list .db files
		if !strings.HasSuffix(entry.Name(), ".db") {
			continue
		}

		info, err := entry.Info()
		if err != nil {
			m.logger.Warn().Err(err).Str("filename", entry.Name()).Msg("failed to get file info")
			continue
		}

		fullPath := filepath.Join(dir, entry.Name())
		dbs = append(dbs, MigrationDBInfo{
			Filename:   entry.Name(),
			Path:       fullPath,
			Size:       info.Size(),
			ModifiedAt: info.ModTime(),
		})
	}

	// Sort by modified time (newest first)
	sort.Slice(dbs, func(i, j int) bool {
		return dbs[i].ModifiedAt.After(dbs[j].ModifiedAt)
	})

	// Ensure we always return a non-nil slice (even if empty)
	if dbs == nil {
		return []MigrationDBInfo{}, nil
	}

	return dbs, nil
}

func (m *Manager) SubscribeProgress(ctx context.Context, id string) (<-chan ProgressEvent, func(), error) {
	m.mu.Lock()
	record, ok := m.migrations[id]
	if !ok {
		m.mu.Unlock()
		return nil, nil, ErrMigrationNotFound
	}

	ch := make(chan ProgressEvent, 16)
	subID := xid.New().String()
	if _, exists := m.subscribers[id]; !exists {
		m.subscribers[id] = make(map[string]chan ProgressEvent)
	}
	m.subscribers[id][subID] = ch
	snapshot := record.toStatus()
	m.mu.Unlock()

	ch <- ProgressEvent{
		Event:     "snapshot",
		Timestamp: time.Now().UTC(),
		Migration: snapshot,
	}

	cancel := func() {
		m.removeSubscriber(id, subID)
	}

	if ctx != nil {
		go func() {
			<-ctx.Done()
			cancel()
		}()
	}

	return ch, cancel, nil
}

func (m *Manager) runMigration(record *migrationRecord, srcDef, dstDef serviceDefinition, srcFolder, dstFolder fsservices.Folder, opts MigrationOptions) {
	m.logger.Info().
		Str("migration_id", record.ID).
		Str("source", srcDef.ID).
		Str("destination", dstDef.ID).
		Msg("starting migration")

	heartbeat := time.NewTicker(5 * time.Second)
	done := make(chan struct{})
	go func() {
		for {
			select {
			case <-heartbeat.C:
				m.publishProgress(record.ID, "running", nil, nil)
			case <-done:
				heartbeat.Stop()
				return
			}
		}
	}()

	result, err := m.executeMigration(record.ID, srcDef, dstDef, srcFolder, dstFolder, opts)
	close(done)

	if err != nil {
		m.mu.Lock()
		record.Status = migrationStatusFailed
		record.Error = err.Error()
		finished := time.Now().UTC()
		record.CompletedAt = &finished
		m.mu.Unlock()

		m.logger.Error().
			Err(err).
			Str("migration_id", record.ID).
			Msg("migration failed")

		m.publishProgress(record.ID, "failed", nil, nil)
		m.closeSubscribers(record.ID)
		return
	}

	finished := time.Now().UTC()

	m.mu.Lock()
	record.Status = migrationStatusCompleted
	record.CompletedAt = &finished
	record.Result = result
	m.mu.Unlock()

	m.logger.Info().
		Str("migration_id", record.ID).
		Msg("migration completed successfully")

	srcStats := result.Runtime.Src
	dstStats := result.Runtime.Dst
	m.publishProgress(record.ID, "completed", &srcStats, &dstStats)
	m.closeSubscribers(record.ID)
}

func (m *Manager) executeMigration(migrationID string, srcDef, dstDef serviceDefinition, srcFolder, dstFolder fsservices.Folder, opts MigrationOptions) (*migration.Result, error) {
	dbPath, err := m.resolveDatabasePath(opts.DatabasePath, migrationID)
	if err != nil {
		return nil, err
	}

	srcAdapter, srcCleanup, err := m.acquireAdapter(srcDef, srcFolder.Id, opts.SourceConnectionID)
	if err != nil {
		return nil, fmt.Errorf("source adapter: %w", err)
	}
	defer srcCleanup()

	dstAdapter, dstCleanup, err := m.acquireAdapter(dstDef, dstFolder.Id, opts.DestinationConnectionID)
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
		SkipListener:    !m.shouldEnableLoggingTerminal(opts),
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

	// Set SeedRoots to true - LetsMigrate will only use it if DB is empty
	cfg.SeedRoots = true

	// LetsMigrate automatically detects and resumes in-progress migrations
	// when provided with an existing database file
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

func (m *Manager) shouldEnableLoggingTerminal(opts MigrationOptions) bool {
	// If explicitly set in options, use that
	if opts.EnableLoggingTerminal {
		return true
	}
	// Otherwise use config default
	return m.cfg.Runtime.EnableLoggingTerminal
}

// findTerminalEmulator finds an available terminal emulator on the system.
// Returns the command name.
func findTerminalEmulator() string {
	// Try common terminal emulators in order of preference
	terminals := []string{
		"gnome-terminal",
		"konsole",
		"xterm",
		"x-terminal-emulator", // Generic fallback on Debian/Ubuntu
	}

	for _, term := range terminals {
		if _, err := exec.LookPath(term); err == nil {
			return term
		}
	}

	return ""
}

// spawnLogTerminal spawns the log terminal process if not already running.
// Returns an error if a terminal is already running on the given address.
func (m *Manager) spawnLogTerminal(logAddress string) error {
	m.logTerminalMu.Lock()
	defer m.logTerminalMu.Unlock()

	if m.logTerminal != nil {
		// Check if process is still running
		if m.logTerminal.ProcessState == nil {
			// Process is still running (hasn't exited yet)
			return fmt.Errorf("log terminal already running on %s", logAddress)
		}
		// Process exited, clear it
		m.logTerminal = nil
	}

	// Find the Migration Engine directory
	// Try relative path from current working directory first (for development)
	migrationEnginePath := "../Migration-Engine"
	if _, err := os.Stat(migrationEnginePath); os.IsNotExist(err) {
		// Fallback: try relative to executable
		execPath, err := os.Executable()
		if err == nil {
			migrationEnginePath = filepath.Join(filepath.Dir(filepath.Dir(filepath.Dir(execPath))), "Migration-Engine")
		}
		// If still not found, try absolute path from go.mod replace directive
		if _, err := os.Stat(migrationEnginePath); os.IsNotExist(err) {
			return fmt.Errorf("Migration-Engine directory not found (tried %s)", migrationEnginePath)
		}
	}

	spawnPath := filepath.Join(migrationEnginePath, "pkg", "logservice", "main", "spawn.go")
	if _, err := os.Stat(spawnPath); os.IsNotExist(err) {
		return fmt.Errorf("log service spawn.go not found at %s", spawnPath)
	}

	// Find an available terminal emulator
	terminalCmd := findTerminalEmulator()
	if terminalCmd == "" {
		return fmt.Errorf("no terminal emulator found (tried: xterm, gnome-terminal, konsole, x-terminal-emulator)")
	}

	// Build the command to run in the new terminal
	goRunCmd := fmt.Sprintf("go run %s %s", spawnPath, logAddress)

	// Prepare terminal command with arguments
	var cmd *exec.Cmd
	switch terminalCmd {
	case "gnome-terminal":
		cmd = exec.Command(terminalCmd, "--title=Log Terminal", "--", "sh", "-c", goRunCmd)
	case "konsole":
		cmd = exec.Command(terminalCmd, "-e", "sh", "-c", goRunCmd)
	case "xterm":
		cmd = exec.Command(terminalCmd, "-T", "Log Terminal", "-e", "sh", "-c", goRunCmd)
	default:
		// Generic x-terminal-emulator
		cmd = exec.Command(terminalCmd, "-e", "sh", "-c", goRunCmd)
	}

	// Don't attach stdout/stderr - let the terminal handle it
	cmd.Stdin = nil
	cmd.Stdout = nil
	cmd.Stderr = nil

	if err := cmd.Start(); err != nil {
		return fmt.Errorf("failed to start log terminal: %w", err)
	}

	m.logTerminal = cmd

	// Monitor process in background
	go func() {
		if err := cmd.Wait(); err != nil {
			m.logger.Warn().
				Err(err).
				Str("address", logAddress).
				Msg("log terminal process exited")
		}
		m.logTerminalMu.Lock()
		if m.logTerminal == cmd {
			m.logTerminal = nil
		}
		m.logTerminalMu.Unlock()
	}()

	m.logger.Info().
		Str("address", logAddress).
		Int("pid", cmd.Process.Pid).
		Msg("spawned log terminal")

	return nil
}

// stopLogTerminal stops the currently running log terminal process if any.
func (m *Manager) stopLogTerminal() error {
	m.logTerminalMu.Lock()
	defer m.logTerminalMu.Unlock()

	if m.logTerminal == nil {
		return nil
	}

	if m.logTerminal.ProcessState != nil && m.logTerminal.ProcessState.Exited() {
		m.logTerminal = nil
		return nil
	}

	if err := m.logTerminal.Process.Kill(); err != nil {
		return fmt.Errorf("failed to kill log terminal: %w", err)
	}

	m.logger.Info().
		Int("pid", m.logTerminal.Process.Pid).
		Msg("stopped log terminal")

	m.logTerminal = nil
	return nil
}

// ToggleLogTerminal toggles the log terminal on or off.
func (m *Manager) ToggleLogTerminal(ctx context.Context, enable bool, logAddress string) error {
	if enable {
		if logAddress == "" {
			logAddress = m.cfg.Runtime.LogAddress
		}
		if logAddress == "" {
			return fmt.Errorf("log address is required")
		}
		return m.spawnLogTerminal(logAddress)
	}
	return m.stopLogTerminal()
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

func (m *Manager) acquireAdapter(def serviceDefinition, rootID, connectionID string) (fsservices.FSAdapter, func(), error) {
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
		return m.acquireSpectraAdapter(def, rootID, connectionID)
	default:
		return nil, nil, fmt.Errorf("unsupported service type: %s", def.Type)
	}
}

func (m *Manager) acquireSpectraAdapter(def serviceDefinition, rootID, connectionID string) (fsservices.FSAdapter, func(), error) {
	root := def.Spectra.RootID
	if rootID != "" {
		root = rootID
	}

	if connectionID == "" {
		spectraFS, err := sdk.New(def.Spectra.ConfigPath)
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
			m.releaseConnection(connectionID)
			return nil, nil, err
		}

		return adapter, func() {
			m.releaseConnection(connectionID)
		}, nil
	}
	m.mu.Unlock()

	spectraFS, err := sdk.New(def.Spectra.ConfigPath)
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
			m.releaseConnection(connectionID)
			return nil, nil, err
		}
		return adapter, func() {
			m.releaseConnection(connectionID)
		}, nil
	}

	m.connections[connectionID] = &serviceConnection{
		typ:      ServiceTypeSpectra,
		spectra:  spectraFS,
		refCount: 1,
	}
	m.mu.Unlock()

	return adapter, func() {
		m.releaseConnection(connectionID)
	}, nil
}

func (m *Manager) releaseConnection(connectionID string) {
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

// findSpectraServiceByWorld finds the first Spectra service with the given world.
func (m *Manager) findSpectraServiceByWorld(world string) (serviceDefinition, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	for _, svc := range m.services {
		if svc.Type == ServiceTypeSpectra && svc.Spectra != nil && svc.Spectra.World == world {
			return svc, nil
		}
	}
	return serviceDefinition{}, ErrServiceNotFound
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

func queueStatsSnapshotFrom(stats queue.QueueStats) QueueStatsSnapshot {
	return QueueStatsSnapshot{
		Round:        stats.Round,
		Pending:      stats.Pending,
		InProgress:   stats.InProgress,
		TotalTracked: stats.TotalTracked,
		Workers:      stats.Workers,
	}
}

func (m *Manager) publishProgress(id, event string, srcStats, dstStats *queue.QueueStats) {
	m.mu.RLock()
	record, ok := m.migrations[id]
	if !ok {
		m.mu.RUnlock()
		return
	}

	status := record.toStatus()
	subscribers := m.subscribers[id]
	channels := make([]chan ProgressEvent, 0, len(subscribers))
	for _, ch := range subscribers {
		channels = append(channels, ch)
	}
	m.mu.RUnlock()

	if len(channels) == 0 {
		return
	}

	update := ProgressEvent{
		Event:     event,
		Timestamp: time.Now().UTC(),
		Migration: status,
	}
	if srcStats != nil {
		update.Source = queueStatsSnapshotFrom(*srcStats)
	}
	if dstStats != nil {
		update.Destination = queueStatsSnapshotFrom(*dstStats)
	}

	for _, ch := range channels {
		select {
		case ch <- update:
		default:
		}
	}
}

func (m *Manager) removeSubscriber(migrationID, subscriberID string) {
	m.mu.Lock()
	subs, ok := m.subscribers[migrationID]
	if !ok {
		m.mu.Unlock()
		return
	}

	ch, ok := subs[subscriberID]
	if ok {
		delete(subs, subscriberID)
	}
	if len(subs) == 0 {
		delete(m.subscribers, migrationID)
	}
	m.mu.Unlock()

	if ok {
		close(ch)
	}
}

func (m *Manager) closeSubscribers(migrationID string) {
	m.mu.Lock()
	subs := m.subscribers[migrationID]
	delete(m.subscribers, migrationID)
	m.mu.Unlock()

	for _, ch := range subs {
		close(ch)
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
