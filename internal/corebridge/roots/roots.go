package roots

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"

	"github.com/Project-Sylos/Migration-Engine/pkg/db"
	"github.com/Project-Sylos/Migration-Engine/pkg/migration"
	"github.com/Project-Sylos/Sylos-API/internal/corebridge/services"
	fstypes "github.com/Project-Sylos/Sylos-FS/pkg/types"
	"github.com/rs/xid"
	"github.com/rs/zerolog"
)

// FolderDescriptor represents a folder descriptor
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

// SetRootRequest represents a request to set a root
type SetRootRequest struct {
	MigrationID  string
	Role         string
	ServiceID    string
	ConnectionID string
	Root         FolderDescriptor
}

// SetRootResponse represents a response from setting a root
type SetRootResponse struct {
	MigrationID             string
	Role                    string
	Ready                   bool
	DatabasePath            string
	RootSummary             *migration.RootSeedSummary
	SourceConnectionID      string
	DestinationConnectionID string
}

// Manager handles root-related operations
type Manager struct {
	logger        zerolog.Logger
	dataDir       string
	plans         map[string]*RootPlan
	mu            sync.RWMutex
	serviceMgr    *services.ServiceManager
	resolveDBPath func(path, migrationID string) (string, error)
}

type RootPlan struct {
	HasSource               bool
	SourceDefinition        services.ServiceDefinition
	SourceRoot              fstypes.Folder
	SourceConnectionID      string
	HasDestination          bool
	DestinationDefinition   services.ServiceDefinition
	DestinationRoot         fstypes.Folder
	DestinationConnectionID string
	DatabasePath            string
	RootSummary             migration.RootSeedSummary
	Seeded                  bool
	Seeding                 bool
}

func NewManager(logger zerolog.Logger, dataDir string, serviceMgr *services.ServiceManager, resolveDBPath func(path, migrationID string) (string, error)) *Manager {
	return &Manager{
		logger:        logger,
		dataDir:       dataDir,
		plans:         make(map[string]*RootPlan),
		serviceMgr:    serviceMgr,
		resolveDBPath: resolveDBPath,
	}
}

func FolderFromDescriptor(desc FolderDescriptor) (fstypes.Folder, error) {
	if desc.ID == "" {
		return fstypes.Folder{}, fmt.Errorf("folder id cannot be empty")
	}

	folder := fstypes.Folder{
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
		folder.DisplayName = folder.ID()
	}
	if folder.LocationPath == "" {
		folder.LocationPath = "/"
	}
	if folder.Type == "" {
		folder.Type = fstypes.NodeTypeFolder
	}
	// LastUpdated will be set by caller if needed

	return folder, nil
}

func (m *Manager) SetRoot(ctx context.Context, req SetRootRequest) (SetRootResponse, error) {
	role := strings.ToLower(strings.TrimSpace(req.Role))
	if role != "source" && role != "destination" {
		return SetRootResponse{}, fmt.Errorf("role must be 'source' or 'destination'")
	}

	if req.ServiceID == "" {
		return SetRootResponse{}, fmt.Errorf("serviceId is required")
	}

	// Map "spectra" virtual service to the appropriate world based on role
	serviceID := req.ServiceID
	var serviceDef services.ServiceDefinition
	var err error

	if serviceID == "spectra" {
		var world string
		switch role {
		case "source":
			world = "primary"
		case "destination":
			world = "s1"
		default:
			return SetRootResponse{}, fmt.Errorf("role must be 'source' or 'destination' when using 'spectra' service")
		}

		serviceDef, err = m.serviceMgr.GetServiceDefinitionByWorld(world)
		if err != nil {
			return SetRootResponse{}, fmt.Errorf("spectra service with world %s not found: %w", world, err)
		}
	} else {
		serviceDef, err = m.serviceMgr.GetServiceDefinition(serviceID)
		if err != nil {
			return SetRootResponse{}, err
		}
	}

	folder, err := FolderFromDescriptor(req.Root)
	if err != nil {
		return SetRootResponse{}, fmt.Errorf("invalid %s root: %w", role, err)
	}

	migrationID := req.MigrationID
	if migrationID == "" {
		migrationID = xid.New().String()
	}

	m.mu.Lock()
	plan := m.plans[migrationID]
	if plan == nil {
		plan = &RootPlan{}
		m.plans[migrationID] = plan
	}

	plan.Seeded = false
	plan.Seeding = false
	plan.DatabasePath = ""
	plan.RootSummary = migration.RootSeedSummary{}

	if role == "source" {
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
		if _, _, _, err := m.SeedPlanIfReady(migrationID); err != nil {
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

	// Migration Engine will update its YAML config when roots are set
	// No need to update our minimal metadata here

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

func (m *Manager) SeedPlanIfReady(migrationID string) (bool, migration.RootSeedSummary, string, error) {
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
		dbPath, err = m.resolveDBPath("", migrationID)
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
		options := db.Options{
			Path: dbPath,
		}
		database, err := db.Open(options)
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
						plan.SourceRoot.ID() != sourceRoot.ID() ||
						!plan.HasDestination ||
						plan.DestinationDefinition.ID != destDef.ID ||
						plan.DestinationRoot.ID() != destRoot.ID() {
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
	database, wasFresh, err := migration.SetupDatabase(migration.DatabaseConfig{
		Path:           dbPath,
		RemoveExisting: false, // Always false - anti-pattern to remove existing DB
	})
	if err != nil {
		m.mu.Lock()
		if plan, ok := m.plans[migrationID]; ok {
			plan.Seeding = false
		}
		m.mu.Unlock()
		return wasFresh, migration.RootSeedSummary{}, "", err
	}

	summary, seedErr := migration.SeedRootTasks(sourceRoot, destRoot, database)
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
		plan.SourceRoot.ID() != sourceRoot.ID() ||
		!plan.HasDestination ||
		plan.DestinationDefinition.ID != destDef.ID ||
		plan.DestinationRoot.ID() != destRoot.ID() {
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

func (m *Manager) GetPlan(migrationID string) *RootPlan {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.plans[migrationID]
}
