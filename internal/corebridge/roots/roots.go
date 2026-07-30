package roots

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"math/rand"
	"time"

	"codeberg.org/Sylos/Migration-Engine/pkg/migration"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge/services"
	fstypes "codeberg.org/Sylos/Sylos-FS/pkg/types"
	"github.com/oklog/ulid/v2"
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
	Config       map[string]any `json:"config,omitempty"` // Optional service-specific config (e.g., Spectra config JSON)
	// Children are immediate children of Root from the root-pick review step (optional).
	Children []RootChildPlan
	// ExcludedIDs marks source children to exclude; ids not in Children are ignored.
	ExcludedIDs []string
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
	SourceRootPrepared      bool
	DestinationRootPrepared bool
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
	HasSource                 bool
	SourceDefinition          services.ServiceDefinition
	SourceRoot                fstypes.Folder
	SourceConnectionID        string // For Spectra: sessionID returned by RegisterSpectraSession
	SourceAdapter             fstypes.FSAdapter
	SourceAdapterRelease      func()
	HasDestination            bool
	DestinationDefinition     services.ServiceDefinition
	DestinationRoot           fstypes.Folder
	DestinationConnectionID   string // For Spectra: sessionID returned by RegisterSpectraSession
	DestinationAdapter        fstypes.FSAdapter
	DestinationAdapterRelease func()
	DatabasePath              string
	RootSummary               migration.RootSeedSummary
	Seeded                    bool
	Seeding                   bool
	// PathCheckTarget selects destination-name rules: "none", "auto", or a provider id (e.g. "windows").
	PathCheckTarget string
	// WindowsCompat enables Windows desktop-sync overlays on soft cloud destinations.
	WindowsCompat bool

	SourceRootPrepared      bool
	DestinationRootPrepared bool
	SourceChildren          []RootChildPlan
	DestinationChildren     []RootChildPlan
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
		ServiceID:    desc.ID,
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

// ValidateMigrationRoot applies Sylos-FS provider policy before any migration
// state is created or mutated.
func (m *Manager) ValidateMigrationRoot(serviceID string, desc FolderDescriptor) error {
	folder, err := FolderFromDescriptor(desc)
	if err != nil {
		return err
	}
	return m.serviceMgr.ValidateMigrationRoot(serviceID, folder)
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
	// Use the resolved concrete service id (spectra-primary / spectra-s1), not
	// the virtual catalog id "spectra".
	if err := m.serviceMgr.ValidateMigrationRoot(serviceDef.ID, folder); err != nil {
		return SetRootResponse{}, err
	}

	var reviewedChildren []RootChildPlan
	hasChildPayload := len(req.Children) > 0 || len(req.ExcludedIDs) > 0
	if hasChildPayload {
		allowExclude := role == "source"
		if role == "destination" && len(req.ExcludedIDs) > 0 {
			return SetRootResponse{}, fmt.Errorf("destination children cannot set excluded")
		}
		normalized, err := NormalizeRootChildren(req.Children, req.ExcludedIDs, allowExclude)
		if err != nil {
			return SetRootResponse{}, err
		}
		if role == "source" && len(normalized) == 0 {
			return SetRootResponse{}, fmt.Errorf("source root review requires at least one included child")
		}
		reviewedChildren = normalized
	}

	// Spectra needs a registered session before ListChildren works, and that
	// session is created later in this flow. Synthetic Spectra roots are never
	// "empty" in the filesystem sense, so skip the pre-check.
	if role == "source" && serviceDef.Type != services.ServiceTypeSpectra {
		if err := m.validateSourceRootNotEmpty(ctx, serviceID, folder, req.ConnectionID); err != nil {
			return SetRootResponse{}, err
		}
	}

	migrationID := req.MigrationID
	if migrationID == "" {
		// Generate ULID for migration run ID (lexicographically sortable); node IDs use engine's DeterministicNodeID
		entropy := rand.New(rand.NewSource(time.Now().UnixNano()))
		migrationID = ulid.MustNew(ulid.Timestamp(time.Now()), entropy).String()
	}

	m.mu.Lock()
	if m.plans[migrationID] == nil {
		m.plans[migrationID] = &RootPlan{}
	}
	m.mu.Unlock()

	// For Spectra services, use Sylos-FS's session-based pattern:
	// 1. API registers a session with ServiceManager using RegisterSpectraSession(configPath, connectionID)
	// 2. ServiceManager creates the session internally and owns it
	// 3. API receives a sessionID and uses it to acquire adapters
	// This ensures both source and destination adapters share the same *sdk.SpectraFS instance
	connectionID := req.ConnectionID
	var sessionID string
	switch serviceDef.Type {
	case services.ServiceTypeSpectra:
		m.mu.RLock()
		plan := m.plans[migrationID]
		existingSessionID := ""
		if plan != nil {
			existingSessionID = plan.SourceConnectionID
		}
		m.mu.RUnlock()

		if existingSessionID == "" {
			// No session registered yet - register one now
			// Create or reuse override config with absolute DB path to avoid path resolution issues
			var overridePath string
			var err error

			// Debug: Check what we received
			fmt.Printf("DEBUG: req.Config is nil: %v, len: %d\n", req.Config == nil, len(req.Config))
			if len(req.Config) > 0 {
				fmt.Printf("DEBUG: req.Config keys: %v\n", getMapKeys(req.Config))
			}

			// If config was provided in the request, always use it (even if override exists)
			if len(req.Config) > 0 {
				fmt.Printf("Using config from request for migration %s (config keys: %v)\n", migrationID, getMapKeys(req.Config))
				// Save config from request data
				overridePath, err = services.SaveSpectraConfigFromData(m.dataDir, migrationID, req.Config)
				if err != nil {
					return SetRootResponse{}, fmt.Errorf("failed to save Spectra config from request: %w", err)
				}
				fmt.Printf("Saved config from request to: %s\n", overridePath)
			} else {
				fmt.Printf("No config provided in request for migration %s, checking for existing override\n", migrationID)
				// Check if override config already exists
				var exists bool
				overridePath, exists, err = services.LoadSpectraConfigOverride(m.dataDir, migrationID)
				if err != nil {
					return SetRootResponse{}, fmt.Errorf("failed to check for existing Spectra config override: %w", err)
				}
				if !exists {
					// Create new override config from original config path
					overridePath, err = services.SaveSpectraConfigOverride(m.dataDir, migrationID, serviceDef.Spectra.ConfigPath)
					if err != nil {
						return SetRootResponse{}, fmt.Errorf("failed to create Spectra config override: %w", err)
					}
				}
			}

			// Determine connectionID for session registration
			if connectionID == "" {
				connectionID = fmt.Sprintf("spectra-%s", migrationID)
			}

			// Register session with ServiceManager (ServiceManager creates it internally)
			fmt.Printf("Registering Spectra session for migration %s with config: %s, connectionID: %s\n", migrationID, overridePath, connectionID)
			sessionID, err = m.serviceMgr.FS.RegisterSpectraSession(overridePath, connectionID)
			if err != nil {
				return SetRootResponse{}, fmt.Errorf("failed to register Spectra session: %w", err)
			}

			// Store sessionID in plan for reuse
			m.mu.Lock()
			plan = m.plans[migrationID] // Re-fetch in case it was modified
			if plan == nil {
				m.mu.Unlock()
				return SetRootResponse{}, fmt.Errorf("migration plan was deleted during session registration")
			}
			plan.SourceConnectionID = sessionID // Store sessionID for reuse
			m.mu.Unlock()

			fmt.Printf("Registered Spectra session successfully - sessionID: %s\n", sessionID)
		} else {
			// Reuse existing Spectra session - reuse the existing sessionID
			sessionID = existingSessionID
			fmt.Printf("Reusing existing Spectra session - sessionID: %s\n", sessionID)
		}
	case services.ServiceTypeCloud:
		if connectionID == "" {
			return SetRootResponse{}, fmt.Errorf("connectionId is required for cloud services")
		}
		sessionID = connectionID
	default:
		// For non-Spectra services, use connectionID as-is
		sessionID = connectionID
	}

	// Acquire before releasing any previous adapter for this role. Releasing first
	// drops the last ref on cloud/Spectra sessions and deletes them, so editing a
	// root with the same connectionId fails with "connection not found".
	adapter, release, err := m.serviceMgr.FS.AcquireAdapter(serviceDef, folder, sessionID)
	if err != nil {
		return SetRootResponse{}, fmt.Errorf("failed to acquire %s adapter: %w", role, err)
	}

	fmt.Printf("Acquired adapter - role: %s, sessionID: %s\n", role, sessionID)

	m.mu.Lock()
	plan := m.plans[migrationID]
	if plan == nil {
		m.mu.Unlock()
		release()
		return SetRootResponse{}, fmt.Errorf("migration plan was deleted during adapter acquisition")
	}

	var oldRelease func()
	if role == "source" {
		oldRelease = plan.SourceAdapterRelease
		plan.HasSource = true
		plan.SourceDefinition = serviceDef
		plan.SourceRoot = folder
		plan.SourceConnectionID = sessionID
		plan.SourceAdapter = adapter
		plan.SourceAdapterRelease = release
		plan.SourceChildren = reviewedChildren
		plan.SourceRootPrepared = len(reviewedChildren) > 0
	} else {
		oldRelease = plan.DestinationAdapterRelease
		plan.HasDestination = true
		plan.DestinationDefinition = serviceDef
		plan.DestinationRoot = folder
		plan.DestinationConnectionID = sessionID
		plan.DestinationAdapter = adapter
		plan.DestinationAdapterRelease = release
		plan.DestinationChildren = reviewedChildren
		plan.DestinationRootPrepared = len(reviewedChildren) > 0
	}

	plan.Seeded = false
	plan.Seeding = false
	plan.DatabasePath = ""
	plan.RootSummary = migration.RootSeedSummary{}
	planReady := plan.HasSource && plan.HasDestination
	_ = m.savePreparationLocked(migrationID, plan)
	m.mu.Unlock()

	if oldRelease != nil {
		oldRelease()
	}

	if planReady {
		m.mu.Lock()
		if latestPlan := m.plans[migrationID]; latestPlan != nil {
			latestPlan.Seeded = true
		}
		m.mu.Unlock()
	}

	m.mu.RLock()
	plan = m.plans[migrationID]
	var (
		ready      bool
		dbPath     string
		summary    *migration.RootSeedSummary
		sourceConn string
		destConn   string
		srcPrep    bool
		dstPrep    bool
	)
	if plan != nil {
		ready = plan.HasSource && plan.HasDestination
		dbPath = plan.DatabasePath
		sourceConn = plan.SourceConnectionID
		destConn = plan.DestinationConnectionID
		srcPrep = plan.SourceRootPrepared
		dstPrep = plan.DestinationRootPrepared
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
		SourceRootPrepared:      srcPrep,
		DestinationRootPrepared: dstPrep,
	}, nil
}

func (m *Manager) SeedPlanIfReady(migrationID string) (bool, migration.RootSeedSummary, string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	plan, ok := m.plans[migrationID]
	if !ok {
		return false, migration.RootSeedSummary{}, "", nil
	}
	ready := plan.HasSource && plan.HasDestination
	return ready, migration.RootSeedSummary{}, "", nil
}

func (m *Manager) GetPlan(migrationID string) *RootPlan {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.plans[migrationID]
}

// SetPathCheckTarget stores the user-selected destination-name check profile on the plan.
func (m *Manager) SetPathCheckTarget(migrationID, target string) {
	if migrationID == "" {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	plan := m.plans[migrationID]
	if plan == nil {
		plan = &RootPlan{}
		m.plans[migrationID] = plan
	}
	plan.PathCheckTarget = strings.TrimSpace(target)
}

// SetWindowsCompat stores the Windows desktop-sync overlay opt-in on the plan.
func (m *Manager) SetWindowsCompat(migrationID string, enabled bool) {
	if migrationID == "" {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	plan := m.plans[migrationID]
	if plan == nil {
		plan = &RootPlan{}
		m.plans[migrationID] = plan
	}
	plan.WindowsCompat = enabled
}

// ApplyRehydratedSide attaches an adapter for one role after loading FS credential state from the migration DB (e.g. API restart).
// It replaces any existing adapter for that role, calling the previous release callback first.
func (m *Manager) ApplyRehydratedSide(migrationID, role string, def services.ServiceDefinition, folder fstypes.Folder, connectionID string, adapter fstypes.FSAdapter, release func()) error {
	role = strings.ToLower(strings.TrimSpace(role))
	if role != "source" && role != "destination" {
		return fmt.Errorf("role must be 'source' or 'destination'")
	}
	var oldRelease func()
	m.mu.Lock()
	plan := m.plans[migrationID]
	if plan == nil {
		plan = &RootPlan{}
		m.plans[migrationID] = plan
	}
	if role == "source" {
		oldRelease = plan.SourceAdapterRelease
		plan.SourceAdapterRelease = nil
		plan.SourceAdapter = nil
	} else {
		oldRelease = plan.DestinationAdapterRelease
		plan.DestinationAdapterRelease = nil
		plan.DestinationAdapter = nil
	}
	m.mu.Unlock()
	if oldRelease != nil {
		oldRelease()
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	plan = m.plans[migrationID]
	if plan == nil {
		release()
		return fmt.Errorf("migration plan was removed during rehydration")
	}
	if role == "source" {
		plan.HasSource = true
		plan.SourceDefinition = def
		plan.SourceRoot = folder
		plan.SourceConnectionID = connectionID
		plan.SourceAdapter = adapter
		plan.SourceAdapterRelease = release
	} else {
		plan.HasDestination = true
		plan.DestinationDefinition = def
		plan.DestinationRoot = folder
		plan.DestinationConnectionID = connectionID
		plan.DestinationAdapter = adapter
		plan.DestinationAdapterRelease = release
	}
	return nil
}

// ClearAdapters clears adapter references from a RootPlan after migration completes
// For Spectra: ServiceManager automatically closes sessions when all adapters are released
// The API doesn't need to (and shouldn't) manually close sessions - ServiceManager handles that
func (m *Manager) ClearAdapters(migrationID string) {
	m.mu.Lock()
	plan := m.plans[migrationID]
	var srcRelease, dstRelease func()
	if plan != nil {
		// Extract release functions before clearing
		srcRelease = plan.SourceAdapterRelease
		dstRelease = plan.DestinationAdapterRelease
		// Clear references
		plan.SourceAdapter = nil
		plan.SourceAdapterRelease = nil
		plan.DestinationAdapter = nil
		plan.DestinationAdapterRelease = nil
	}
	m.mu.Unlock()

	// Call release functions outside of lock (they may perform I/O)
	// These decrement reference counts and ServiceManager will close sessions when count reaches zero
	if srcRelease != nil {
		srcRelease()
	}
	if dstRelease != nil {
		dstRelease()
	}
}

// ClearAllPlans releases adapters and removes all in-memory root plans.
func (m *Manager) ClearAllPlans() {
	m.mu.Lock()
	plans := m.plans
	m.plans = make(map[string]*RootPlan)
	m.mu.Unlock()

	for migrationID, plan := range plans {
		if plan == nil {
			continue
		}
		if plan.SourceAdapterRelease != nil {
			plan.SourceAdapterRelease()
		}
		if plan.DestinationAdapterRelease != nil {
			plan.DestinationAdapterRelease()
		}
		_ = migrationID
	}
}

func (m *Manager) validateSourceRootNotEmpty(ctx context.Context, serviceID string, folder fstypes.Folder, connectionID string) error {
	rootType, driveID := cloudRootListMeta(folder)
	count, err := m.serviceMgr.CountChildren(ctx, services.ListChildrenRequest{
		ServiceID:    serviceID,
		Identifier:   folder.ServiceID,
		Role:         "source",
		ConnectionID: connectionID,
		RootType:     rootType,
		DriveID:      driveID,
	})
	if err != nil {
		return fmt.Errorf("failed to validate source root: %w", err)
	}
	if count == 0 {
		return fmt.Errorf("source root cannot be empty — there is nothing to migrate")
	}
	return nil
}

// cloudRootListMeta returns rootType/driveId for virtual cloud roots so ListChildren
// uses the provider's virtual-root query (e.g. sharedWithMe=true) instead of treating
// the sentinel id as a normal folder.
func cloudRootListMeta(folder fstypes.Folder) (rootType, driveID string) {
	switch folder.Type {
	case "shared_with_me", "my_drive", "shared_drive", "user_root", "team_space", "team_folder", "shared_folder":
		return folder.Type, folder.ParentId
	default:
		return "", ""
	}
}

// getMapKeys returns the keys of a map for debugging purposes
func getMapKeys(m map[string]any) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}
