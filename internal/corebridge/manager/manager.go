package manager

import (
	"os"
	"path/filepath"
	"sync"
	"time"

	"codeberg.org/Sylos/Migration-Engine/pkg/migration"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge/database"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge/roots"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge/services"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge/terminal"
	"codeberg.org/Sylos/Sylos-API/pkg/config"
	"github.com/rs/zerolog"
)

// Manager implements corebridge.Bridge.
type Manager struct {
	logger      zerolog.Logger
	cfg         config.Config
	serviceMgr  *services.ServiceManager
	rootsMgr    *roots.Manager
	engineMgr   *migration.MigrationManager
	terminalMgr *terminal.Manager
	bgTaskMgr   *corebridge.BackgroundTaskManager

	mu              sync.RWMutex
	runtimeByID     map[string]*runtimeMigration
	progressByID    map[string]map[string]chan corebridge.ProgressEvent
	progressCounter uint64
}

type runtimeMigration struct {
	Migration     *migration.Migration
	SourceID      string
	DestinationID string
	StartedAt     time.Time
	CompletedAt   *time.Time
	Status        string
	Error         string
}

// NewManager creates a new Manager implementing corebridge.Bridge.
func NewManager(logger zerolog.Logger, cfg config.Config) (*Manager, error) {
	serviceMgr := services.NewServiceManager()
	if err := serviceMgr.LoadServices(cfg); err != nil {
		return nil, err
	}

	resolveDBPath := func(path, migrationID string) (string, error) {
		return database.ResolveDatabasePath(cfg.Runtime.DataDir, path, migrationID)
	}

	rootsMgr := roots.NewManager(logger, cfg.Runtime.DataDir, serviceMgr, resolveDBPath)
	engineDBPath := filepath.Join(cfg.Runtime.DataDir, "engine_migrations.duckdb")
	if err := os.MkdirAll(filepath.Dir(engineDBPath), 0o755); err != nil {
		return nil, err
	}
	engineMgr, err := migration.NewMigrationManager(migration.DatabaseConfig{Path: engineDBPath})
	if err != nil {
		return nil, err
	}
	terminalMgr := terminal.NewManager(logger, cfg)
	bgTaskMgr := corebridge.NewBackgroundTaskManager(logger)

	mgr := &Manager{
		logger:       logger,
		cfg:          cfg,
		serviceMgr:   serviceMgr,
		rootsMgr:     rootsMgr,
		engineMgr:    engineMgr,
		terminalMgr:  terminalMgr,
		bgTaskMgr:    bgTaskMgr,
		runtimeByID:  make(map[string]*runtimeMigration),
		progressByID: make(map[string]map[string]chan corebridge.ProgressEvent),
	}

	return mgr, nil
}

// Ensure Manager implements corebridge.Bridge at compile time.
var _ corebridge.Bridge = (*Manager)(nil)
