package manager

import (
	"context"
	"path/filepath"
	"sync"
	"time"

	"codeberg.org/Sylos/Migration-Engine/pkg/migration"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge/database"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge/metadata"
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
	engineMgr, err := migration.NewMigrationManager(migration.DatabaseConfig{})
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

// migrationDirFor returns the absolute path to the folder for the given migration (e.g. dataDir/{id}).
func (m *Manager) migrationDirFor(migrationID string) (string, error) {
	dir := database.GetMigrationDir(m.cfg.Runtime.DataDir, migrationID)
	return filepath.Abs(dir)
}

// GetMigration returns the engine *Migration for the given ID, or ErrMigrationNotFound.
// When using per-migration DBs, the engine expects the migration folder path (e.g. data/{id}) so it can open or create the DB there.
func (m *Manager) GetMigration(_ context.Context, migrationID string) (*migration.Migration, error) {
	migrationDir, err := m.migrationDirFor(migrationID)
	if err != nil {
		return nil, err
	}
	mig, err := m.engineMgr.GetMigration(migrationID, migrationDir)
	if err != nil {
		return nil, err
	}
	if mig == nil {
		return nil, corebridge.ErrMigrationNotFound
	}
	return mig, nil
}

// MarkPathReviewChanges updates metadata after exclusion or retry mark/unmark.
func (m *Manager) MarkPathReviewChanges(_ context.Context, migrationID string, hasChanges bool) error {
	metaMgr := metadata.NewManager(m.cfg.Runtime.DataDir)
	meta, err := metaMgr.GetMigrationMetadata(migrationID)
	if err != nil {
		meta = metadata.MigrationMetadata{
			ID:                   migrationID,
			Name:                 migrationID,
			HasPathReviewChanges: hasChanges,
		}
	} else {
		meta.HasPathReviewChanges = hasChanges
	}
	return metaMgr.UpdateMigrationMetadata(meta)
}

// Ensure Manager implements corebridge.Bridge at compile time.
var _ corebridge.Bridge = (*Manager)(nil)
