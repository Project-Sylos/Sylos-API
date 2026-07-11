package manager

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"time"

	"codeberg.org/Sylos/Migration-Engine/pkg/migration"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge/apidb"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge/connections"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge/database"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge/migrationaccess"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge/roots"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge/services"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge/terminal"
	"codeberg.org/Sylos/Sylos-API/pkg/config"
	"codeberg.org/Sylos/Sylos-API/pkg/oauthcreds"
	"github.com/rs/zerolog"
)

// Manager implements corebridge.Bridge.
type Manager struct {
	logger      zerolog.Logger
	cfg         config.Config
	serviceMgr  *services.ServiceManager
	connMgr     *connections.Manager
	rootsMgr    *roots.Manager
	engineMgr   *migration.MigrationManager
	*terminal.Manager
	bgTaskMgr   *corebridge.BackgroundTaskManager
	apiDB       *apidb.DB
	migAccess   *migrationaccess.Opener

	mu              sync.RWMutex
	runtimeByID     map[string]*runtimeMigration
	progressByID    map[string]map[string]chan corebridge.ProgressEvent
	progressCounter uint64
	oauthCreds      oauthcreds.Config

	oauthHealthReschedule   func()
	oauthHealthRescheduleMu sync.Mutex
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
func NewManager(logger zerolog.Logger, cfg config.Config, apiDB *apidb.DB) (*Manager, error) {
	serviceMgr := services.NewServiceManager()
	if err := serviceMgr.LoadServices(cfg); err != nil {
		return nil, err
	}

	resolveDBPath := func(path, migrationID string) (string, error) {
		return database.ResolveDatabasePath(cfg.Runtime.DataDir, path, migrationID)
	}

	rootsMgr := roots.NewManager(logger, cfg.Runtime.DataDir, serviceMgr, resolveDBPath)
	engineMgr := migration.NewMigrationManager()
	terminalMgr := terminal.NewManager(logger, cfg)
	bgTaskMgr := corebridge.NewBackgroundTaskManager(logger)

	mgr := &Manager{
		logger:       logger,
		cfg:          cfg,
		serviceMgr:   serviceMgr,
		connMgr:      connections.NewManager(),
		rootsMgr:     rootsMgr,
		engineMgr:    engineMgr,
		Manager:      terminalMgr,
		bgTaskMgr:    bgTaskMgr,
		apiDB:        apiDB,
		runtimeByID:  make(map[string]*runtimeMigration),
		progressByID: make(map[string]map[string]chan corebridge.ProgressEvent),
	}
	if apiDB != nil {
		mgr.migAccess = &migrationaccess.Opener{
			APIDB:   apiDB,
			Engine:  engineMgr,
			DataDir: cfg.Runtime.DataDir,
		}
	}

	return mgr, nil
}

func (m *Manager) SetOAuthCreds(creds oauthcreds.Config) {
	m.oauthCreds = creds
}

func (m *Manager) oauthClientID(providerID string) string {
	switch providerID {
	case "google_drive":
		if m.oauthCreds.GoogleDrive != nil {
			return m.oauthCreds.GoogleDrive.ClientID
		}
	case "dropbox":
		if m.oauthCreds.Dropbox != nil {
			return m.oauthCreds.Dropbox.ClientID
		}
	}
	return ""
}

func (m *Manager) oauthProviderCredentials(providerID string) (oauthcreds.ProviderCredentials, error) {
	if m.apiDB != nil {
		app, err := m.apiDB.GetProviderOAuthApp(providerID)
		if err == nil && app.ClientID != "" {
			return oauthcreds.ProviderCredentials{
				ClientID:     app.ClientID,
				ClientSecret: app.ClientSecret,
			}, nil
		}
	}
	switch providerID {
	case "google_drive":
		if m.oauthCreds.GoogleDrive == nil {
			return oauthcreds.ProviderCredentials{}, fmt.Errorf("google oauth not configured: add credentials in Settings → Cloud providers")
		}
		return *m.oauthCreds.GoogleDrive, nil
	case "dropbox":
		if m.oauthCreds.Dropbox == nil {
			return oauthcreds.ProviderCredentials{}, fmt.Errorf("dropbox oauth not configured: add credentials in Settings → Cloud providers")
		}
		return *m.oauthCreds.Dropbox, nil
	default:
		return oauthcreds.ProviderCredentials{}, fmt.Errorf("unsupported provider %q", providerID)
	}
}

// migrationDirFor returns the absolute path to the folder for the given migration (e.g. dataDir/{id}).
func (m *Manager) migrationDirFor(migrationID string) (string, error) {
	dir := filepath.Join(m.cfg.Runtime.DataDir, migrationID)
	return filepath.Abs(dir)
}

// GetMigration returns the engine *Migration for the given ID, or ErrMigrationNotFound.
// When using per-migration DBs, the engine expects the migration folder path (e.g. data/{id}) so it can open or create the DB there.
// FS adapters are not rehydrated here; call ensureFSAdaptersRehydrated before traversal, copy, or live FS browse.
func (m *Manager) GetMigration(_ context.Context, migrationID string) (*migration.Migration, error) {
	if m.migAccess == nil {
		return nil, fmt.Errorf("API database not configured")
	}
	return m.migAccess.OpenMigrationDB(context.Background(), migrationID, "")
}

func (m *Manager) upsertMigrationRecord(rec apidb.MigrationRecord) error {
	if m.apiDB == nil {
		return fmt.Errorf("API database not configured")
	}
	return m.apiDB.UpsertMigration(rec)
}

func (m *Manager) getMigrationRecord(migrationID string) (apidb.MigrationRecord, error) {
	if m.apiDB == nil {
		return apidb.MigrationRecord{}, fmt.Errorf("API database not configured")
	}
	rec, err := m.apiDB.GetMigration(migrationID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return apidb.MigrationRecord{ID: migrationID, Name: migrationID}, nil
		}
		return apidb.MigrationRecord{}, err
	}
	return rec, nil
}

// MarkPathReviewChanges updates metadata after exclusion or retry mark/unmark.
func (m *Manager) MarkPathReviewChanges(_ context.Context, migrationID string, hasChanges bool) error {
	rec, err := m.getMigrationRecord(migrationID)
	if err != nil {
		return err
	}
	rec.HasPathReviewChanges = hasChanges
	return m.upsertMigrationRecord(rec)
}

// Ensure Manager implements corebridge.Bridge at compile time.
var _ corebridge.Bridge = (*Manager)(nil)
