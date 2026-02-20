package migrations

import (
	corebridgeDB "codeberg.org/Sylos/Sylos-API/internal/corebridge/database"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge/metadata"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge/roots"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge/services"
	"codeberg.org/Sylos/Sylos-API/pkg/config"
	"github.com/rs/zerolog"
)

func NewManager(logger zerolog.Logger, cfg config.Config, serviceMgr *services.ServiceManager, rootsMgr *roots.Manager, resolveDBPath func(path, migrationID string) (string, error)) *Manager {
	return &Manager{
		logger:        logger,
		cfg:           cfg,
		migrations:    make(map[string]*MigrationRecord),
		subscribers:   make(map[string]map[string]chan ProgressEvent),
		serviceMgr:    serviceMgr,
		rootsMgr:      rootsMgr,
		metadataMgr:   metadata.NewManager(cfg.Runtime.DataDir),
		resolveDBPath: resolveDBPath,
		dbPool:        NewDBPool(logger),
		duckdbPool:    corebridgeDB.NewDuckDBPool(logger),
	}
}

// SetBackgroundTaskCallback sets the callback function to start background tasks
func (m *Manager) SetBackgroundTaskCallback(callback func(migrationID string, taskType string, path string) string) {
	m.startBgTaskFunc = callback
}

// SetBackgroundTaskCompleteCallback sets the callback to complete background tasks
func (m *Manager) SetBackgroundTaskCompleteCallback(callback func(migrationID, taskID string)) {
	m.startBgTaskCompleteFunc = callback
}

// SetBackgroundTaskFailCallback sets the callback to fail background tasks
func (m *Manager) SetBackgroundTaskFailCallback(callback func(migrationID, taskID string, err error)) {
	m.startBgTaskFailFunc = callback
}
