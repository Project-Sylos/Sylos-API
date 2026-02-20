package manager

import (
	"codeberg.org/Sylos/Sylos-API/internal/corebridge"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge/database"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge/migrations"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge/roots"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge/services"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge/terminal"
	"codeberg.org/Sylos/Sylos-API/pkg/config"
	"github.com/rs/zerolog"
)

// Manager implements corebridge.Bridge.
type Manager struct {
	logger        zerolog.Logger
	cfg           config.Config
	serviceMgr    *services.ServiceManager
	rootsMgr      *roots.Manager
	migrationsMgr *migrations.Manager
	terminalMgr   *terminal.Manager
	bgTaskMgr     *corebridge.BackgroundTaskManager
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
	migrationsMgr := migrations.NewManager(logger, cfg, serviceMgr, rootsMgr, resolveDBPath)
	terminalMgr := terminal.NewManager(logger, cfg)
	bgTaskMgr := corebridge.NewBackgroundTaskManager(logger)

	migrationsMgr.SetBackgroundTaskCallback(func(migrationID string, taskType string, path string) string {
		return bgTaskMgr.StartTaskWithPath(migrationID, corebridge.BackgroundTaskType(taskType), path)
	})
	migrationsMgr.SetBackgroundTaskCompleteCallback(func(migrationID, taskID string) {
		bgTaskMgr.CompleteTask(migrationID, taskID)
	})
	migrationsMgr.SetBackgroundTaskFailCallback(func(migrationID, taskID string, err error) {
		bgTaskMgr.FailTask(migrationID, taskID, err)
	})

	mgr := &Manager{
		logger:        logger,
		cfg:           cfg,
		serviceMgr:    serviceMgr,
		rootsMgr:      rootsMgr,
		migrationsMgr: migrationsMgr,
		terminalMgr:   terminalMgr,
		bgTaskMgr:     bgTaskMgr,
	}

	go migrationsMgr.RecoverInterruptedETL()

	return mgr, nil
}

// Ensure Manager implements corebridge.Bridge at compile time.
var _ corebridge.Bridge = (*Manager)(nil)
