package manager

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"codeberg.org/Sylos/Migration-Engine/pkg/migration"
	"codeberg.org/Sylos/Spectra/sdk"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge/database"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge/metadata"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge/services"
	fslib "codeberg.org/Sylos/Sylos-FS/pkg/fs"
	fstypes "codeberg.org/Sylos/Sylos-FS/pkg/types"
)

func (m *Manager) TriggerRetrySweep(ctx context.Context, migrationID string, config corebridge.SweepConfigRequest) (corebridge.SweepResponse, error) {
	if err := m.checkPhaseLock(migrationID, "retrySweep"); err != nil {
		return corebridge.SweepResponse{
			Success: false,
			Error:   err.Error(),
		}, err
	}

	runningTasks := m.bgTaskMgr.GetRunningTasks(migrationID)
	if len(runningTasks) > 0 {
		return corebridge.SweepResponse{
			Success: false,
			Error:   fmt.Sprintf("cannot start retry sweep: there are %d running background tasks. Please wait for them to complete", len(runningTasks)),
		}, fmt.Errorf("cannot start retry sweep: there are %d running background tasks", len(runningTasks))
	}

	if m.bgTaskMgr.HasRunningTask(migrationID, corebridge.BackgroundTaskTypeRetrySweep) {
		return corebridge.SweepResponse{
			Success: false,
			Error:   "retry sweep is already running for this migration",
		}, fmt.Errorf("retry sweep is already running")
	}

	metaMgr := metadata.NewManager(m.cfg.Runtime.DataDir)
	meta, err := metaMgr.GetMigrationMetadata(migrationID)
	if err != nil {
		return corebridge.SweepResponse{
			Success: false,
			Error:   "migration not found",
		}, corebridge.ErrMigrationNotFound
	}

	configPath := meta.ConfigPath
	if configPath == "" {
		dbPath, err := database.ResolveDatabasePath(m.cfg.Runtime.DataDir, "", migrationID)
		if err != nil {
			return corebridge.SweepResponse{
				Success: false,
				Error:   "failed to resolve database path",
			}, fmt.Errorf("failed to resolve database path: %w", err)
		}
		configPath = database.ConfigPathFromDatabasePath(dbPath)
	}

	if _, err := os.Stat(configPath); os.IsNotExist(err) {
		return corebridge.SweepResponse{
			Success: false,
			Error:   fmt.Sprintf("migration config file not found: %s", configPath),
		}, fmt.Errorf("migration config file not found: %s", configPath)
	}

	dbPath := strings.TrimSuffix(configPath, ".yaml") + ".db"
	if dbPath == ".db" {
		dbPath, err = database.ResolveDatabasePath(m.cfg.Runtime.DataDir, "", migrationID)
		if err != nil {
			return corebridge.SweepResponse{
				Success: false,
				Error:   "failed to resolve database path",
			}, fmt.Errorf("failed to resolve database path: %w", err)
		}
	}

	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		return corebridge.SweepResponse{
			Success: false,
			Error:   "migration DB file not found. Migration must be in 'Awaiting-Path-Review' status before retry sweep",
		}, fmt.Errorf("migration DB file not found")
	}

	pendingWork, err := m.CheckPendingWork(ctx, migrationID)
	if err != nil {
		m.logger.Warn().Err(err).Str("migration_id", migrationID).Msg("failed to check pending work, proceeding anyway")
	} else if !pendingWork.HasPendingRetries {
		return corebridge.SweepResponse{
			Success: false,
			Error:   "no pending retries found. Nothing to retry",
		}, fmt.Errorf("no pending retries found")
	}

	if err := m.migrationsMgr.KillDuckDBConnection(migrationID); err != nil {
		m.logger.Warn().Err(err).Str("migration_id", migrationID).Msg("failed to close DuckDB connection (may not be open), proceeding anyway")
	}
	if err := m.migrationsMgr.CloseDB(migrationID); err != nil {
		m.logger.Warn().Err(err).Str("migration_id", migrationID).Msg("failed to close DB connection (may not be open), proceeding anyway")
	}

	overridePath, exists, _ := services.LoadSpectraConfigOverride(m.cfg.Runtime.DataDir, migrationID)
	var spectraConfigPath string
	if exists {
		spectraConfigPath = overridePath
	}

	yamlCfg, err := migration.LoadMigrationConfig(configPath)
	if err != nil {
		return corebridge.SweepResponse{
			Success: false,
			Error:   fmt.Sprintf("failed to load migration config: %v", err),
		}, fmt.Errorf("failed to load migration config: %w", err)
	}

	maxKnownDepth := config.MaxKnownDepth
	if maxKnownDepth == 0 {
		maxKnownDepth = -1
	}

	yamlCfg.State.Status = "Filters-Set"
	if err := migration.SaveMigrationConfig(configPath, yamlCfg); err != nil {
		return corebridge.SweepResponse{
			Success: false,
			Error:   fmt.Sprintf("failed to update status: %v", err),
		}, fmt.Errorf("failed to save migration config: %w", err)
	}

	m.logger.Info().
		Str("migration_id", migrationID).
		Int("max_known_depth", maxKnownDepth).
		Msg("starting retry sweep")

	taskID := m.bgTaskMgr.StartTask(migrationID, corebridge.BackgroundTaskTypeRetrySweep)

	go func() {
		defer func() {
			if r := recover(); r != nil {
				m.bgTaskMgr.FailTask(migrationID, taskID, fmt.Errorf("panic: %v", r))
				panic(r)
			}
		}()

		srcAdapter, dstAdapter, shared, err := m.acquireSharedSpectraAdaptersForSweep(yamlCfg.Services.Source, yamlCfg.Services.Destination, spectraConfigPath)
		if err != nil {
			m.logger.Error().Err(err).Str("migration_id", migrationID).Msg("failed to acquire shared adapters for retry sweep")
			m.bgTaskMgr.FailTask(migrationID, taskID, fmt.Errorf("failed to acquire shared adapters: %w", err))
			return
		}

		if !shared {
			srcAdapter, err = m.acquireAdapterFromYAMLConfigForSweep(yamlCfg.Services.Source, spectraConfigPath)
			if err != nil {
				m.logger.Error().Err(err).Str("migration_id", migrationID).Msg("failed to acquire source adapter for retry sweep")
				m.bgTaskMgr.FailTask(migrationID, taskID, fmt.Errorf("failed to acquire source adapter: %w", err))
				return
			}

			dstAdapter, err = m.acquireAdapterFromYAMLConfigForSweep(yamlCfg.Services.Destination, spectraConfigPath)
			if err != nil {
				if srcAdapter != nil {
					if closer, ok := srcAdapter.(interface{ Close() error }); ok {
						_ = closer.Close()
					}
				}
				m.logger.Error().Err(err).Str("migration_id", migrationID).Msg("failed to acquire destination adapter for retry sweep")
				m.bgTaskMgr.FailTask(migrationID, taskID, fmt.Errorf("failed to acquire destination adapter: %w", err))
				return
			}
		}

		migrationCfg, err := migration.LoadMigrationConfigFromYAML(configPath, srcAdapter, dstAdapter)
		if err != nil {
			m.logger.Error().Err(err).Str("migration_id", migrationID).Msg("failed to load migration config with adapters for retry sweep")
			m.bgTaskMgr.FailTask(migrationID, taskID, fmt.Errorf("failed to load migration config: %w", err))
			return
		}

		sweepCfg := migration.SweepConfig{
			DBPath:        dbPath,
			SrcAdapter:    migrationCfg.Source.Adapter,
			DstAdapter:    migrationCfg.Destination.Adapter,
			WorkerCount:   m.selectWorkerCountForSweep(config.WorkerCount),
			MaxRetries:    m.selectMaxRetriesForSweep(config.MaxRetries),
			MaxKnownDepth: maxKnownDepth,
			ConfigPath:    configPath,
			YAMLConfig:    yamlCfg,
		}

		if config.LogAddress != "" {
			sweepCfg.LogAddress = config.LogAddress
		} else if m.cfg.Runtime.LogAddress != "" {
			sweepCfg.LogAddress = m.cfg.Runtime.LogAddress
		}

		if config.LogLevel != "" {
			sweepCfg.LogLevel = config.LogLevel
		} else if m.cfg.Runtime.LogLevel != "" {
			sweepCfg.LogLevel = m.cfg.Runtime.LogLevel
		} else {
			sweepCfg.LogLevel = "info"
		}

		if config.SkipListener != nil {
			sweepCfg.SkipListener = *config.SkipListener
		} else {
			sweepCfg.SkipListener = true
		}

		if config.StartupDelaySec > 0 {
			sweepCfg.StartupDelay = time.Duration(config.StartupDelaySec) * time.Second
		} else {
			sweepCfg.StartupDelay = 500 * time.Millisecond
		}

		if config.ProgressTickMillis > 0 {
			sweepCfg.ProgressTick = time.Duration(config.ProgressTickMillis) * time.Millisecond
		} else {
			sweepCfg.ProgressTick = 1 * time.Second
		}

		sweepCfg.ShutdownContext = context.Background()

		m.logger.Info().Str("migration_id", migrationID).Msg("starting retry sweep (discovery phase)")

		stats, err := migration.RunRetrySweep(sweepCfg)
		if err != nil {
			m.logger.Error().Err(err).Str("migration_id", migrationID).Msg("retry sweep failed")
			m.bgTaskMgr.FailTask(migrationID, taskID, err)
			return
		}

		m.logger.Info().
			Str("migration_id", migrationID).
			Dur("duration", stats.Duration).
			Int("src_round", stats.Src.Round).
			Int("src_pending", stats.Src.Pending).
			Int("src_in_progress", stats.Src.InProgress).
			Int("dst_round", stats.Dst.Round).
			Int("dst_pending", stats.Dst.Pending).
			Int("dst_in_progress", stats.Dst.InProgress).
			Msg("retry sweep completed")

		updatedYamlCfg, err := migration.LoadMigrationConfig(configPath)
		if err == nil && updatedYamlCfg.State.Status != "Awaiting-Path-Review" {
			updatedYamlCfg.State.Status = "Awaiting-Path-Review"
			_ = migration.SaveMigrationConfig(configPath, updatedYamlCfg)
		}

		if duckdbPool := m.migrationsMgr.GetDuckDBPool(); duckdbPool != nil {
			if _, err := duckdbPool.OpenDuckDB(migrationID, dbPath); err != nil {
				m.logger.Warn().Err(err).Str("migration_id", migrationID).Msg("failed to open DB for path review")
			}
		}

		if err := m.markPathReviewChanges(migrationID, false); err != nil {
			m.logger.Warn().Err(err).Str("migration_id", migrationID).Msg("failed to clear path review changes flag")
		}

		m.bgTaskMgr.CompleteTask(migrationID, taskID)
	}()

	return corebridge.SweepResponse{
		Success: true,
		Message: "Retry sweep started",
	}, nil
}

func (m *Manager) selectWorkerCountForSweep(value int) int {
	if value > 0 {
		return value
	}
	if m.cfg.Runtime.DefaultWorkerCount > 0 {
		return m.cfg.Runtime.DefaultWorkerCount
	}
	return 10
}

func (m *Manager) selectMaxRetriesForSweep(value int) int {
	if value > 0 {
		return value
	}
	if m.cfg.Runtime.DefaultMaxRetries > 0 {
		return m.cfg.Runtime.DefaultMaxRetries
	}
	return 3
}

func (m *Manager) acquireSharedSpectraAdaptersForSweep(srcCfg, dstCfg migration.ServiceConfigYAML, spectraConfigOverridePath string) (fstypes.FSAdapter, fstypes.FSAdapter, bool, error) {
	if strings.ToLower(srcCfg.Type) != "spectra" || strings.ToLower(dstCfg.Type) != "spectra" {
		return nil, nil, false, nil
	}

	getConfigPath := func(serviceCfg migration.ServiceConfigYAML) (string, error) {
		if spectraConfigOverridePath != "" {
			return spectraConfigOverridePath, nil
		}
		def, err := m.serviceMgr.GetServiceDefinition(serviceCfg.Name)
		if err == nil && def.Spectra != nil {
			return def.Spectra.ConfigPath, nil
		}
		world := "primary"
		if strings.Contains(strings.ToLower(serviceCfg.Name), "s1") {
			world = "s1"
		}
		def, err = m.serviceMgr.GetServiceDefinitionByWorld(world)
		if err == nil && def.Spectra != nil {
			return def.Spectra.ConfigPath, nil
		}
		return "", fmt.Errorf("spectra config path not found for service %s", serviceCfg.Name)
	}

	srcConfigPath, err := getConfigPath(srcCfg)
	if err != nil {
		return nil, nil, false, fmt.Errorf("failed to get source config path: %w", err)
	}

	dstConfigPath, err := getConfigPath(dstCfg)
	if err != nil {
		return nil, nil, false, fmt.Errorf("failed to get destination config path: %w", err)
	}

	if srcConfigPath != dstConfigPath {
		return nil, nil, false, nil
	}

	m.logger.Info().
		Str("config_path", srcConfigPath).
		Msg("source and destination use same Spectra config, sharing SDK session for sweep")

	spectraFS, err := sdk.New(srcConfigPath)
	if err != nil {
		return nil, nil, false, fmt.Errorf("failed to create shared SpectraFS session: %w", err)
	}

	srcRootID := srcCfg.RootID
	if srcRootID == "" {
		srcRootID = "root"
	}

	srcWorld := "primary"
	if strings.Contains(strings.ToLower(srcCfg.Name), "s1") {
		srcWorld = "s1"
	} else {
		def, err := m.serviceMgr.GetServiceDefinition(srcCfg.Name)
		if err == nil && def.Spectra != nil {
			srcWorld = def.Spectra.World
		}
	}

	isEphemeral, err := services.IsEphemeralMode(srcConfigPath)
	if err != nil {
		m.logger.Warn().Err(err).Str("config_path", srcConfigPath).Msg("failed to detect ephemeral mode, defaulting to persistent")
		isEphemeral = false
	}

	srcAdapter, err := fslib.NewSpectraFS(spectraFS, srcRootID, srcWorld, isEphemeral)
	if err != nil {
		_ = spectraFS.Close()
		return nil, nil, false, fmt.Errorf("failed to create source adapter: %w", err)
	}

	dstRootID := dstCfg.RootID
	if dstRootID == "" {
		dstRootID = "root"
	}

	dstWorld := "primary"
	if strings.Contains(strings.ToLower(dstCfg.Name), "s1") {
		dstWorld = "s1"
	} else {
		def, err := m.serviceMgr.GetServiceDefinition(dstCfg.Name)
		if err == nil && def.Spectra != nil {
			dstWorld = def.Spectra.World
		}
	}

	dstAdapter, err := fslib.NewSpectraFS(spectraFS, dstRootID, dstWorld, isEphemeral)
	if err != nil {
		_ = spectraFS.Close()
		return nil, nil, false, fmt.Errorf("failed to create destination adapter: %w", err)
	}

	m.logger.Info().
		Str("src_root", srcRootID).
		Str("src_world", srcWorld).
		Str("dst_root", dstRootID).
		Str("dst_world", dstWorld).
		Msg("created shared Spectra adapters for sweep")

	return srcAdapter, dstAdapter, true, nil
}

func (m *Manager) acquireAdapterFromYAMLConfigForSweep(serviceCfg migration.ServiceConfigYAML, spectraConfigOverridePath string) (fstypes.FSAdapter, error) {
	serviceType := strings.ToLower(serviceCfg.Type)
	switch serviceType {
	case "spectra":
		configPath := spectraConfigOverridePath
		if configPath == "" {
			def, err := m.serviceMgr.GetServiceDefinition(serviceCfg.Name)
			if err == nil && def.Spectra != nil {
				configPath = def.Spectra.ConfigPath
			} else {
				world := "primary"
				if strings.Contains(strings.ToLower(serviceCfg.Name), "s1") {
					world = "s1"
				}
				def, err := m.serviceMgr.GetServiceDefinitionByWorld(world)
				if err == nil && def.Spectra != nil {
					configPath = def.Spectra.ConfigPath
				} else {
					return nil, fmt.Errorf("spectra config path not found for service %s", serviceCfg.Name)
				}
			}
		}

		spectraFS, err := sdk.New(configPath)
		if err != nil {
			return nil, fmt.Errorf("failed to create SpectraFS: %w", err)
		}

		rootID := serviceCfg.RootID
		if rootID == "" {
			rootID = "root"
		}

		world := "primary"
		if strings.Contains(strings.ToLower(serviceCfg.Name), "s1") {
			world = "s1"
		} else {
			def, err := m.serviceMgr.GetServiceDefinition(serviceCfg.Name)
			if err == nil && def.Spectra != nil {
				world = def.Spectra.World
			} else {
				def, err := m.serviceMgr.GetServiceDefinitionByWorld(world)
				if err == nil && def.Spectra != nil {
					world = def.Spectra.World
				}
			}
		}

		isEphemeral, err := services.IsEphemeralMode(configPath)
		if err != nil {
			m.logger.Warn().Err(err).Str("config_path", configPath).Msg("failed to detect ephemeral mode, defaulting to persistent")
			isEphemeral = false
		}

		adapter, err := fslib.NewSpectraFS(spectraFS, rootID, world, isEphemeral)
		if err != nil {
			_ = spectraFS.Close()
			return nil, fmt.Errorf("failed to create SpectraFS adapter: %w", err)
		}

		return adapter, nil

	case "local":
		rootPath := serviceCfg.RootPath
		if rootPath == "" {
			return nil, fmt.Errorf("local service %s missing root path", serviceCfg.Name)
		}

		adapter, err := fslib.NewLocalFS(rootPath)
		if err != nil {
			return nil, fmt.Errorf("failed to create LocalFS adapter: %w", err)
		}

		return adapter, nil

	default:
		return nil, fmt.Errorf("unsupported service type: %s", serviceType)
	}
}
