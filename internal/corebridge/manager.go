package corebridge

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/Project-Sylos/Migration-Engine/pkg/migration"
	"github.com/Project-Sylos/Spectra/sdk"
	"github.com/Project-Sylos/Sylos-API/internal/corebridge/database"
	"github.com/Project-Sylos/Sylos-API/internal/corebridge/metadata"
	"github.com/Project-Sylos/Sylos-API/internal/corebridge/migrations"
	"github.com/Project-Sylos/Sylos-API/internal/corebridge/roots"
	"github.com/Project-Sylos/Sylos-API/internal/corebridge/services"
	"github.com/Project-Sylos/Sylos-API/internal/corebridge/terminal"
	"github.com/Project-Sylos/Sylos-API/pkg/config"
	fslib "github.com/Project-Sylos/Sylos-FS/pkg/fs"
	fstypes "github.com/Project-Sylos/Sylos-FS/pkg/types"
	"github.com/rs/zerolog"
)

type Manager struct {
	logger        zerolog.Logger
	cfg           config.Config
	serviceMgr    *services.ServiceManager
	rootsMgr      *roots.Manager
	migrationsMgr *migrations.Manager
	terminalMgr   *terminal.Manager
	bgTaskMgr     *BackgroundTaskManager
}

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
	bgTaskMgr := NewBackgroundTaskManager(logger)

	// Set up callbacks for migrations manager to manage background tasks
	migrationsMgr.SetBackgroundTaskCallback(func(migrationID string, taskType string, path string) string {
		return bgTaskMgr.StartTaskWithPath(migrationID, BackgroundTaskType(taskType), path)
	})
	migrationsMgr.SetBackgroundTaskCompleteCallback(func(migrationID, taskID string) {
		bgTaskMgr.CompleteTask(migrationID, taskID)
	})
	migrationsMgr.SetBackgroundTaskFailCallback(func(migrationID, taskID string, err error) {
		bgTaskMgr.FailTask(migrationID, taskID, err)
	})

	manager := &Manager{
		logger:        logger,
		cfg:           cfg,
		serviceMgr:    serviceMgr,
		rootsMgr:      rootsMgr,
		migrationsMgr: migrationsMgr,
		terminalMgr:   terminalMgr,
		bgTaskMgr:     bgTaskMgr,
	}

	// Recover any interrupted ETL processes on startup
	go migrationsMgr.RecoverInterruptedETL()

	return manager, nil
}

// Bridge interface implementation

func (m *Manager) ListSources(ctx context.Context) ([]Source, error) {
	sources, err := m.serviceMgr.ListSources(ctx)
	if err != nil {
		return nil, err
	}
	result := make([]Source, len(sources))
	for i, s := range sources {
		result[i] = Source{
			ID:          s.ID,
			DisplayName: s.DisplayName,
			Type:        ServiceType(s.Type),
			Metadata:    s.Metadata,
		}
	}
	return result, nil
}

func (m *Manager) ListChildren(ctx context.Context, req ListChildrenRequest) (ListChildrenResponse, error) {
	svcReq := services.ListChildrenRequest{
		ServiceID:   req.ServiceID,
		Identifier:  req.Identifier,
		Role:        req.Role,
		Offset:      req.Offset,
		Limit:       req.Limit,
		FoldersOnly: req.FoldersOnly,
	}
	result, pagination, err := m.serviceMgr.ListChildren(ctx, svcReq)
	if err != nil {
		return ListChildrenResponse{}, err
	}
	return ListChildrenResponse{
		Folders: result.Folders,
		Files:   result.Files,
		Pagination: PaginationInfo{
			Offset:       pagination.Offset,
			Limit:        pagination.Limit,
			Total:        pagination.Total,
			TotalFolders: pagination.TotalFolders,
			TotalFiles:   pagination.TotalFiles,
			HasMore:      pagination.HasMore,
		},
	}, nil
}

func (m *Manager) ListDrives(ctx context.Context, serviceID string) ([]DriveInfo, error) {
	drives, err := m.serviceMgr.ListDrives(ctx, serviceID)
	if err != nil {
		return nil, err
	}
	result := make([]DriveInfo, len(drives))
	for i, d := range drives {
		result[i] = DriveInfo{
			Path:        d.Path,
			DisplayName: d.DisplayName,
			Type:        d.Type,
		}
	}
	return result, nil
}

func (m *Manager) SetRoot(ctx context.Context, req SetRootRequest) (SetRootResponse, error) {
	// Check phase lock - root selection is locked when traversal starts
	if err := m.checkPhaseLock(req.MigrationID, "setRoot"); err != nil {
		return SetRootResponse{}, err
	}

	rootsReq := roots.SetRootRequest{
		MigrationID:  req.MigrationID,
		Role:         req.Role,
		ServiceID:    req.ServiceID,
		ConnectionID: req.ConnectionID,
		Root: roots.FolderDescriptor{
			ID:           req.Root.ID,
			ParentID:     req.Root.ParentID,
			ParentPath:   req.Root.ParentPath,
			DisplayName:  req.Root.DisplayName,
			LocationPath: req.Root.LocationPath,
			LastUpdated:  req.Root.LastUpdated,
			DepthLevel:   req.Root.DepthLevel,
			Type:         req.Root.Type,
		},
		Config: req.Config, // Copy config from request
	}
	resp, err := m.rootsMgr.SetRoot(ctx, rootsReq)
	if err != nil {
		return SetRootResponse{}, err
	}

	// Update metadata in migrations.yaml when roots are set
	// When source root is set, create new migration entry with IsNewMigration=true
	// When destination root is set, update existing entry (keep IsNewMigration flag)
	metaMgr := metadata.NewManager(m.cfg.Runtime.DataDir)
	existingMeta, err := metaMgr.GetMigrationMetadata(resp.MigrationID)
	isNewMigration := true
	if err == nil {
		// Migration already exists - preserve IsNewMigration flag if it was set
		isNewMigration = existingMeta.IsNewMigration
	}

	// Determine config path (will be set when migration starts, but we can prepare it)
	var configPath string
	if resp.DatabasePath != "" {
		configPath = database.ConfigPathFromDatabasePath(resp.DatabasePath)
	}

	meta := metadata.MigrationMetadata{
		ID:             resp.MigrationID,
		Name:           resp.MigrationID, // Default to ID, user can change later
		ConfigPath:     configPath,
		IsNewMigration: isNewMigration,
	}
	if err := metaMgr.UpdateMigrationMetadata(meta); err != nil {
		m.logger.Warn().Err(err).Str("migration_id", resp.MigrationID).Msg("failed to update migration metadata when setting root")
	}

	return SetRootResponse{
		MigrationID:             resp.MigrationID,
		Role:                    resp.Role,
		Ready:                   resp.Ready,
		DatabasePath:            resp.DatabasePath,
		RootSummary:             resp.RootSummary,
		SourceConnectionID:      resp.SourceConnectionID,
		DestinationConnectionID: resp.DestinationConnectionID,
	}, nil
}

func (m *Manager) StartMigration(ctx context.Context, req StartMigrationRequest) (Migration, error) {
	migReq := migrations.StartMigrationRequest{
		MigrationID: req.MigrationID,
		Options: migrations.MigrationOptions{
			MigrationID:             req.Options.MigrationID,
			DatabasePath:            req.Options.DatabasePath,
			RemoveExistingDB:        req.Options.RemoveExistingDB,
			UsePreseededDB:          req.Options.UsePreseededDB,
			SourceConnectionID:      req.Options.SourceConnectionID,
			DestinationConnectionID: req.Options.DestinationConnectionID,
			WorkerCount:             req.Options.WorkerCount,
			MaxRetries:              req.Options.MaxRetries,
			CoordinatorLead:         req.Options.CoordinatorLead,
			LogAddress:              req.Options.LogAddress,
			LogLevel:                req.Options.LogLevel,
			SkipListener:            req.Options.SkipListener,
			StartupDelaySec:         req.Options.StartupDelaySec,
			ProgressTickMillis:      req.Options.ProgressTickMillis,
			Verification: migrations.VerificationOptions{
				AllowPending:  req.Options.Verification.AllowPending,
				AllowNotOnSrc: req.Options.Verification.AllowNotOnSrc,
			},
		},
	}
	mig, err := m.migrationsMgr.StartMigration(ctx, migReq)
	if err != nil {
		return Migration{}, err
	}
	return Migration{
		ID:            mig.ID,
		SourceID:      mig.SourceID,
		DestinationID: mig.DestinationID,
		StartedAt:     mig.StartedAt,
		Status:        mig.Status,
	}, nil
}

// ChangePhase changes the migration phase (traversal or copy) with pending work validation
// It first runs ETL from DuckDB to BoltDB to create a fresh BoltDB file, then starts the migration
func (m *Manager) ChangePhase(ctx context.Context, migrationID string, phase string, req StartMigrationRequest) (Migration, error) {
	// Validate phase
	if phase != "traversal" && phase != "copy" {
		return Migration{}, fmt.Errorf("invalid phase: %s (must be 'traversal' or 'copy')", phase)
	}

	// Check pending work
	pendingWork, err := m.CheckPendingWork(ctx, migrationID)
	if err != nil {
		return Migration{}, fmt.Errorf("failed to check pending work: %w", err)
	}

	// Block copy phase if there are pending retries
	if phase == "copy" && pendingWork.HasPendingRetries {
		return Migration{}, fmt.Errorf("cannot start copy phase: there are pending retries. Please run traversal phase first")
	}

	// Get migration metadata to find paths
	metaMgr := metadata.NewManager(m.cfg.Runtime.DataDir)
	meta, err := metaMgr.GetMigrationMetadata(migrationID)
	if err != nil {
		return Migration{}, fmt.Errorf("failed to get migration metadata: %w", err)
	}

	// Derive database path from config path
	dbPath := strings.TrimSuffix(meta.ConfigPath, ".yaml") + ".db"
	if dbPath == ".db" {
		dbPath, err = database.ResolveDatabasePath(m.cfg.Runtime.DataDir, "", migrationID)
		if err != nil {
			return Migration{}, fmt.Errorf("failed to resolve database path: %w", err)
		}
	}

	// Check if there are any running background tasks
	runningTasks := m.bgTaskMgr.GetRunningTasks(migrationID)
	if len(runningTasks) > 0 {
		return Migration{}, fmt.Errorf("cannot change phase: there are %d running background tasks. Please wait for them to complete", len(runningTasks))
	}

	// Verify DuckDB exists (migration should be in Awaiting-Path-Review)
	duckdbExists, err := database.CheckDuckDBExists(dbPath)
	if err != nil {
		return Migration{}, fmt.Errorf("failed to check DuckDB existence: %w", err)
	}
	if !duckdbExists {
		return Migration{}, fmt.Errorf("DuckDB file not found. Migration must be in 'Awaiting-Path-Review' status before changing phase")
	}

	// Force close all DB connections before ETL (ETL will open its own instances)
	// This ensures files are not locked when ETL tries to open them

	// Close DuckDB connection
	if err := m.migrationsMgr.KillDuckDBConnection(migrationID); err != nil {
		m.logger.Warn().
			Err(err).
			Str("migration_id", migrationID).
			Msg("failed to close DuckDB connection (may not be open), proceeding anyway")
	} else {
		m.logger.Info().
			Str("migration_id", migrationID).
			Msg("closed DuckDB connection before phase change ETL")
	}

	// Close BoltDB connection
	if err := m.migrationsMgr.CloseDB(migrationID); err != nil {
		m.logger.Warn().
			Err(err).
			Str("migration_id", migrationID).
			Msg("failed to close BoltDB connection (may not be open), proceeding anyway")
	} else {
		m.logger.Info().
			Str("migration_id", migrationID).
			Msg("closed BoltDB connection before phase change ETL")
	}

	// Get or create migration record
	record := m.migrationsMgr.GetRecord(migrationID)
	if record == nil {
		// Create a temporary record for ETL tracking
		record = &migrations.MigrationRecord{
			ID: migrationID,
		}
	}

	// Check if Spectra override config exists
	overridePath, exists, _ := services.LoadSpectraConfigOverride(m.cfg.Runtime.DataDir, migrationID)
	var spectraConfigPath string
	if exists {
		spectraConfigPath = overridePath
	}

	// Load YAML config to update status
	yamlCfg, err := migration.LoadMigrationConfig(meta.ConfigPath)
	if err != nil {
		return Migration{}, fmt.Errorf("failed to load migration config: %w", err)
	}

	// Update status based on phase
	if phase == "copy" {
		// Update status to Preparing-For-Copy
		yamlCfg.State.Status = "Preparing-For-Copy"
		if err := migration.SaveMigrationConfig(meta.ConfigPath, yamlCfg); err != nil {
			return Migration{}, fmt.Errorf("failed to update status to Preparing-For-Copy: %w", err)
		}
		m.logger.Info().
			Str("migration_id", migrationID).
			Msg("updated status to Preparing-For-Copy")
	}

	// Convert StartMigrationRequest to MigrationOptions
	migOpts := migrations.MigrationOptions{
		MigrationID:             req.Options.MigrationID,
		DatabasePath:            req.Options.DatabasePath,
		RemoveExistingDB:        req.Options.RemoveExistingDB,
		UsePreseededDB:          req.Options.UsePreseededDB,
		SourceConnectionID:      req.Options.SourceConnectionID,
		DestinationConnectionID: req.Options.DestinationConnectionID,
		WorkerCount:             req.Options.WorkerCount,
		MaxRetries:              req.Options.MaxRetries,
		CoordinatorLead:         req.Options.CoordinatorLead,
		LogAddress:              req.Options.LogAddress,
		LogLevel:                req.Options.LogLevel,
		SkipListener:            req.Options.SkipListener,
		StartupDelaySec:         req.Options.StartupDelaySec,
		ProgressTickMillis:      req.Options.ProgressTickMillis,
		Verification: migrations.VerificationOptions{
			AllowPending:  req.Options.Verification.AllowPending,
			AllowNotOnSrc: req.Options.Verification.AllowNotOnSrc,
		},
	}

	// Trigger ETL from DuckDB to BoltDB in background
	// After ETL completes, it will trigger the appropriate phase (traversal or copy)
	go func() {
		m.migrationsMgr.RunETLFromDuckToBolt(record, dbPath, meta.ConfigPath, func() {
			// ETL completed successfully, now start the appropriate phase
			m.logger.Info().
				Str("migration_id", migrationID).
				Str("phase", phase).
				Msg("ETL completed, starting migration phase")

			if phase == "copy" {
				// Run copy phase
				err := m.migrationsMgr.RunCopyPhase(record, dbPath, meta.ConfigPath, migOpts, spectraConfigPath)
				if err != nil {
					m.logger.Error().
						Err(err).
						Str("migration_id", migrationID).
						Str("phase", phase).
						Msg("failed to start copy phase after ETL")
				}
			} else {
				// Start the migration (traversal phase)
				// The Migration Engine will automatically handle traversal based on checkpoint state
				_, err := m.StartMigration(ctx, req)
				if err != nil {
					m.logger.Error().
						Err(err).
						Str("migration_id", migrationID).
						Str("phase", phase).
						Msg("failed to start migration after ETL")
				}
			}
		})
	}()

	// Return immediately with accepted status
	return Migration{
		ID:      migrationID,
		Status:  "preparing", // Indicates ETL is running before phase change
		Success: true,
	}, nil
}

func (m *Manager) GetMigrationStatus(ctx context.Context, id string) (Status, error) {
	migStatus, err := m.migrationsMgr.GetMigrationStatus(ctx, id)
	if err != nil {
		return Status{}, err
	}

	// Get checkpoint status from YAML config (this is now the primary status)
	var status string
	metaMgr := metadata.NewManager(m.cfg.Runtime.DataDir)
	meta, err := metaMgr.GetMigrationMetadata(id)
	if err == nil && meta.ConfigPath != "" {
		// Try to load YAML config to get checkpoint state
		yamlCfg, err := migration.LoadMigrationConfig(meta.ConfigPath)
		if err == nil && yamlCfg.State.Status != "" {
			status = yamlCfg.State.Status
		}
	}

	// Fall back to traversal status if checkpoint status is not available
	if status == "" {
		status = migStatus.Status
	}

	return Status{
		Migration: Migration{
			ID:            migStatus.ID,
			SourceID:      migStatus.SourceID,
			DestinationID: migStatus.DestinationID,
			StartedAt:     migStatus.StartedAt,
			Status:        status,
		},
		CompletedAt: migStatus.CompletedAt,
		Error:       migStatus.Error,
		Result:      convertResultView(migStatus.Result),
	}, nil
}

func convertResultView(r *migrations.ResultView) *ResultView {
	if r == nil {
		return nil
	}
	return &ResultView{
		RootSummary: RootSummaryView{
			SrcRoots: r.RootSummary.SrcRoots,
			DstRoots: r.RootSummary.DstRoots,
		},
		Runtime: RuntimeStatsView{
			Duration: r.Runtime.Duration,
			Src: QueueStatsView{
				Name:         r.Runtime.Src.Name,
				Round:        r.Runtime.Src.Round,
				Pending:      r.Runtime.Src.Pending,
				InProgress:   r.Runtime.Src.InProgress,
				TotalTracked: r.Runtime.Src.TotalTracked,
				Workers:      r.Runtime.Src.Workers,
			},
			Dst: QueueStatsView{
				Name:         r.Runtime.Dst.Name,
				Round:        r.Runtime.Dst.Round,
				Pending:      r.Runtime.Dst.Pending,
				InProgress:   r.Runtime.Dst.InProgress,
				TotalTracked: r.Runtime.Dst.TotalTracked,
				Workers:      r.Runtime.Dst.Workers,
			},
		},
		Verification: VerificationView{
			SrcTotal:    r.Verification.SrcTotal,
			DstTotal:    r.Verification.DstTotal,
			SrcPending:  r.Verification.SrcPending,
			DstPending:  r.Verification.DstPending,
			SrcFailed:   r.Verification.SrcFailed,
			DstFailed:   r.Verification.DstFailed,
			DstNotOnSrc: r.Verification.DstNotOnSrc,
		},
	}
}

func (m *Manager) InspectMigrationStatus(ctx context.Context, migrationID string) (migration.MigrationStatus, error) {
	return m.migrationsMgr.InspectMigrationStatus(ctx, migrationID)
}

func (m *Manager) InspectMigrationStatusFromDB(ctx context.Context, dbPath string) (migration.MigrationStatus, error) {
	return database.InspectMigrationStatusFromDB(ctx, m.logger, dbPath)
}

func (m *Manager) UploadMigrationDB(ctx context.Context, migrationID string, data []byte, overwrite bool) (UploadMigrationDBResponse, error) {
	resp, err := database.UploadMigrationDB(ctx, m.logger, m.cfg.Runtime.DataDir, migrationID, data, overwrite)
	if err != nil {
		return UploadMigrationDBResponse{}, err
	}
	return UploadMigrationDBResponse{
		Success: resp.Success,
		Error:   resp.Error,
		Path:    resp.Path,
	}, nil
}

func (m *Manager) UploadMigrationYAML(ctx context.Context, migrationID string, data []byte, overwrite bool) (UploadMigrationDBResponse, error) {
	resp, err := database.UploadMigrationYAML(ctx, m.logger, m.cfg.Runtime.DataDir, migrationID, data, overwrite)
	if err != nil {
		return UploadMigrationDBResponse{}, err
	}
	return UploadMigrationDBResponse{
		Success: resp.Success,
		Error:   resp.Error,
		Path:    resp.Path,
	}, nil
}

func (m *Manager) UploadMigrationData(ctx context.Context, migrationID string, zipData []byte, overwrite bool) (UploadMigrationDBResponse, error) {
	resp, err := database.UploadMigrationData(ctx, m.logger, m.cfg.Runtime.DataDir, migrationID, zipData, overwrite)
	if err != nil {
		return UploadMigrationDBResponse{}, err
	}
	return UploadMigrationDBResponse{
		Success: resp.Success,
		Error:   resp.Error,
		Path:    resp.Path,
	}, nil
}

func (m *Manager) ListMigrationDBs(ctx context.Context) ([]MigrationDBInfo, error) {
	dbs, err := database.ListMigrationDBs(ctx, m.logger, m.cfg.Runtime.MigrationDBStorageDir)
	if err != nil {
		return nil, err
	}
	result := make([]MigrationDBInfo, len(dbs))
	for i, db := range dbs {
		result[i] = MigrationDBInfo{
			Filename:   db.Filename,
			Path:       db.Path,
			Size:       db.Size,
			ModifiedAt: db.ModifiedAt,
		}
	}
	return result, nil
}

func (m *Manager) SubscribeProgress(ctx context.Context, id string) (<-chan ProgressEvent, func(), error) {
	migCh, cancel, err := m.migrationsMgr.SubscribeProgress(ctx, id)
	if err != nil {
		return nil, nil, err
	}
	// Convert channel
	ch := make(chan ProgressEvent, cap(migCh))
	go func() {
		for migEvent := range migCh {
			ch <- ProgressEvent{
				Event:     migEvent.Event,
				Timestamp: migEvent.Timestamp,
				Migration: convertStatus(migEvent.Migration),
				Source: QueueStatsSnapshot{
					Round:        migEvent.Source.Round,
					Pending:      migEvent.Source.Pending,
					InProgress:   migEvent.Source.InProgress,
					TotalTracked: migEvent.Source.TotalTracked,
					Workers:      migEvent.Source.Workers,
				},
				Destination: QueueStatsSnapshot{
					Round:        migEvent.Destination.Round,
					Pending:      migEvent.Destination.Pending,
					InProgress:   migEvent.Destination.InProgress,
					TotalTracked: migEvent.Destination.TotalTracked,
					Workers:      migEvent.Destination.Workers,
				},
			}
		}
		close(ch)
	}()
	return ch, cancel, nil
}

func convertStatus(s migrations.Status) Status {
	return Status{
		Migration: Migration{
			ID:            s.ID,
			SourceID:      s.SourceID,
			DestinationID: s.DestinationID,
			StartedAt:     s.StartedAt,
			Status:        s.Status,
		},
		CompletedAt: s.CompletedAt,
		Error:       s.Error,
		Result:      convertResultView(s.Result),
	}
}

func (m *Manager) ToggleLogTerminal(ctx context.Context, enable bool, logAddress string) error {
	return m.terminalMgr.ToggleLogTerminal(ctx, enable, logAddress)
}

// GetMigrationMetadata retrieves metadata for a specific migration
func (m *Manager) GetMigrationMetadata(ctx context.Context, migrationID string) (MigrationMetadata, error) {
	metaMgr := metadata.NewManager(m.cfg.Runtime.DataDir)
	meta, err := metaMgr.GetMigrationMetadata(migrationID)
	if err != nil {
		return MigrationMetadata{}, err
	}
	return convertMetadata(meta), nil
}

// UpdateMigrationName updates the name of a migration
func (m *Manager) UpdateMigrationName(ctx context.Context, migrationID, name string) error {
	metaMgr := metadata.NewManager(m.cfg.Runtime.DataDir)
	meta, err := metaMgr.GetMigrationMetadata(migrationID)
	if err != nil {
		return err
	}
	meta.Name = name
	return metaMgr.UpdateMigrationMetadata(meta)
}

// ListAllMigrations returns all migrations with full status information, including pagination
func (m *Manager) ListAllMigrations(ctx context.Context, req ListMigrationsRequest) (ListMigrationsResponse, error) {
	// Validate and set defaults for pagination
	offset := req.Offset
	if offset < 0 {
		offset = 0
	}
	limit := req.Limit
	if limit <= 0 {
		limit = 100 // Default limit
	}
	if limit > 1000 {
		limit = 1000 // Max limit
	}

	// Get all migration metadata (including those without config files)
	metaMgr := metadata.NewManager(m.cfg.Runtime.DataDir)
	allMeta, err := metaMgr.LoadAllMetadata()
	if err != nil {
		return ListMigrationsResponse{}, fmt.Errorf("failed to load migration metadata: %w", err)
	}

	total := len(allMeta.Migrations)
	statuses := make([]Status, 0, total)

	// Build Status for each migration
	for id, meta := range allMeta.Migrations {
		// Try to get full status, but handle gracefully if it fails
		status, err := m.GetMigrationStatus(ctx, id)
		if err != nil {
			// Migration might not have DB/config yet - build minimal status from metadata
			status = Status{
				Migration: Migration{
					ID:            meta.ID,
					SourceID:      "", // Not available from metadata alone
					DestinationID: "", // Not available from metadata alone
					StartedAt:     meta.CreatedAt,
					Status:        "", // No status yet - migration hasn't started
				},
				CompletedAt: nil,
				Error:       "",
				Result:      nil,
			}

			// Try to get source/destination IDs from roots if available
			plan := m.rootsMgr.GetPlan(id)
			if plan != nil {
				if plan.HasSource {
					status.SourceID = plan.SourceDefinition.ID
				}
				if plan.HasDestination {
					status.DestinationID = plan.DestinationDefinition.ID
				}
				// If roots are set but migration hasn't started, status could be "Roots-Set"
				if plan.HasSource && plan.HasDestination {
					status.Status = "Roots-Set"
				} else if plan.HasSource || plan.HasDestination {
					status.Status = "Roots-Partial" // One root set but not both
				}
			}
		}

		statuses = append(statuses, status)
	}

	// Sort by CreatedAt/StartedAt (newest first)
	sort.Slice(statuses, func(i, j int) bool {
		timeI := statuses[i].StartedAt
		timeJ := statuses[j].StartedAt
		if timeI.IsZero() {
			timeI = time.Time{} // Treat zero time as oldest
		}
		if timeJ.IsZero() {
			timeJ = time.Time{}
		}
		return timeI.After(timeJ) // Newest first
	})

	// Apply pagination
	hasMore := offset+limit < total
	end := offset + limit
	if end > total {
		end = total
	}

	var paginatedStatuses []Status
	if offset < total {
		paginatedStatuses = statuses[offset:end]
	} else {
		paginatedStatuses = []Status{} // Empty if offset is beyond total
	}

	return ListMigrationsResponse{
		Migrations: paginatedStatuses,
		Total:      total,
		Offset:     offset,
		Limit:      limit,
		HasMore:    hasMore,
	}, nil
}

// LoadMigration loads and resumes a migration from its YAML config file
func (m *Manager) LoadMigration(ctx context.Context, migrationID string) (Migration, error) {
	// Get migration metadata to find the config path
	metaMgr := metadata.NewManager(m.cfg.Runtime.DataDir)
	meta, err := metaMgr.GetMigrationMetadata(migrationID)
	if err != nil {
		return Migration{}, fmt.Errorf("migration metadata not found: %w", err)
	}

	if meta.ConfigPath == "" {
		return Migration{}, fmt.Errorf("migration %s has no config path", migrationID)
	}

	// Use the migrations manager's LoadMigrationFromConfigPath method
	mg, err := m.migrationsMgr.LoadMigrationFromConfigPath(ctx, migrationID, meta.ConfigPath)
	if err != nil {
		return Migration{}, err
	}

	// Convert migrations.Migration to corebridge.Migration
	return Migration{
		ID:            mg.ID,
		SourceID:      mg.SourceID,
		DestinationID: mg.DestinationID,
		StartedAt:     mg.StartedAt,
		Status:        mg.Status,
	}, nil
}

func (m *Manager) StopMigration(ctx context.Context, migrationID string) (Status, error) {
	// Call the migrations manager's StopMigration method
	result, err := m.migrationsMgr.StopMigration(ctx, migrationID)
	if err != nil {
		return Status{}, err
	}

	// Get the migration status to build a complete Status response
	status, err := m.GetMigrationStatus(ctx, migrationID)
	if err != nil {
		// If we can't get status, create a minimal status from the result
		status = Status{
			Migration: Migration{
				ID:     migrationID,
				Status: MigrationStatusSuspended,
			},
		}
		if result != nil {
			status.Result = convertResultToView(result)
			finished := time.Now().UTC()
			status.CompletedAt = &finished
		}
	}

	return status, nil
}

func convertResultToView(res *migration.Result) *ResultView {
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
			Src: QueueStatsView{
				Name:         res.Runtime.Src.Name,
				Round:        res.Runtime.Src.Round,
				Pending:      res.Runtime.Src.Pending,
				InProgress:   res.Runtime.Src.InProgress,
				TotalTracked: res.Runtime.Src.TotalTracked,
				Workers:      res.Runtime.Src.Workers,
			},
			Dst: QueueStatsView{
				Name:         res.Runtime.Dst.Name,
				Round:        res.Runtime.Dst.Round,
				Pending:      res.Runtime.Dst.Pending,
				InProgress:   res.Runtime.Dst.InProgress,
				TotalTracked: res.Runtime.Dst.TotalTracked,
				Workers:      res.Runtime.Dst.Workers,
			},
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

// GetQueueMetrics retrieves queue metrics for a migration
func (m *Manager) GetQueueMetrics(ctx context.Context, migrationID string) (*QueueMetricsResponse, error) {
	// Get DB instance - tries record.DB first, then pool
	boltDB := m.migrationsMgr.GetDB(migrationID)

	var dbMetrics *database.QueueMetricsResponse
	var err error

	if boltDB != nil {
		// Use the shared DB instance from the pool
		dbMetrics, err = database.GetQueueMetricsFromDBInstance(ctx, m.logger, boltDB)
		if err != nil {
			// Check if this is a database not available error - indicates DB was closed unexpectedly
			if isDatabaseNotAvailableError(err) {
				// CRITICAL: DB was closed unexpectedly - this indicates a lifecycle violation
				m.logger.Error().
					Err(err).
					Str("migration_id", migrationID).
					Msg("CRITICAL: DB for migration was closed unexpectedly — this indicates an ME lifecycle violation")

				return &QueueMetricsResponse{
					Success:   false,
					ErrorCode: "DATABASE_NOT_AVAILABLE",
					Error:     "database instance is not available (migration may be initializing or completed)",
				}, nil
			}
			return nil, fmt.Errorf("failed to get queue metrics from DB instance: %w", err)
		}
	} else {
		// Fallback: migration not running or DB not available, open a new connection
		// Get migration metadata to find the config path
		metaMgr := metadata.NewManager(m.cfg.Runtime.DataDir)
		meta, err := metaMgr.GetMigrationMetadata(migrationID)
		if err != nil {
			return nil, ErrMigrationNotFound
		}

		// Derive database path from config path
		// Config path is {db_path}.yaml, so DB path is {config_path sans .yaml}.db
		dbPath := strings.TrimSuffix(meta.ConfigPath, ".yaml") + ".db"
		if dbPath == ".db" {
			// Fallback: try to resolve from migration ID
			dbPath, err = database.ResolveDatabasePath(m.cfg.Runtime.DataDir, "", migrationID)
			if err != nil {
				return nil, fmt.Errorf("failed to resolve database path: %w", err)
			}
		}

		// Check if database file exists
		if _, err := os.Stat(dbPath); os.IsNotExist(err) {
			return nil, ErrMigrationNotFound
		}

		// Get queue metrics from database
		dbMetrics, err = database.GetQueueMetricsFromDB(ctx, m.logger, dbPath)
		if err != nil {
			return nil, fmt.Errorf("failed to get queue metrics: %w", err)
		}
	}

	// Convert database.QueueMetricsResponse to corebridge.QueueMetricsResponse
	metrics := &QueueMetricsResponse{
		Success: true, // Operation succeeded
	}
	if dbMetrics.SrcTraversal != nil {
		metrics.SrcTraversal = &ExternalQueueMetrics{
			// Traversal phase metrics
			FilesDiscoveredTotal:     dbMetrics.SrcTraversal.FilesDiscoveredTotal,
			FoldersDiscoveredTotal:   dbMetrics.SrcTraversal.FoldersDiscoveredTotal,
			DiscoveryRateItemsPerSec: dbMetrics.SrcTraversal.DiscoveryRateItemsPerSec,
			TotalDiscovered:          dbMetrics.SrcTraversal.TotalDiscovered,

			// Common state fields
			Round:        dbMetrics.SrcTraversal.Round,
			Pending:      dbMetrics.SrcTraversal.Pending,
			InProgress:   dbMetrics.SrcTraversal.InProgress,
			Workers:      dbMetrics.SrcTraversal.Workers,
			Name:         dbMetrics.SrcTraversal.Name,
			TotalPending: dbMetrics.SrcTraversal.TotalPending,
			TotalFailed:  dbMetrics.SrcTraversal.TotalFailed,
			TotalTracked: dbMetrics.SrcTraversal.TotalTracked,
		}
	}
	if dbMetrics.DstTraversal != nil {
		metrics.DstTraversal = &ExternalQueueMetrics{
			// Traversal phase metrics
			FilesDiscoveredTotal:     dbMetrics.DstTraversal.FilesDiscoveredTotal,
			FoldersDiscoveredTotal:   dbMetrics.DstTraversal.FoldersDiscoveredTotal,
			DiscoveryRateItemsPerSec: dbMetrics.DstTraversal.DiscoveryRateItemsPerSec,
			TotalDiscovered:          dbMetrics.DstTraversal.TotalDiscovered,

			// Common state fields
			Round:        dbMetrics.DstTraversal.Round,
			Pending:      dbMetrics.DstTraversal.Pending,
			InProgress:   dbMetrics.DstTraversal.InProgress,
			Workers:      dbMetrics.DstTraversal.Workers,
			Name:         dbMetrics.DstTraversal.Name,
			TotalPending: dbMetrics.DstTraversal.TotalPending,
			TotalFailed:  dbMetrics.DstTraversal.TotalFailed,
			TotalTracked: dbMetrics.DstTraversal.TotalTracked,
		}
	}
	if dbMetrics.Copy != nil {
		metrics.Copy = &ExternalQueueMetrics{
			// Copy phase metrics (new format from engine)
			Folders:        dbMetrics.Copy.Folders,
			Files:          dbMetrics.Copy.Files,
			Total:          dbMetrics.Copy.Total,
			Bytes:          dbMetrics.Copy.Bytes,
			ItemsPerSecond: dbMetrics.Copy.ItemsPerSecond,
			BytesPerSecond: dbMetrics.Copy.BytesPerSecond,

			// Common state fields
			Round:        dbMetrics.Copy.Round,
			Pending:      dbMetrics.Copy.Pending,
			InProgress:   dbMetrics.Copy.InProgress,
			Workers:      dbMetrics.Copy.Workers,
			TotalPending: dbMetrics.Copy.TotalPending,
			TotalFailed:  dbMetrics.Copy.TotalFailed,
			Name:         dbMetrics.Copy.Name,

			// Legacy fields for backward compatibility
			TotalTracked: dbMetrics.Copy.TotalTracked,

			// Traversal phase fields (may be empty for copy phase)
			FilesDiscoveredTotal:     dbMetrics.Copy.FilesDiscoveredTotal,
			FoldersDiscoveredTotal:   dbMetrics.Copy.FoldersDiscoveredTotal,
			DiscoveryRateItemsPerSec: dbMetrics.Copy.DiscoveryRateItemsPerSec,
			TotalDiscovered:          dbMetrics.Copy.TotalDiscovered,
		}
	}

	return metrics, nil
}

// isDatabaseNotAvailableError checks if an error indicates the database is not available
// This includes errors like "database not open" from BoltDB
func isDatabaseNotAvailableError(err error) bool {
	if err == nil {
		return false
	}
	errStr := err.Error()
	return strings.Contains(errStr, "database not open") ||
		strings.Contains(errStr, "database not available") ||
		strings.Contains(errStr, "not open")
}

// GetLogs retrieves logs for a migration
func (m *Manager) GetLogs(ctx context.Context, migrationID string, req GetLogsRequest) (*GetLogsResponse, error) {
	// Get DB instance - tries record.DB first, then pool
	boltDB := m.migrationsMgr.GetDB(migrationID)

	var dbLogs map[string][]database.LogEntry
	var err error

	if boltDB != nil {
		// Use the shared DB instance from the pool
		dbLogs, err = database.GetLogsFromDBInstance(ctx, m.logger, boltDB)
		if err != nil {
			// Check if this is a database not available error - indicates DB was closed unexpectedly
			if isDatabaseNotAvailableError(err) {
				// CRITICAL: DB was closed unexpectedly - this indicates a lifecycle violation
				m.logger.Error().
					Err(err).
					Str("migration_id", migrationID).
					Msg("CRITICAL: DB for migration was closed unexpectedly — this indicates an ME lifecycle violation")

				return &GetLogsResponse{
					Success:   false,
					ErrorCode: "DATABASE_NOT_AVAILABLE",
					Error:     "database instance is not available (migration may be initializing or completed)",
					Logs:      make(map[string][]LogEntry),
				}, nil
			}
			return nil, fmt.Errorf("failed to get logs from DB instance: %w", err)
		}
	} else {
		// Fallback: migration not running or DB not available, open a new connection
		// Get migration metadata to find the config path
		metaMgr := metadata.NewManager(m.cfg.Runtime.DataDir)
		meta, err := metaMgr.GetMigrationMetadata(migrationID)
		if err != nil {
			return nil, ErrMigrationNotFound
		}

		// Derive database path from config path
		// Config path is {db_path}.yaml, so DB path is {config_path sans .yaml}.db
		dbPath := strings.TrimSuffix(meta.ConfigPath, ".yaml") + ".db"
		if dbPath == ".db" {
			// Fallback: try to resolve from migration ID
			dbPath, err = database.ResolveDatabasePath(m.cfg.Runtime.DataDir, "", migrationID)
			if err != nil {
				return nil, fmt.Errorf("failed to resolve database path: %w", err)
			}
		}

		// Check if database file exists
		if _, err := os.Stat(dbPath); os.IsNotExist(err) {
			return nil, ErrMigrationNotFound
		}

		// Get logs from database
		dbLogs, err = database.GetLogsFromDB(ctx, m.logger, dbPath)
		if err != nil {
			return nil, fmt.Errorf("failed to get logs: %w", err)
		}
	}

	// Convert database.LogEntry to corebridge.LogEntry
	logs := make(map[string][]LogEntry)
	for level, entries := range dbLogs {
		logEntries := make([]LogEntry, len(entries))
		for i, entry := range entries {
			logEntries[i] = LogEntry{
				ID:    entry.ID,
				Level: entry.Level,
				Data:  entry.Data,
			}
		}
		logs[level] = logEntries
	}

	return &GetLogsResponse{
		Success: true, // Operation succeeded
		Logs:    logs,
	}, nil
}

// ListChildrenDiffs retrieves merged children from both SRC and DST queues with status information
func (m *Manager) ListChildrenDiffs(ctx context.Context, req ListChildrenDiffsRequest) (ListChildrenDiffsResponse, error) {
	// Get migration metadata to check status and get paths
	metaMgr := metadata.NewManager(m.cfg.Runtime.DataDir)
	meta, err := metaMgr.GetMigrationMetadata(req.MigrationID)
	if err != nil {
		return ListChildrenDiffsResponse{}, ErrMigrationNotFound
	}

	// Derive database path from config path
	dbPath := strings.TrimSuffix(meta.ConfigPath, ".yaml") + ".db"
	if dbPath == ".db" {
		dbPath, err = database.ResolveDatabasePath(m.cfg.Runtime.DataDir, "", req.MigrationID)
		if err != nil {
			return ListChildrenDiffsResponse{}, fmt.Errorf("failed to resolve database path: %w", err)
		}
	}

	// Check if DuckDB is available (status is Awaiting-Path-Review or Awaiting-Copy-Review)
	useDuckDB := false
	if meta.ConfigPath != "" {
		yamlCfg, err := migration.LoadMigrationConfig(meta.ConfigPath)
		if err == nil {
			status := strings.TrimSpace(yamlCfg.State.Status)
			if status == "Awaiting-Path-Review" || status == "Awaiting-Copy-Review" {
				useDuckDB = true
			} else if status == "Preparing-Path-Review" {
				// Ensure ETL is running or completed
				err := m.migrationsMgr.EnsureETLCompleted(req.MigrationID, meta.ConfigPath, dbPath)
				if err != nil {
					return ListChildrenDiffsResponse{}, fmt.Errorf("ETL not ready: %w", err)
				}
				// Re-check status after ETL
				yamlCfg, err = migration.LoadMigrationConfig(meta.ConfigPath)
				if err == nil {
					updatedStatus := strings.TrimSpace(yamlCfg.State.Status)
					if updatedStatus == "Awaiting-Path-Review" || updatedStatus == "Awaiting-Copy-Review" {
						useDuckDB = true
					}
				}
			}
		}
	}

	var dbItems map[string]database.PathNodes
	var dbPagination database.PaginationInfo

	if useDuckDB {
		// Use DuckDB for path review operations
		duckdbPath := database.GetDuckDBPath(dbPath)
		duckdbConn := m.migrationsMgr.GetDuckDB(req.MigrationID)
		if duckdbConn == nil {
			// Try to open DuckDB
			duckdbPool := m.migrationsMgr.GetDuckDBPool()
			if duckdbPool != nil {
				duckdbConn, err = duckdbPool.OpenDuckDB(req.MigrationID, duckdbPath)
				if err != nil {
					return ListChildrenDiffsResponse{}, fmt.Errorf("failed to open DuckDB: %w", err)
				}
			} else {
				return ListChildrenDiffsResponse{}, fmt.Errorf("DuckDB pool not available")
			}
		}

		// Extract sort parameters
		sortField := ""
		sortDir := "asc"
		if req.Sort != nil {
			sortField = req.Sort.Field
			if req.Sort.Direction != "" {
				sortDir = req.Sort.Direction
			}
		}

		// Determine review phase from YAML status
		reviewPhase := "traversal" // default
		if meta.ConfigPath != "" {
			yamlCfg, err := migration.LoadMigrationConfig(meta.ConfigPath)
			if err == nil {
				status := strings.TrimSpace(yamlCfg.State.Status)
				if status == "Awaiting-Copy-Review" {
					reviewPhase = "copy"
				}
			}
		}

		dbItems, dbPagination, err = database.GetChildrenDiffsFromDuckDB(ctx, m.logger, duckdbConn, req.Path, req.Offset, req.Limit, req.FoldersOnly, sortField, sortDir, reviewPhase)
		if err != nil {
			return ListChildrenDiffsResponse{}, fmt.Errorf("failed to get children diffs from DuckDB: %w", err)
		}
	}

	// Convert database types to corebridge types
	items := make(map[string]PathNodes, len(dbItems))
	for path, dbPathNodes := range dbItems {
		pathNodes := PathNodes{}

		if dbPathNodes.Src != nil {
			pathNodes.Src = &PathNodeItem{
				Queue:           dbPathNodes.Src.Queue,
				Id:              dbPathNodes.Src.Id,
				ParentId:        dbPathNodes.Src.ParentId,
				ParentPath:      dbPathNodes.Src.ParentPath,
				Name:            dbPathNodes.Src.Name,
				LocationPath:    dbPathNodes.Src.LocationPath,
				LastUpdated:     dbPathNodes.Src.LastUpdated,
				DepthLevel:      dbPathNodes.Src.DepthLevel,
				Type:            dbPathNodes.Src.Type,
				Size:            dbPathNodes.Src.Size,
				TraversalStatus: dbPathNodes.Src.TraversalStatus,
				CopyStatus:      dbPathNodes.Src.CopyStatus,
			}
		}

		if dbPathNodes.Dst != nil {
			pathNodes.Dst = &PathNodeItem{
				Queue:           dbPathNodes.Dst.Queue,
				Id:              dbPathNodes.Dst.Id,
				ParentId:        dbPathNodes.Dst.ParentId,
				ParentPath:      dbPathNodes.Dst.ParentPath,
				Name:            dbPathNodes.Dst.Name,
				LocationPath:    dbPathNodes.Dst.LocationPath,
				LastUpdated:     dbPathNodes.Dst.LastUpdated,
				DepthLevel:      dbPathNodes.Dst.DepthLevel,
				Type:            dbPathNodes.Dst.Type,
				Size:            dbPathNodes.Dst.Size,
				TraversalStatus: dbPathNodes.Dst.TraversalStatus,
				CopyStatus:      dbPathNodes.Dst.CopyStatus,
			}
		}

		items[path] = pathNodes
	}

	return ListChildrenDiffsResponse{
		Items: items,
		Pagination: PaginationInfo{
			Offset:       dbPagination.Offset,
			Limit:        dbPagination.Limit,
			Total:        dbPagination.Total,
			TotalFolders: dbPagination.TotalFolders,
			TotalFiles:   dbPagination.TotalFiles,
			HasMore:      dbPagination.HasMore,
		},
	}, nil
}

// SearchPathReviewItems searches for path review items matching the given conditions
func (m *Manager) SearchPathReviewItems(ctx context.Context, migrationID string, req SearchRequest, offset, limit int) (ListChildrenDiffsResponse, error) {
	// Get migration metadata to check status and get paths
	metaMgr := metadata.NewManager(m.cfg.Runtime.DataDir)
	meta, err := metaMgr.GetMigrationMetadata(migrationID)
	if err != nil {
		return ListChildrenDiffsResponse{}, ErrMigrationNotFound
	}

	// Derive database path from config path
	dbPath := strings.TrimSuffix(meta.ConfigPath, ".yaml") + ".db"
	if dbPath == ".db" {
		dbPath, err = database.ResolveDatabasePath(m.cfg.Runtime.DataDir, "", migrationID)
		if err != nil {
			return ListChildrenDiffsResponse{}, fmt.Errorf("failed to resolve database path: %w", err)
		}
	}

	// Check if DuckDB is available (status is Awaiting-Path-Review or Awaiting-Copy-Review)
	useDuckDB := false
	if meta.ConfigPath != "" {
		yamlCfg, err := migration.LoadMigrationConfig(meta.ConfigPath)
		if err == nil {
			status := strings.TrimSpace(yamlCfg.State.Status)
			if status == "Awaiting-Path-Review" || status == "Awaiting-Copy-Review" {
				useDuckDB = true
			}
		}
	}

	if !useDuckDB {
		return ListChildrenDiffsResponse{}, ErrDatabaseNotAvailable
	}

	// Open DuckDB connection
	duckdbPath := database.GetDuckDBPath(dbPath)
	duckdbConn := m.migrationsMgr.GetDuckDB(migrationID)
	if duckdbConn == nil {
		duckdbPool := m.migrationsMgr.GetDuckDBPool()
		if duckdbPool != nil {
			duckdbConn, err = duckdbPool.OpenDuckDB(migrationID, duckdbPath)
			if err != nil {
				return ListChildrenDiffsResponse{}, fmt.Errorf("failed to open DuckDB: %w", err)
			}
		} else {
			return ListChildrenDiffsResponse{}, fmt.Errorf("DuckDB pool not available")
		}
	}

	// Convert corebridge.SearchCondition to database.SearchCondition
	dbConditions := make([]database.SearchCondition, len(req.Conditions))
	for i, cond := range req.Conditions {
		dbConditions[i] = database.SearchCondition{
			Field:    cond.Field,
			Operator: cond.Operator,
			Value:    cond.Value,
		}
	}

	// Extract sort parameters
	sortField := ""
	sortDir := "asc"
	if req.Sort != nil {
		sortField = req.Sort.Field
		if req.Sort.Direction != "" {
			sortDir = req.Sort.Direction
		}
	}

	// Extract statusSearchType from request (default to "both")
	statusSearchType := req.StatusSearchType
	if statusSearchType == "" {
		statusSearchType = "both"
	}

	// Determine review phase from context
	reviewPhase := "traversal" // default
	if meta.ConfigPath != "" {
		yamlCfg, err := migration.LoadMigrationConfig(meta.ConfigPath)
		if err == nil {
			status := strings.TrimSpace(yamlCfg.State.Status)
			if status == "Awaiting-Copy-Review" {
				reviewPhase = "copy"
			}
		}
	}

	// Call database search function
	dbItems, dbPagination, err := database.SearchPathReviewItemsDuckDB(ctx, m.logger, duckdbConn, dbConditions, offset, limit, sortField, sortDir, statusSearchType, reviewPhase)
	if err != nil {
		return ListChildrenDiffsResponse{}, fmt.Errorf("failed to search path review items: %w", err)
	}

	// Convert database types to corebridge types (same as ListChildrenDiffs)
	items := make(map[string]PathNodes, len(dbItems))
	for path, dbPathNodes := range dbItems {
		pathNodes := PathNodes{}

		if dbPathNodes.Src != nil {
			pathNodes.Src = &PathNodeItem{
				Queue:           dbPathNodes.Src.Queue,
				Id:              dbPathNodes.Src.Id,
				ParentId:        dbPathNodes.Src.ParentId,
				ParentPath:      dbPathNodes.Src.ParentPath,
				Name:            dbPathNodes.Src.Name,
				LocationPath:    dbPathNodes.Src.LocationPath,
				LastUpdated:     dbPathNodes.Src.LastUpdated,
				DepthLevel:      dbPathNodes.Src.DepthLevel,
				Type:            dbPathNodes.Src.Type,
				Size:            dbPathNodes.Src.Size,
				TraversalStatus: dbPathNodes.Src.TraversalStatus,
				CopyStatus:      dbPathNodes.Src.CopyStatus,
			}
		}

		if dbPathNodes.Dst != nil {
			pathNodes.Dst = &PathNodeItem{
				Queue:           dbPathNodes.Dst.Queue,
				Id:              dbPathNodes.Dst.Id,
				ParentId:        dbPathNodes.Dst.ParentId,
				ParentPath:      dbPathNodes.Dst.ParentPath,
				Name:            dbPathNodes.Dst.Name,
				LocationPath:    dbPathNodes.Dst.LocationPath,
				LastUpdated:     dbPathNodes.Dst.LastUpdated,
				DepthLevel:      dbPathNodes.Dst.DepthLevel,
				Type:            dbPathNodes.Dst.Type,
				Size:            dbPathNodes.Dst.Size,
				TraversalStatus: dbPathNodes.Dst.TraversalStatus,
				CopyStatus:      dbPathNodes.Dst.CopyStatus,
			}
		}

		items[path] = pathNodes
	}

	return ListChildrenDiffsResponse{
		Items: items,
		Pagination: PaginationInfo{
			Offset:       dbPagination.Offset,
			Limit:        dbPagination.Limit,
			Total:        dbPagination.Total,
			TotalFolders: dbPagination.TotalFolders,
			TotalFiles:   dbPagination.TotalFiles,
			HasMore:      dbPagination.HasMore,
		},
	}, nil
}

// ExcludeNode excludes nodes and queues their children for exclusion propagation
// Accepts either a single nodeID (for backward compatibility) or an ExclusionRequest
func (m *Manager) ExcludeNode(ctx context.Context, migrationID string, nodeID string) (*ExclusionResponse, error) {
	// For backward compatibility, treat single nodeID as a request with one node
	req := ExclusionRequest{
		NodeIDs: []string{nodeID},
	}
	return m.ExcludeNodes(ctx, migrationID, req)
}

// ExcludeNodes excludes nodes based on ExclusionRequest
func (m *Manager) ExcludeNodes(ctx context.Context, migrationID string, req ExclusionRequest) (*ExclusionResponse, error) {
	// Prepare path review context first to check review phase
	prc, err := m.preparePathReviewContext(ctx, migrationID)
	if err != nil {
		if err == ErrMigrationNotFound {
			return &ExclusionResponse{
				Success: false,
				Error:   "migration not found",
			}, err
		}
		return &ExclusionResponse{
			Success: false,
			Error:   err.Error(),
		}, err
	}

	// Exclusion is only allowed in traversal phase, not copy phase
	if prc.ReviewPhase == "copy" {
		return &ExclusionResponse{
			Success: false,
			Error:   "exclusion operations are not available in copy phase (exclusion only applies to traversal)",
		}, fmt.Errorf("exclusion operations are locked in copy phase")
	}
	if err != nil {
		if err == ErrMigrationNotFound {
			return &ExclusionResponse{
				Success: false,
				Error:   "migration not found",
			}, err
		}
		return &ExclusionResponse{
			Success: false,
			Error:   err.Error(),
		}, err
	}

	// Handle 'all' option
	if req.All {
		// Check if filter is specified
		if req.Filter != nil && req.Filter.Status == "failed" {
			// Mark all failed as excluded
			taskID := m.bgTaskMgr.StartTask(prc.MigrationID, BackgroundTaskTypeExclusionSweep)
			go func() {
				defer m.bgTaskMgr.CompleteTask(prc.MigrationID, taskID)
				if err := database.MarkAllFailedAsExcludedDuckDB(context.Background(), m.logger, prc.DuckDBConn); err != nil {
					m.logger.Error().
						Err(err).
						Str("migration_id", prc.MigrationID).
						Msg("failed to mark all failed items as excluded")
					m.bgTaskMgr.FailTask(prc.MigrationID, taskID, err)
					return
				}
				if err := m.markPathReviewChanges(migrationID, true); err != nil {
					m.logger.Warn().Err(err).Str("migration_id", migrationID).Msg("failed to mark path review changes")
				}
			}()
			return &ExclusionResponse{
				Success: true,
				TaskID:  taskID,
			}, nil
		}
		// For 'all' without filter, get all pending and failed items
		pendingPaths, err := database.GetAllNodesByStatusDuckDB(ctx, m.logger, prc.DuckDBConn, "pending")
		if err != nil {
			return &ExclusionResponse{
				Success: false,
				Error:   fmt.Sprintf("failed to get pending nodes: %v", err),
			}, err
		}
		failedPaths, err := database.GetAllNodesByStatusDuckDB(ctx, m.logger, prc.DuckDBConn, "failed")
		if err != nil {
			return &ExclusionResponse{
				Success: false,
				Error:   fmt.Sprintf("failed to get failed nodes: %v", err),
			}, err
		}
		// Combine and deduplicate
		allPaths := make(map[string]bool)
		for _, p := range pendingPaths {
			allPaths[p] = true
		}
		for _, p := range failedPaths {
			allPaths[p] = true
		}
		req.NodeIDs = make([]string, 0, len(allPaths))
		for p := range allPaths {
			req.NodeIDs = append(req.NodeIDs, p)
		}
	}

	// Process each node ID
	for _, nodeID := range req.NodeIDs {
		// Find node path from ULID
		nodePath, err := m.findNodePathFromID(ctx, prc, nodeID)
		if err != nil {
			m.logger.Warn().
				Err(err).
				Str("node_id", nodeID).
				Msg("failed to find node path, skipping")
			continue
		}

		// Mark immediate parent as excluded
		err = database.SetNodeExclusionDuckDB(ctx, m.logger, prc.DuckDBConn, nodePath, true)
		if err != nil {
			m.logger.Warn().
				Err(err).
				Str("node_path", nodePath).
				Msg("failed to set node exclusion, skipping")
			continue
		}

		// Trigger background propagation task
		taskID := m.bgTaskMgr.StartTaskWithPath(prc.MigrationID, BackgroundTaskTypeExclusionPropagate, nodePath)
		go func(path string) {
			defer m.bgTaskMgr.CompleteTask(prc.MigrationID, taskID)
			if err := database.PropagateExclusionDuckDB(context.Background(), m.logger, prc.DuckDBConn, path, true); err != nil {
				m.logger.Error().
					Err(err).
					Str("migration_id", prc.MigrationID).
					Str("node_path", path).
					Msg("failed to propagate exclusion")
				m.bgTaskMgr.FailTask(prc.MigrationID, taskID, err)
			}
		}(nodePath)
	}

	// Mark that user made changes during path review
	if err := m.markPathReviewChanges(migrationID, true); err != nil {
		m.logger.Warn().Err(err).Str("migration_id", migrationID).Msg("failed to mark path review changes")
	}

	return &ExclusionResponse{
		Success: true,
	}, nil
}

// UnexcludeNode unexcludes a node and queues its children for unexclusion propagation
// Accepts either a single nodeID (for backward compatibility) or an ExclusionRequest
func (m *Manager) UnexcludeNode(ctx context.Context, migrationID string, nodeID string) (*ExclusionResponse, error) {
	// For backward compatibility, treat single nodeID as a request with one node
	req := ExclusionRequest{
		NodeIDs: []string{nodeID},
	}
	return m.UnexcludeNodes(ctx, migrationID, req)
}

// UnexcludeNodes unexcludes nodes based on ExclusionRequest
func (m *Manager) UnexcludeNodes(ctx context.Context, migrationID string, req ExclusionRequest) (*ExclusionResponse, error) {
	// Prepare path review context first to check review phase
	prc, err := m.preparePathReviewContext(ctx, migrationID)
	if err != nil {
		if err == ErrMigrationNotFound {
			return &ExclusionResponse{
				Success: false,
				Error:   "migration not found",
			}, err
		}
		return &ExclusionResponse{
			Success: false,
			Error:   err.Error(),
		}, err
	}

	// Unexclusion is only allowed in traversal phase, not copy phase
	if prc.ReviewPhase == "copy" {
		return &ExclusionResponse{
			Success: false,
			Error:   "exclusion operations are not available in copy phase (exclusion only applies to traversal)",
		}, fmt.Errorf("exclusion operations are locked in copy phase")
	}
	if err != nil {
		if err == ErrMigrationNotFound {
			return &ExclusionResponse{
				Success: false,
				Error:   "migration not found",
			}, err
		}
		return &ExclusionResponse{
			Success: false,
			Error:   err.Error(),
		}, err
	}

	// Handle 'all' option
	if req.All {
		// Get all excluded items (exclusion_explicit or exclusion_inherited)
		explicitPaths, err := database.GetAllNodesByStatusDuckDB(ctx, m.logger, prc.DuckDBConn, "exclusion_explicit")
		if err != nil {
			return &ExclusionResponse{
				Success: false,
				Error:   fmt.Sprintf("failed to get exclusion_explicit nodes: %v", err),
			}, err
		}
		inheritedPaths, err := database.GetAllNodesByStatusDuckDB(ctx, m.logger, prc.DuckDBConn, "exclusion_inherited")
		if err != nil {
			return &ExclusionResponse{
				Success: false,
				Error:   fmt.Sprintf("failed to get exclusion_inherited nodes: %v", err),
			}, err
		}
		// Combine and deduplicate
		allPaths := make(map[string]bool)
		for _, p := range explicitPaths {
			allPaths[p] = true
		}
		for _, p := range inheritedPaths {
			allPaths[p] = true
		}
		req.NodeIDs = make([]string, 0, len(allPaths))
		for p := range allPaths {
			req.NodeIDs = append(req.NodeIDs, p)
		}
	}

	// Process each node ID
	for _, nodeID := range req.NodeIDs {
		// Find node path from ULID
		nodePath, err := m.findNodePathFromID(ctx, prc, nodeID)
		if err != nil {
			m.logger.Warn().
				Err(err).
				Str("node_id", nodeID).
				Msg("failed to find node path, skipping")
			continue
		}

		// Mark immediate parent as unexcluded (set to pending)
		err = database.SetNodeExclusionDuckDB(ctx, m.logger, prc.DuckDBConn, nodePath, false)
		if err != nil {
			m.logger.Warn().
				Err(err).
				Str("node_path", nodePath).
				Msg("failed to set node unexclusion, skipping")
			continue
		}

		// Trigger background propagation task
		taskID := m.bgTaskMgr.StartTaskWithPath(prc.MigrationID, BackgroundTaskTypeUnexclusionPropagate, nodePath)
		go func(path string) {
			defer m.bgTaskMgr.CompleteTask(prc.MigrationID, taskID)
			if err := database.PropagateExclusionDuckDB(context.Background(), m.logger, prc.DuckDBConn, path, false); err != nil {
				m.logger.Error().
					Err(err).
					Str("migration_id", prc.MigrationID).
					Str("node_path", path).
					Msg("failed to propagate unexclusion")
				m.bgTaskMgr.FailTask(prc.MigrationID, taskID, err)
			}
		}(nodePath)
	}

	// Mark that user made changes during path review
	if err := m.markPathReviewChanges(migrationID, true); err != nil {
		m.logger.Warn().Err(err).Str("migration_id", migrationID).Msg("failed to mark path review changes")
	}

	return &ExclusionResponse{
		Success: true,
	}, nil
}

// TriggerRetrySweep triggers a retry sweep for a migration
// It first runs ETL from DuckDB to BoltDB to sync path review changes, then starts the retry sweep
func (m *Manager) TriggerRetrySweep(ctx context.Context, migrationID string, config SweepConfigRequest) (SweepResponse, error) {
	// Check phase lock - retry sweep is only allowed in traversal phase
	if err := m.checkPhaseLock(migrationID, "retrySweep"); err != nil {
		return SweepResponse{
			Success: false,
			Error:   err.Error(),
		}, err
	}

	// Check for running background tasks (prevent overlapping operations)
	runningTasks := m.bgTaskMgr.GetRunningTasks(migrationID)
	if len(runningTasks) > 0 {
		return SweepResponse{
			Success: false,
			Error:   fmt.Sprintf("cannot start retry sweep: there are %d running background tasks. Please wait for them to complete", len(runningTasks)),
		}, fmt.Errorf("cannot start retry sweep: there are %d running background tasks", len(runningTasks))
	}

	// Check if retry sweep is already running
	if m.bgTaskMgr.HasRunningTask(migrationID, BackgroundTaskTypeRetrySweep) {
		return SweepResponse{
			Success: false,
			Error:   "retry sweep is already running for this migration",
		}, fmt.Errorf("retry sweep is already running")
	}

	// Get metadata to find config path
	metaMgr := metadata.NewManager(m.cfg.Runtime.DataDir)
	meta, err := metaMgr.GetMigrationMetadata(migrationID)
	if err != nil {
		return SweepResponse{
			Success: false,
			Error:   "migration not found",
		}, ErrMigrationNotFound
	}

	// Check if config path exists
	configPath := meta.ConfigPath
	if configPath == "" {
		// Try to derive from migration ID
		dbPath, err := database.ResolveDatabasePath(m.cfg.Runtime.DataDir, "", migrationID)
		if err != nil {
			return SweepResponse{
				Success: false,
				Error:   "failed to resolve database path",
			}, fmt.Errorf("failed to resolve database path: %w", err)
		}
		configPath = database.ConfigPathFromDatabasePath(dbPath)
	}

	// Check if config file exists
	if _, err := os.Stat(configPath); os.IsNotExist(err) {
		return SweepResponse{
			Success: false,
			Error:   fmt.Sprintf("migration config file not found: %s", configPath),
		}, fmt.Errorf("migration config file not found: %s", configPath)
	}

	// Derive database path from config path
	dbPath := strings.TrimSuffix(configPath, ".yaml") + ".db"
	if dbPath == ".db" {
		dbPath, err = database.ResolveDatabasePath(m.cfg.Runtime.DataDir, "", migrationID)
		if err != nil {
			return SweepResponse{
				Success: false,
				Error:   "failed to resolve database path",
			}, fmt.Errorf("failed to resolve database path: %w", err)
		}
	}

	// Verify DuckDB exists (migration should be in Awaiting-Path-Review)
	duckdbExists, err := database.CheckDuckDBExists(dbPath)
	if err != nil {
		return SweepResponse{
			Success: false,
			Error:   "failed to check DuckDB existence",
		}, fmt.Errorf("failed to check DuckDB existence: %w", err)
	}
	if !duckdbExists {
		return SweepResponse{
			Success: false,
			Error:   "DuckDB file not found. Migration must be in 'Awaiting-Path-Review' status before retry sweep",
		}, fmt.Errorf("DuckDB file not found")
	}

	// Check if there are pending retries
	pendingWork, err := m.CheckPendingWork(ctx, migrationID)
	if err != nil {
		// If we can't check, log warning but continue (DuckDB might not be accessible)
		m.logger.Warn().
			Err(err).
			Str("migration_id", migrationID).
			Msg("failed to check pending work, proceeding anyway")
	} else if !pendingWork.HasPendingRetries {
		return SweepResponse{
			Success: false,
			Error:   "no pending retries found. Nothing to retry",
		}, fmt.Errorf("no pending retries found")
	}

	// Force close all DB connections before ETL (ETL will open its own instances)
	// This ensures files are not locked when ETL tries to open them

	// Close DuckDB connection
	if err := m.migrationsMgr.KillDuckDBConnection(migrationID); err != nil {
		m.logger.Warn().
			Err(err).
			Str("migration_id", migrationID).
			Msg("failed to close DuckDB connection (may not be open), proceeding anyway")
	} else {
		m.logger.Info().
			Str("migration_id", migrationID).
			Msg("closed DuckDB connection before retry sweep ETL")
	}

	// Close BoltDB connection
	if err := m.migrationsMgr.CloseDB(migrationID); err != nil {
		m.logger.Warn().
			Err(err).
			Str("migration_id", migrationID).
			Msg("failed to close BoltDB connection (may not be open), proceeding anyway")
	} else {
		m.logger.Info().
			Str("migration_id", migrationID).
			Msg("closed BoltDB connection before retry sweep ETL")
	}

	// Check if Spectra override config exists
	overridePath, exists, _ := services.LoadSpectraConfigOverride(m.cfg.Runtime.DataDir, migrationID)
	var spectraConfigPath string
	if exists {
		spectraConfigPath = overridePath
	}

	// Load YAML config to update checkpoint state (without adapters, just for state)
	yamlCfg, err := migration.LoadMigrationConfig(configPath)
	if err != nil {
		return SweepResponse{
			Success: false,
			Error:   fmt.Sprintf("failed to load migration config: %v", err),
		}, fmt.Errorf("failed to load migration config: %w", err)
	}

	// Set MaxKnownDepth (default to -1 for auto-detect if not specified)
	maxKnownDepth := config.MaxKnownDepth
	if maxKnownDepth == 0 {
		maxKnownDepth = -1
	}

	// Step 1: Update status to Preparing-For-Retry when retry is triggered
	// This signals that we're preparing for a retry sweep
	// The ETL function will update to ETL-Duck-To-Bolt-In-Progress when ETL starts
	// Then to Filters-Set when ETL completes
	yamlCfg.State.Status = "Preparing-For-Retry"
	if err := migration.SaveMigrationConfig(configPath, yamlCfg); err != nil {
		return SweepResponse{
			Success: false,
			Error:   fmt.Sprintf("failed to update status to Preparing-For-Retry: %v", err),
		}, fmt.Errorf("failed to save migration config: %w", err)
	}

	m.logger.Info().
		Str("migration_id", migrationID).
		Str("status", "Preparing-For-Retry").
		Int("max_known_depth", maxKnownDepth).
		Msg("updated status to Preparing-For-Retry (retry sweep triggered)")

	// Get or create migration record
	record := m.migrationsMgr.GetRecord(migrationID)
	if record == nil {
		// Create a temporary record for ETL tracking
		record = &migrations.MigrationRecord{
			ID: migrationID,
		}
	}

	// Retry sweep is like running discovery phase again - it's not a background task
	// but we'll track it for UI visibility
	taskID := m.bgTaskMgr.StartTask(migrationID, BackgroundTaskTypeRetrySweep)

	// Trigger ETL from DuckDB to BoltDB in background
	// After ETL completes, run retry sweep, then ETL back to DuckDB
	go func() {
		defer func() {
			// Handle panic recovery
			if r := recover(); r != nil {
				m.bgTaskMgr.FailTask(migrationID, taskID, fmt.Errorf("panic: %v", r))
				panic(r) // Re-panic
			}
		}()

		m.migrationsMgr.RunETLFromDuckToBolt(record, dbPath, configPath, func() {
			// ETL from DuckDB to BoltDB completed successfully
			m.logger.Info().
				Str("migration_id", migrationID).
				Msg("ETL from DuckDB to BoltDB completed, starting retry sweep")

			// Update task progress
			m.bgTaskMgr.UpdateTaskProgress(migrationID, taskID, map[string]any{
				"stage": "running_sweep",
			})

			// Try to acquire shared adapters if both use same Spectra config
			srcAdapter, dstAdapter, shared, err := m.acquireSharedSpectraAdaptersForSweep(yamlCfg.Services.Source, yamlCfg.Services.Destination, spectraConfigPath)
			if err != nil {
				m.logger.Error().
					Err(err).
					Str("migration_id", migrationID).
					Msg("failed to acquire shared adapters for retry sweep")
				m.bgTaskMgr.FailTask(migrationID, taskID, fmt.Errorf("failed to acquire shared adapters: %w", err))
				return
			}

			// If not shared (different configs or not both Spectra), acquire separately
			if !shared {
				srcAdapter, err = m.acquireAdapterFromYAMLConfigForSweep(yamlCfg.Services.Source, spectraConfigPath)
				if err != nil {
					m.logger.Error().
						Err(err).
						Str("migration_id", migrationID).
						Msg("failed to acquire source adapter for retry sweep")
					m.bgTaskMgr.FailTask(migrationID, taskID, fmt.Errorf("failed to acquire source adapter: %w", err))
					return
				}

				dstAdapter, err = m.acquireAdapterFromYAMLConfigForSweep(yamlCfg.Services.Destination, spectraConfigPath)
				if err != nil {
					// Close source adapter if we got it
					if srcAdapter != nil {
						if closer, ok := srcAdapter.(interface{ Close() error }); ok {
							_ = closer.Close()
						}
					}
					m.logger.Error().
						Err(err).
						Str("migration_id", migrationID).
						Msg("failed to acquire destination adapter for retry sweep")
					m.bgTaskMgr.FailTask(migrationID, taskID, fmt.Errorf("failed to acquire destination adapter: %w", err))
					return
				}
			}

			// Load full config with adapters for sweep execution
			migrationCfg, err := migration.LoadMigrationConfigFromYAML(configPath, srcAdapter, dstAdapter)
			if err != nil {
				m.logger.Error().
					Err(err).
					Str("migration_id", migrationID).
					Msg("failed to load migration config with adapters for retry sweep")
				m.bgTaskMgr.FailTask(migrationID, taskID, fmt.Errorf("failed to load migration config: %w", err))
				return
			}

			// Open the new BoltDB instance created by ETL
			// ETL created a fresh BoltDB, so we need to open it
			dbInstance, _, err := migration.SetupDatabase(migration.DatabaseConfig{
				Path:           dbPath,
				RemoveExisting: false,
			})
			if err != nil {
				m.logger.Error().
					Err(err).
					Str("migration_id", migrationID).
					Str("db_path", dbPath).
					Msg("failed to open BoltDB after ETL")
				m.bgTaskMgr.FailTask(migrationID, taskID, fmt.Errorf("failed to open BoltDB: %w", err))
				return
			}
			defer dbInstance.Close()

			// Build sweep config with defaults and overrides
			sweepCfg := migration.SweepConfig{
				BoltDB:        dbInstance,
				SrcAdapter:    migrationCfg.Source.Adapter,
				DstAdapter:    migrationCfg.Destination.Adapter,
				WorkerCount:   m.selectWorkerCountForSweep(config.WorkerCount),
				MaxRetries:    m.selectMaxRetriesForSweep(config.MaxRetries),
				MaxKnownDepth: maxKnownDepth,
			}

			// Apply optional config overrides
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
				sweepCfg.SkipListener = true // Default to skip listener
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

			// Use background context for shutdown
			sweepCfg.ShutdownContext = context.Background()

			// Run retry sweep (this is like running discovery phase again)
			m.logger.Info().
				Str("migration_id", migrationID).
				Msg("starting retry sweep (discovery phase)")

			stats, err := migration.RunRetrySweep(sweepCfg)
			if err != nil {
				m.logger.Error().
					Err(err).
					Str("migration_id", migrationID).
					Msg("retry sweep failed")
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

			// Update task progress
			m.bgTaskMgr.UpdateTaskProgress(migrationID, taskID, map[string]any{
				"stage": "etl_to_duckdb",
			})

			// After retry sweep completes, run ETL from BoltDB to DuckDB
			// This creates a new DuckDB with the updated data
			m.logger.Info().
				Str("migration_id", migrationID).
				Msg("retry sweep completed, running ETL from BoltDB to DuckDB")

			// Update task progress
			m.bgTaskMgr.UpdateTaskProgress(migrationID, taskID, map[string]any{
				"stage": "etl_to_duckdb",
			})

			// Run ETL from BoltDB to DuckDB (creates new DuckDB)
			// This will update status to Awaiting-Path-Review and open DuckDB connection
			m.migrationsMgr.RunETLFromBoltToDuck(record, dbPath, configPath, dbInstance)

			// After ETL completes, open DuckDB connection for path review
			// runETL already handles opening the connection, but let's ensure it's open
			duckdbPath := database.GetDuckDBPath(dbPath)
			duckdbPool := m.migrationsMgr.GetDuckDBPool()
			if duckdbPool != nil {
				duckdbConn, err := duckdbPool.OpenDuckDB(migrationID, duckdbPath)
				if err != nil {
					m.logger.Warn().
						Err(err).
						Str("migration_id", migrationID).
						Msg("failed to open DuckDB connection after ETL (may already be open)")
				} else {
					m.logger.Info().
						Str("migration_id", migrationID).
						Msg("opened DuckDB connection for path review")
					_ = duckdbConn // Connection is managed by pool
				}
			}

			// Ensure YAML config status is set to Awaiting-Path-Review
			// runETL should have done this, but let's verify
			updatedYamlCfg, err := migration.LoadMigrationConfig(configPath)
			if err == nil {
				if updatedYamlCfg.State.Status != "Awaiting-Path-Review" {
					updatedYamlCfg.State.Status = "Awaiting-Path-Review"
					if err := migration.SaveMigrationConfig(configPath, updatedYamlCfg); err != nil {
						m.logger.Warn().
							Err(err).
							Str("migration_id", migrationID).
							Msg("failed to update status to Awaiting-Path-Review")
					} else {
						m.logger.Info().
							Str("migration_id", migrationID).
							Msg("updated status to Awaiting-Path-Review after retry sweep")
					}
				}
			}

			// Clear path review changes flag on successful sweep completion
			if err := m.markPathReviewChanges(migrationID, false); err != nil {
				m.logger.Warn().Err(err).Str("migration_id", migrationID).Msg("failed to clear path review changes flag")
			}

			// Mark task as completed
			m.bgTaskMgr.CompleteTask(migrationID, taskID)
		})
	}()

	return SweepResponse{
		Success: true,
		Message: "Retry sweep started",
	}, nil
}

// Helper methods for selecting defaults
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

// acquireSharedSpectraAdaptersForSweep checks if both services use the same Spectra config and shares a session if they do
// Returns (srcAdapter, dstAdapter, sharedSession, error)
// If not shareable (different configs or not both Spectra), returns (nil, nil, false, nil)
func (m *Manager) acquireSharedSpectraAdaptersForSweep(srcCfg, dstCfg migration.ServiceConfigYAML, spectraConfigOverridePath string) (fstypes.FSAdapter, fstypes.FSAdapter, bool, error) {
	// Check if both are Spectra
	if strings.ToLower(srcCfg.Type) != "spectra" || strings.ToLower(dstCfg.Type) != "spectra" {
		return nil, nil, false, nil
	}

	// Helper to get config path for a service
	getConfigPath := func(serviceCfg migration.ServiceConfigYAML) (string, error) {
		if spectraConfigOverridePath != "" {
			return spectraConfigOverridePath, nil
		}

		// Try to get from service definition
		def, err := m.serviceMgr.GetServiceDefinition(serviceCfg.Name)
		if err == nil && def.Spectra != nil {
			return def.Spectra.ConfigPath, nil
		}

		// Try to find by world
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

	// Get config paths for both services
	srcConfigPath, err := getConfigPath(srcCfg)
	if err != nil {
		return nil, nil, false, fmt.Errorf("failed to get source config path: %w", err)
	}

	dstConfigPath, err := getConfigPath(dstCfg)
	if err != nil {
		return nil, nil, false, fmt.Errorf("failed to get destination config path: %w", err)
	}

	// If config paths are different, can't share
	if srcConfigPath != dstConfigPath {
		return nil, nil, false, nil
	}

	m.logger.Info().
		Str("config_path", srcConfigPath).
		Msg("source and destination use same Spectra config, sharing SDK session for sweep")

	// Same config path - create ONE shared SDK session
	spectraFS, err := sdk.New(srcConfigPath)
	if err != nil {
		return nil, nil, false, fmt.Errorf("failed to create shared SpectraFS session: %w", err)
	}

	// Create source adapter
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

	srcAdapter, err := fslib.NewSpectraFS(spectraFS, srcRootID, srcWorld)
	if err != nil {
		_ = spectraFS.Close()
		return nil, nil, false, fmt.Errorf("failed to create source adapter: %w", err)
	}

	// Create destination adapter from SAME SDK session
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

	dstAdapter, err := fslib.NewSpectraFS(spectraFS, dstRootID, dstWorld)
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

// acquireAdapterFromYAMLConfigForSweep acquires an adapter based on YAML service configuration for sweep operations
func (m *Manager) acquireAdapterFromYAMLConfigForSweep(serviceCfg migration.ServiceConfigYAML, spectraConfigOverridePath string) (fstypes.FSAdapter, error) {
	serviceType := strings.ToLower(serviceCfg.Type)
	switch serviceType {
	case "spectra":
		// Use override config if provided, otherwise try to extract from service name
		configPath := spectraConfigOverridePath
		if configPath == "" {
			// Try to get original config path from service name
			def, err := m.serviceMgr.GetServiceDefinition(serviceCfg.Name)
			if err == nil && def.Spectra != nil {
				configPath = def.Spectra.ConfigPath
			} else {
				// Try to find by world if name lookup fails
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

		// Extract world from service name or use default
		world := "primary"
		if strings.Contains(strings.ToLower(serviceCfg.Name), "s1") {
			world = "s1"
		} else {
			// Try to get world from service definition
			def, err := m.serviceMgr.GetServiceDefinition(serviceCfg.Name)
			if err == nil && def.Spectra != nil {
				world = def.Spectra.World
			} else {
				// Try to find by world
				def, err := m.serviceMgr.GetServiceDefinitionByWorld(world)
				if err == nil && def.Spectra != nil {
					world = def.Spectra.World
				}
			}
		}

		adapter, err := fslib.NewSpectraFS(spectraFS, rootID, world)
		if err != nil {
			_ = spectraFS.Close()
			return nil, fmt.Errorf("failed to create SpectraFS adapter: %w", err)
		}

		return adapter, nil

	case "local":
		// For local services, use RootPath
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

// markPathReviewChanges updates the HasPathReviewChanges flag in migration metadata
// This flag tracks if the user made changes (exclusions, retries) during path review
// Set to true when user makes changes, false when sweeps complete successfully
func (m *Manager) markPathReviewChanges(migrationID string, hasChanges bool) error {
	metaMgr := metadata.NewManager(m.cfg.Runtime.DataDir)
	meta, err := metaMgr.GetMigrationMetadata(migrationID)
	if err != nil {
		// If metadata doesn't exist yet, create it
		meta = metadata.MigrationMetadata{
			ID:                   migrationID,
			Name:                 migrationID,
			HasPathReviewChanges: hasChanges,
		}
	} else {
		// Update existing metadata
		meta.HasPathReviewChanges = hasChanges
	}

	return metaMgr.UpdateMigrationMetadata(meta)
}

// CheckPendingWork checks if there are pending exclusions or retries for a migration
func (m *Manager) CheckPendingWork(ctx context.Context, migrationID string) (PendingWorkResponse, error) {
	// Prepare path review context (gets DuckDB connection)
	prc, err := m.preparePathReviewContext(ctx, migrationID)
	if err != nil {
		// If DuckDB is not available (migration not in path review phase), return zero counts
		// This is expected for migrations that haven't reached path review yet
		if err == ErrMigrationNotFound {
			return PendingWorkResponse{}, err
		}
		// For other errors (like DuckDB not available), return zero counts
		// This allows the UI to still show the migration even if DuckDB isn't ready
		return PendingWorkResponse{
			HasPendingRetries:    false,
			HasPathReviewChanges: false,
			PendingRetriesCount:  0,
		}, nil
	}

	// Count pending retries from DuckDB
	retriesCount, err := database.CountPendingRetriesDuckDB(ctx, m.logger, prc.DuckDBConn)
	if err != nil {
		return PendingWorkResponse{}, fmt.Errorf("failed to count pending retries: %w", err)
	}

	// Check if user made changes during path review (from metadata)
	metaMgr := metadata.NewManager(m.cfg.Runtime.DataDir)
	meta, err := metaMgr.GetMigrationMetadata(migrationID)
	hasUnsavedChanges := false
	if err == nil {
		hasUnsavedChanges = meta.HasPathReviewChanges
	}

	return PendingWorkResponse{
		HasPendingRetries:    retriesCount > 0,
		HasPathReviewChanges: hasUnsavedChanges,
		PendingRetriesCount:  retriesCount,
	}, nil
}

// MarkNodesForRetry marks nodes for retry based on MarkRetryRequest
// This works for both traversal and copy phase retries - the phase is determined by the review context
func (m *Manager) MarkNodesForRetry(ctx context.Context, migrationID string, req MarkRetryRequest) (*MarkRetryResponse, error) {
	// Prepare path review context
	prc, err := m.preparePathReviewContext(ctx, migrationID)
	if err != nil {
		if err == ErrMigrationNotFound {
			return &MarkRetryResponse{
				Success: false,
				Error:   "migration not found",
			}, err
		}
		return &MarkRetryResponse{
			Success: false,
			Error:   err.Error(),
		}, err
	}

	// Handle 'all' option
	if req.All {
		// Retry all failed items
		taskID := m.bgTaskMgr.StartTask(prc.MigrationID, BackgroundTaskTypeRetryAll)
		go func() {
			defer m.bgTaskMgr.CompleteTask(prc.MigrationID, taskID)
			if err := database.RetryAllFailedDuckDB(context.Background(), m.logger, prc.DuckDBConn); err != nil {
				m.logger.Error().
					Err(err).
					Str("migration_id", prc.MigrationID).
					Msg("failed to retry all failed items")
				m.bgTaskMgr.FailTask(prc.MigrationID, taskID, err)
				return
			}
			if err := m.markPathReviewChanges(migrationID, true); err != nil {
				m.logger.Warn().Err(err).Str("migration_id", migrationID).Msg("failed to mark path review changes")
			}
		}()
		return &MarkRetryResponse{
			Success: true,
			TaskID:  taskID,
		}, nil
	}

	// Process each node ID
	for _, nodeID := range req.NodeIDs {
		// Find node path from ULID
		nodePath, err := m.findNodePathFromID(ctx, prc, nodeID)
		if err != nil {
			m.logger.Warn().
				Err(err).
				Str("node_id", nodeID).
				Msg("failed to find node path, skipping")
			continue
		}

		// Mark node for retry in DuckDB
		err = database.MarkNodeForRetryDuckDB(ctx, m.logger, prc.DuckDBConn, nodePath)
		if err != nil {
			m.logger.Warn().
				Err(err).
				Str("node_path", nodePath).
				Msg("failed to mark node for retry, skipping")
			continue
		}
	}

	// Mark that user made changes during path review
	if err := m.markPathReviewChanges(migrationID, true); err != nil {
		m.logger.Warn().Err(err).Str("migration_id", migrationID).Msg("failed to mark path review changes")
	}

	return &MarkRetryResponse{
		Success: true,
	}, nil
}

// RetryAllFailed marks all failed items for retry in a background task
func (m *Manager) RetryAllFailed(ctx context.Context, migrationID string) (*MarkRetryResponse, error) {
	// Prepare path review context
	prc, err := m.preparePathReviewContext(ctx, migrationID)
	if err != nil {
		if err == ErrMigrationNotFound {
			return &MarkRetryResponse{
				Success: false,
				Error:   "migration not found",
			}, err
		}
		return &MarkRetryResponse{
			Success: false,
			Error:   err.Error(),
		}, err
	}

	// Start background task
	taskID := m.bgTaskMgr.StartTask(prc.MigrationID, BackgroundTaskTypeRetryAll)
	go func() {
		defer m.bgTaskMgr.CompleteTask(prc.MigrationID, taskID)
		if err := database.RetryAllFailedDuckDB(context.Background(), m.logger, prc.DuckDBConn); err != nil {
			m.logger.Error().
				Err(err).
				Str("migration_id", prc.MigrationID).
				Msg("failed to retry all failed items")
			m.bgTaskMgr.FailTask(prc.MigrationID, taskID, err)
			return
		}

		// Mark that user made changes during path review
		if err := m.markPathReviewChanges(migrationID, true); err != nil {
			m.logger.Warn().Err(err).Str("migration_id", migrationID).Msg("failed to mark path review changes")
		}
	}()

	return &MarkRetryResponse{
		Success: true,
	}, nil
}

// MarkAllFailedAsExcluded marks all failed items as excluded in a background task
func (m *Manager) MarkAllFailedAsExcluded(ctx context.Context, migrationID string) (*ExclusionResponse, error) {
	// Prepare path review context
	prc, err := m.preparePathReviewContext(ctx, migrationID)
	if err != nil {
		if err == ErrMigrationNotFound {
			return &ExclusionResponse{
				Success: false,
				Error:   "migration not found",
			}, err
		}
		return &ExclusionResponse{
			Success: false,
			Error:   err.Error(),
		}, err
	}

	// Start background task
	taskID := m.bgTaskMgr.StartTask(prc.MigrationID, BackgroundTaskTypeExclusionSweep)
	go func() {
		defer m.bgTaskMgr.CompleteTask(prc.MigrationID, taskID)
		if err := database.MarkAllFailedAsExcludedDuckDB(context.Background(), m.logger, prc.DuckDBConn); err != nil {
			m.logger.Error().
				Err(err).
				Str("migration_id", prc.MigrationID).
				Msg("failed to mark all failed items as excluded")
			m.bgTaskMgr.FailTask(prc.MigrationID, taskID, err)
			return
		}

		// Mark that user made changes during path review
		if err := m.markPathReviewChanges(migrationID, true); err != nil {
			m.logger.Warn().Err(err).Str("migration_id", migrationID).Msg("failed to mark path review changes")
		}
	}()

	return &ExclusionResponse{
		Success: true,
	}, nil
}

// UnmarkNodeForRetry unmarks a pending node for retry (changes status back to failed)
func (m *Manager) UnmarkNodeForRetry(ctx context.Context, migrationID string, nodeID string) (*MarkRetryResponse, error) {
	// Prepare path review context
	prc, err := m.preparePathReviewContext(ctx, migrationID)
	if err != nil {
		if err == ErrMigrationNotFound {
			return &MarkRetryResponse{
				Success: false,
				Error:   "migration not found",
			}, err
		}
		return &MarkRetryResponse{
			Success: false,
			Error:   err.Error(),
		}, err
	}

	// Find node path from ULID
	nodePath, err := m.findNodePathFromID(ctx, prc, nodeID)
	if err != nil {
		return &MarkRetryResponse{
			Success: false,
			Error:   fmt.Sprintf("node not found: %v", err),
		}, err
	}

	// Unmark node for retry in DuckDB
	err = database.UnmarkNodeForRetryDuckDB(ctx, m.logger, prc.DuckDBConn, nodePath)
	if err != nil {
		return &MarkRetryResponse{
			Success: false,
			Error:   err.Error(),
		}, err
	}

	// Mark that user made changes during path review
	if err := m.markPathReviewChanges(migrationID, true); err != nil {
		m.logger.Warn().Err(err).Str("migration_id", migrationID).Msg("failed to mark path review changes")
	}

	return &MarkRetryResponse{
		Success: true,
	}, nil
}

// MarkNodesForRetryDiscovery marks nodes for discovery/traversal phase retry
// Updates traversal_status from 'failed' to 'pending'
func (m *Manager) MarkNodesForRetryDiscovery(ctx context.Context, migrationID string, req MarkRetryRequest) (*MarkRetryResponse, error) {
	// Prepare path review context
	prc, err := m.preparePathReviewContext(ctx, migrationID)
	if err != nil {
		if err == ErrMigrationNotFound {
			return &MarkRetryResponse{
				Success: false,
				Error:   "migration not found",
			}, err
		}
		return &MarkRetryResponse{
			Success: false,
			Error:   err.Error(),
		}, err
	}

	// Process each node ID
	for _, nodeID := range req.NodeIDs {
		// Find node path from ULID
		nodePath, err := m.findNodePathFromID(ctx, prc, nodeID)
		if err != nil {
			m.logger.Warn().
				Err(err).
				Str("node_id", nodeID).
				Msg("failed to find node path, skipping")
			continue
		}

		// Mark node for discovery retry in DuckDB (updates traversal_status)
		err = database.MarkNodeForRetryDuckDB(ctx, m.logger, prc.DuckDBConn, nodePath)
		if err != nil {
			m.logger.Warn().
				Err(err).
				Str("node_path", nodePath).
				Msg("failed to mark node for discovery retry, skipping")
			continue
		}
	}

	// Mark that user made changes during path review
	if err := m.markPathReviewChanges(migrationID, true); err != nil {
		m.logger.Warn().Err(err).Str("migration_id", migrationID).Msg("failed to mark path review changes")
	}

	return &MarkRetryResponse{
		Success: true,
	}, nil
}

// MarkNodesForRetryCopy marks nodes for copy phase retry
// Updates copy_status from 'failed' to 'pending' (only src nodes have copy_status)
func (m *Manager) MarkNodesForRetryCopy(ctx context.Context, migrationID string, req MarkRetryRequest) (*MarkRetryResponse, error) {
	// Prepare path review context
	prc, err := m.preparePathReviewContext(ctx, migrationID)
	if err != nil {
		if err == ErrMigrationNotFound {
			return &MarkRetryResponse{
				Success: false,
				Error:   "migration not found",
			}, err
		}
		return &MarkRetryResponse{
			Success: false,
			Error:   err.Error(),
		}, err
	}

	// Process each node ID
	for _, nodeID := range req.NodeIDs {
		// Find node path from ULID
		nodePath, err := m.findNodePathFromID(ctx, prc, nodeID)
		if err != nil {
			m.logger.Warn().
				Err(err).
				Str("node_id", nodeID).
				Msg("failed to find node path, skipping")
			continue
		}

		// Mark node for copy retry in DuckDB (updates copy_status, src nodes only)
		err = database.MarkNodeForRetryCopyDuckDB(ctx, m.logger, prc.DuckDBConn, nodePath)
		if err != nil {
			m.logger.Warn().
				Err(err).
				Str("node_path", nodePath).
				Msg("failed to mark node for copy retry, skipping")
			continue
		}
	}

	// Mark that user made changes during path review
	if err := m.markPathReviewChanges(migrationID, true); err != nil {
		m.logger.Warn().Err(err).Str("migration_id", migrationID).Msg("failed to mark path review changes")
	}

	return &MarkRetryResponse{
		Success: true,
	}, nil
}

// UnmarkNodeForRetryDiscovery unmarks a node for discovery/traversal phase retry
// Updates traversal_status from 'pending' back to 'failed'
func (m *Manager) UnmarkNodeForRetryDiscovery(ctx context.Context, migrationID string, nodeID string) (*MarkRetryResponse, error) {
	// Prepare path review context
	prc, err := m.preparePathReviewContext(ctx, migrationID)
	if err != nil {
		if err == ErrMigrationNotFound {
			return &MarkRetryResponse{
				Success: false,
				Error:   "migration not found",
			}, err
		}
		return &MarkRetryResponse{
			Success: false,
			Error:   err.Error(),
		}, err
	}

	// Find node path from ULID
	nodePath, err := m.findNodePathFromID(ctx, prc, nodeID)
	if err != nil {
		return &MarkRetryResponse{
			Success: false,
			Error:   fmt.Sprintf("node not found: %v", err),
		}, err
	}

	// Unmark node for discovery retry in DuckDB (updates traversal_status)
	err = database.UnmarkNodeForRetryDuckDB(ctx, m.logger, prc.DuckDBConn, nodePath)
	if err != nil {
		return &MarkRetryResponse{
			Success: false,
			Error:   err.Error(),
		}, err
	}

	// Mark that user made changes during path review
	if err := m.markPathReviewChanges(migrationID, true); err != nil {
		m.logger.Warn().Err(err).Str("migration_id", migrationID).Msg("failed to mark path review changes")
	}

	return &MarkRetryResponse{
		Success: true,
	}, nil
}

// UnmarkNodeForRetryCopy unmarks a node for copy phase retry
// Updates copy_status from 'pending' back to 'failed' (only src nodes have copy_status)
func (m *Manager) UnmarkNodeForRetryCopy(ctx context.Context, migrationID string, nodeID string) (*MarkRetryResponse, error) {
	// Prepare path review context
	prc, err := m.preparePathReviewContext(ctx, migrationID)
	if err != nil {
		if err == ErrMigrationNotFound {
			return &MarkRetryResponse{
				Success: false,
				Error:   "migration not found",
			}, err
		}
		return &MarkRetryResponse{
			Success: false,
			Error:   err.Error(),
		}, err
	}

	// Find node path from ULID
	nodePath, err := m.findNodePathFromID(ctx, prc, nodeID)
	if err != nil {
		return &MarkRetryResponse{
			Success: false,
			Error:   fmt.Sprintf("node not found: %v", err),
		}, err
	}

	// Unmark node for copy retry in DuckDB (updates copy_status, src nodes only)
	err = database.UnmarkNodeForRetryCopyDuckDB(ctx, m.logger, prc.DuckDBConn, nodePath)
	if err != nil {
		return &MarkRetryResponse{
			Success: false,
			Error:   err.Error(),
		}, err
	}

	// Mark that user made changes during path review
	if err := m.markPathReviewChanges(migrationID, true); err != nil {
		m.logger.Warn().Err(err).Str("migration_id", migrationID).Msg("failed to mark path review changes")
	}

	return &MarkRetryResponse{
		Success: true,
	}, nil
}

// GetPathReviewStats returns statistics for path review
func (m *Manager) GetPathReviewStats(ctx context.Context, migrationID string) (*PathReviewStats, error) {
	// Prepare path review context
	prc, err := m.preparePathReviewContext(ctx, migrationID)
	if err != nil {
		if err == ErrMigrationNotFound {
			return nil, err
		}
		return nil, err
	}

	// Get stats from DuckDB
	stats, err := database.GetPathReviewStatsDuckDB(ctx, m.logger, prc.DuckDBConn)
	if err != nil {
		return nil, fmt.Errorf("failed to get path review stats: %w", err)
	}

	return &PathReviewStats{
		TraversalStatusCounts: stats.TraversalStatusCounts,
		CopyStatusCounts:      stats.CopyStatusCounts,
		FoldersCount:          stats.FoldersCount,
		FilesCount:            stats.FilesCount,
		FoldersRatio:          stats.FoldersRatio,
		FilesRatio:            stats.FilesRatio,
		TotalFileSize: FileSizeStats{
			Src: stats.TotalFileSize.Src,
			Dst: stats.TotalFileSize.Dst,
		},
	}, nil
}

// GetBackgroundTasks returns all background tasks for a migration
func (m *Manager) GetBackgroundTasks(ctx context.Context, migrationID string) ([]BackgroundTask, error) {
	return m.bgTaskMgr.GetTasks(migrationID), nil
}

// GetRunningBackgroundTasks returns only running background tasks for a migration
func (m *Manager) GetRunningBackgroundTasks(ctx context.Context, migrationID string) ([]BackgroundTask, error) {
	return m.bgTaskMgr.GetRunningTasks(migrationID), nil
}

// GetBackgroundTask returns a specific background task by ID for a migration
func (m *Manager) GetBackgroundTask(ctx context.Context, migrationID, taskID string) (*BackgroundTask, error) {
	return m.bgTaskMgr.GetTask(migrationID, taskID)
}

// convertMetadata converts internal metadata to public API metadata
func convertMetadata(meta metadata.MigrationMetadata) MigrationMetadata {
	return MigrationMetadata{
		ID:         meta.ID,
		Name:       meta.Name,
		ConfigPath: meta.ConfigPath,
		CreatedAt:  meta.CreatedAt,
	}
}

// getMigrationPhase determines the current phase of a migration based on YAML status
// Returns: "roots", "traversal", "copy", or "unknown"
// If metadata doesn't exist, returns "roots" (fresh migration, roots phase)
func (m *Manager) getMigrationPhase(migrationID string) (string, error) {
	metaMgr := metadata.NewManager(m.cfg.Runtime.DataDir)
	meta, err := metaMgr.GetMigrationMetadata(migrationID)
	if err != nil {
		// If metadata doesn't exist, this is a fresh migration in roots phase
		// This is expected when setting roots for the first time
		return "roots", nil
	}

	if meta.ConfigPath == "" {
		return "roots", nil // No config yet, must be in roots phase
	}

	yamlCfg, err := migration.LoadMigrationConfig(meta.ConfigPath)
	if err != nil {
		return "unknown", err
	}

	status := strings.TrimSpace(yamlCfg.State.Status)

	// Determine phase from status
	switch {
	case status == "" || status == "Roots-Set":
		return "roots", nil
	case strings.Contains(status, "Traversal") || status == "Preparing-Path-Review" || status == "Awaiting-Path-Review" || status == "Preparing-For-Retry" || status == "Filters-Set" || status == "ETL-Bolt-To-Duck-In-Progress" || status == "ETL-Duck-To-Bolt-In-Progress":
		return "traversal", nil
	case strings.Contains(status, "Copy") || status == "Awaiting-Copy-Review" || status == "Preparing-For-Copy":
		return "copy", nil
	default:
		return "unknown", nil
	}
}

// checkPhaseLock checks if an operation is allowed in the current migration phase
// Returns error if operation is locked, nil if allowed
func (m *Manager) checkPhaseLock(migrationID string, operation string) error {
	phase, err := m.getMigrationPhase(migrationID)
	if err != nil {
		return fmt.Errorf("failed to determine migration phase: %w", err)
	}

	switch operation {
	case "setRoot":
		// Root selection locked when traversal starts
		if phase == "traversal" || phase == "copy" {
			return fmt.Errorf("root selection is locked: migration is in %s phase", phase)
		}
	case "startTraversal", "retrySweep", "exclude", "unexclude", "markRetry":
		// Traversal operations locked when copy starts
		// Note: markRetry is allowed in both phases (for traversal retries and copy retries)
		// but exclusion is only for traversal phase
		if operation == "exclude" || operation == "unexclude" {
			if phase == "copy" {
				return fmt.Errorf("exclusion operations are locked: migration is in copy phase (exclusion only applies to traversal)")
			}
		} else if phase == "copy" {
			return fmt.Errorf("traversal operations are locked: migration is in copy phase")
		}
	}

	return nil
}
