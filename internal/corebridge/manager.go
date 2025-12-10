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

	manager := &Manager{
		logger:        logger,
		cfg:           cfg,
		serviceMgr:    serviceMgr,
		rootsMgr:      rootsMgr,
		migrationsMgr: migrationsMgr,
		terminalMgr:   terminalMgr,
	}

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
				AllowFailed:   req.Options.Verification.AllowFailed,
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

	// Run exclusion sweep if there are pending exclusions (blocking)
	if pendingWork.HasPendingExclusions {
		m.logger.Info().
			Str("migration_id", migrationID).
			Str("phase", phase).
			Msg("running exclusion sweep before phase change")

		// Run exclusion sweep synchronously before starting the phase
		_, err := m.TriggerExclusionSweep(ctx, migrationID, SweepConfigRequest{})
		if err != nil {
			return Migration{}, fmt.Errorf("failed to run exclusion sweep: %w", err)
		}

		m.logger.Info().
			Str("migration_id", migrationID).
			Msg("exclusion sweep completed, proceeding with phase change")
	}

	// Start the migration (which will handle the phase based on checkpoint state)
	// The Migration Engine will automatically handle traversal vs copy based on checkpoint state
	return m.StartMigration(ctx, req)
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
		metrics.SrcTraversal = &QueueObserverMetrics{
			QueueStats: QueueStats{
				Name:         dbMetrics.SrcTraversal.Name,
				Round:        dbMetrics.SrcTraversal.Round,
				Pending:      dbMetrics.SrcTraversal.Pending,
				InProgress:   dbMetrics.SrcTraversal.InProgress,
				TotalTracked: dbMetrics.SrcTraversal.TotalTracked,
				Workers:      dbMetrics.SrcTraversal.Workers,
			},
			AverageExecutionTime: dbMetrics.SrcTraversal.AverageExecutionTime,
			TasksPerSecond:       dbMetrics.SrcTraversal.TasksPerSecond,
			TotalCompleted:       dbMetrics.SrcTraversal.TotalCompleted,
			LastPollTime:         dbMetrics.SrcTraversal.LastPollTime,
		}
	}
	if dbMetrics.DstTraversal != nil {
		metrics.DstTraversal = &QueueObserverMetrics{
			QueueStats: QueueStats{
				Name:         dbMetrics.DstTraversal.Name,
				Round:        dbMetrics.DstTraversal.Round,
				Pending:      dbMetrics.DstTraversal.Pending,
				InProgress:   dbMetrics.DstTraversal.InProgress,
				TotalTracked: dbMetrics.DstTraversal.TotalTracked,
				Workers:      dbMetrics.DstTraversal.Workers,
			},
			AverageExecutionTime: dbMetrics.DstTraversal.AverageExecutionTime,
			TasksPerSecond:       dbMetrics.DstTraversal.TasksPerSecond,
			TotalCompleted:       dbMetrics.DstTraversal.TotalCompleted,
			LastPollTime:         dbMetrics.DstTraversal.LastPollTime,
		}
	}
	if dbMetrics.Copy != nil {
		metrics.Copy = &QueueObserverMetrics{
			QueueStats: QueueStats{
				Name:         dbMetrics.Copy.Name,
				Round:        dbMetrics.Copy.Round,
				Pending:      dbMetrics.Copy.Pending,
				InProgress:   dbMetrics.Copy.InProgress,
				TotalTracked: dbMetrics.Copy.TotalTracked,
				Workers:      dbMetrics.Copy.Workers,
			},
			AverageExecutionTime: dbMetrics.Copy.AverageExecutionTime,
			TasksPerSecond:       dbMetrics.Copy.TasksPerSecond,
			TotalCompleted:       dbMetrics.Copy.TotalCompleted,
			LastPollTime:         dbMetrics.Copy.LastPollTime,
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
	// Get DB instance - tries record.DB first, then pool
	boltDB := m.migrationsMgr.GetDB(req.MigrationID)

	var dbItems map[string]database.PathNodes
	var dbPagination database.PaginationInfo
	var err error

	if boltDB != nil {
		// Use the shared DB instance from the pool
		dbItems, dbPagination, err = database.GetChildrenDiffsFromDBInstance(ctx, m.logger, boltDB, req.Path, req.Offset, req.Limit, req.FoldersOnly)
		if err != nil {
			// Check if this is a database not available error - indicates DB was closed unexpectedly
			if isDatabaseNotAvailableError(err) {
				// CRITICAL: DB was closed unexpectedly - this indicates a lifecycle violation
				m.logger.Error().
					Err(err).
					Str("migration_id", req.MigrationID).
					Msg("CRITICAL: DB for migration was closed unexpectedly — this indicates an ME lifecycle violation")

				return ListChildrenDiffsResponse{}, ErrDatabaseNotAvailable
			}
			return ListChildrenDiffsResponse{}, fmt.Errorf("failed to get children diffs from DB instance: %w", err)
		}
	} else {
		// Fallback: migration not running or DB not available, open a new connection
		// Get migration metadata to find the config path
		metaMgr := metadata.NewManager(m.cfg.Runtime.DataDir)
		meta, err := metaMgr.GetMigrationMetadata(req.MigrationID)
		if err != nil {
			return ListChildrenDiffsResponse{}, ErrMigrationNotFound
		}

		// Derive database path from config path
		// Config path is {db_path}.yaml, so DB path is {config_path sans .yaml}.db
		dbPath := strings.TrimSuffix(meta.ConfigPath, ".yaml") + ".db"
		if dbPath == ".db" {
			// Fallback: try to resolve from migration ID
			dbPath, err = database.ResolveDatabasePath(m.cfg.Runtime.DataDir, "", req.MigrationID)
			if err != nil {
				return ListChildrenDiffsResponse{}, fmt.Errorf("failed to resolve database path: %w", err)
			}
		}

		// Check if database file exists
		if _, err := os.Stat(dbPath); os.IsNotExist(err) {
			return ListChildrenDiffsResponse{}, ErrMigrationNotFound
		}

		// Get children diffs from database
		dbItems, dbPagination, err = database.GetChildrenDiffsFromDB(ctx, m.logger, dbPath, req.Path, req.Offset, req.Limit, req.FoldersOnly)
		if err != nil {
			return ListChildrenDiffsResponse{}, fmt.Errorf("failed to get children diffs: %w", err)
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
				DisplayName:     dbPathNodes.Src.DisplayName,
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
				DisplayName:     dbPathNodes.Dst.DisplayName,
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

// ExcludeNode excludes a node and queues its children for exclusion propagation
func (m *Manager) ExcludeNode(ctx context.Context, migrationID string, nodeID string) (*ExclusionResponse, error) {
	// Get DB instance
	boltDB := m.migrationsMgr.GetDB(migrationID)
	if boltDB == nil {
		// Try to get DB path and open it
		metaMgr := metadata.NewManager(m.cfg.Runtime.DataDir)
		meta, err := metaMgr.GetMigrationMetadata(migrationID)
		if err != nil {
			return &ExclusionResponse{
				Success: false,
				Error:   "migration not found",
			}, ErrMigrationNotFound
		}

		// Derive database path from config path
		dbPath := strings.TrimSuffix(meta.ConfigPath, ".yaml") + ".db"
		if dbPath == ".db" {
			dbPath, err = database.ResolveDatabasePath(m.cfg.Runtime.DataDir, "", migrationID)
			if err != nil {
				return &ExclusionResponse{
					Success: false,
					Error:   "failed to resolve database path",
				}, fmt.Errorf("failed to resolve database path: %w", err)
			}
		}

		// Check if database file exists
		if _, err := os.Stat(dbPath); os.IsNotExist(err) {
			return &ExclusionResponse{
				Success: false,
				Error:   "migration database not found",
			}, ErrMigrationNotFound
		}

		// Open database for update
		var errOpen error
		boltDB, _, errOpen = migration.SetupDatabase(migration.DatabaseConfig{
			Path:           dbPath,
			RemoveExisting: false,
		})
		if errOpen != nil {
			return &ExclusionResponse{
				Success: false,
				Error:   "failed to open database",
			}, fmt.Errorf("failed to open database: %w", errOpen)
		}
		defer func() {
			if err := boltDB.Close(); err != nil {
				m.logger.Warn().Err(err).Str("migration_id", migrationID).Msg("failed to close database after exclusion")
			}
		}()
	}

	// Perform exclusion
	err := database.SetNodeExclusion(ctx, m.logger, boltDB, nodeID, true)
	if err != nil {
		m.logger.Error().Err(err).Str("migration_id", migrationID).Str("node_id", nodeID).Msg("failed to exclude node")
		return &ExclusionResponse{
			Success: false,
			Error:   err.Error(),
		}, err
	}

	// Mark that user made changes during path review
	if err := m.markPathReviewChanges(migrationID, true); err != nil {
		m.logger.Warn().Err(err).Str("migration_id", migrationID).Msg("failed to mark path review changes")
		// Don't fail the exclusion, just log the warning
	}

	return &ExclusionResponse{
		Success: true,
	}, nil
}

// UnexcludeNode unexcludes a node and queues its children for unexclusion propagation
func (m *Manager) UnexcludeNode(ctx context.Context, migrationID string, nodeID string) (*ExclusionResponse, error) {
	// Get DB instance
	boltDB := m.migrationsMgr.GetDB(migrationID)
	if boltDB == nil {
		// Try to get DB path and open it
		metaMgr := metadata.NewManager(m.cfg.Runtime.DataDir)
		meta, err := metaMgr.GetMigrationMetadata(migrationID)
		if err != nil {
			return &ExclusionResponse{
				Success: false,
				Error:   "migration not found",
			}, ErrMigrationNotFound
		}

		// Derive database path from config path
		dbPath := strings.TrimSuffix(meta.ConfigPath, ".yaml") + ".db"
		if dbPath == ".db" {
			dbPath, err = database.ResolveDatabasePath(m.cfg.Runtime.DataDir, "", migrationID)
			if err != nil {
				return &ExclusionResponse{
					Success: false,
					Error:   "failed to resolve database path",
				}, fmt.Errorf("failed to resolve database path: %w", err)
			}
		}

		// Check if database file exists
		if _, err := os.Stat(dbPath); os.IsNotExist(err) {
			return &ExclusionResponse{
				Success: false,
				Error:   "migration database not found",
			}, ErrMigrationNotFound
		}

		// Open database for update
		var errOpen error
		boltDB, _, errOpen = migration.SetupDatabase(migration.DatabaseConfig{
			Path:           dbPath,
			RemoveExisting: false,
		})
		if errOpen != nil {
			return &ExclusionResponse{
				Success: false,
				Error:   "failed to open database",
			}, fmt.Errorf("failed to open database: %w", errOpen)
		}
		defer func() {
			if err := boltDB.Close(); err != nil {
				m.logger.Warn().Err(err).Str("migration_id", migrationID).Msg("failed to close database after unexclusion")
			}
		}()
	}

	// Perform unexclusion
	err := database.SetNodeExclusion(ctx, m.logger, boltDB, nodeID, false)
	if err != nil {
		m.logger.Error().Err(err).Str("migration_id", migrationID).Str("node_id", nodeID).Msg("failed to unexclude node")
		return &ExclusionResponse{
			Success: false,
			Error:   err.Error(),
		}, err
	}

	// Mark that user made changes during path review
	if err := m.markPathReviewChanges(migrationID, true); err != nil {
		m.logger.Warn().Err(err).Str("migration_id", migrationID).Msg("failed to mark path review changes")
		// Don't fail the unexclusion, just log the warning
	}

	return &ExclusionResponse{
		Success: true,
	}, nil
}

// TriggerExclusionSweep triggers an exclusion sweep for a migration
func (m *Manager) TriggerExclusionSweep(ctx context.Context, migrationID string, config SweepConfigRequest) (SweepResponse, error) {
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

	// Check if Spectra override config exists
	overridePath, exists, _ := services.LoadSpectraConfigOverride(m.cfg.Runtime.DataDir, migrationID)
	var spectraConfigPath string
	if exists {
		spectraConfigPath = overridePath
	}

	// Load YAML config and reconstruct adapters
	adapterFactory := m.createAdapterFactoryForSweep(spectraConfigPath)
	migrationCfg, err := migration.LoadMigrationConfigFromYAML(configPath, adapterFactory)
	if err != nil {
		return SweepResponse{
			Success: false,
			Error:   fmt.Sprintf("failed to load migration config: %v", err),
		}, fmt.Errorf("failed to load migration config: %w", err)
	}

	// Get or open DB instance
	dbInstance := m.migrationsMgr.GetDB(migrationID)
	if dbInstance == nil {
		// Try to open DB
		dbPath := migrationCfg.Database.Path
		if dbPath == "" {
			return SweepResponse{
				Success: false,
				Error:   "database path not found in config",
			}, fmt.Errorf("database path not found in config")
		}

		// Open database using migration.SetupDatabase (same pattern as ExcludeNode)
		var errOpen error
		dbInstance, _, errOpen = migration.SetupDatabase(migration.DatabaseConfig{
			Path:           dbPath,
			RemoveExisting: false,
		})
		if errOpen != nil {
			return SweepResponse{
				Success: false,
				Error:   "failed to open database",
			}, fmt.Errorf("failed to open database: %w", errOpen)
		}
	}

	// Build sweep config with defaults and overrides
	sweepCfg := migration.SweepConfig{
		BoltDB:      dbInstance,
		SrcAdapter:  migrationCfg.Source.Adapter,
		DstAdapter:  migrationCfg.Destination.Adapter,
		WorkerCount: m.selectWorkerCountForSweep(config.WorkerCount),
		MaxRetries:  m.selectMaxRetriesForSweep(config.MaxRetries),
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

	// Use background context for shutdown (HTTP request context gets canceled when handler returns)
	sweepCfg.ShutdownContext = context.Background()

	// Start sweep in goroutine (hybrid async pattern)
	go func() {
		stats, err := migration.RunExclusionSweep(sweepCfg)
		if err != nil {
			m.logger.Error().
				Err(err).
				Str("migration_id", migrationID).
				Msg("exclusion sweep failed")
			return
		}

		m.logger.Info().
			Str("migration_id", migrationID).
			Dur("duration", stats.Duration).
			Int("src_round", stats.Src.Round).
			Int("src_pending", stats.Src.Pending).
			Int("dst_round", stats.Dst.Round).
			Int("dst_pending", stats.Dst.Pending).
			Msg("exclusion sweep completed")

		// Clear path review changes flag on successful sweep completion
		if err := m.markPathReviewChanges(migrationID, false); err != nil {
			m.logger.Warn().Err(err).Str("migration_id", migrationID).Msg("failed to clear path review changes flag")
		}
	}()

	return SweepResponse{
		Success: true,
		Message: "Exclusion sweep started",
	}, nil
}

// TriggerRetrySweep triggers a retry sweep for a migration
func (m *Manager) TriggerRetrySweep(ctx context.Context, migrationID string, config SweepConfigRequest) (SweepResponse, error) {
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

	// Step 1: Update checkpoint state to Filters-Set with retry metadata
	// This must be done before running the retry sweep
	// TODO: Use migration.SetStatusFiltersSet(yamlCfg, true, maxKnownDepth) when SDK is updated
	// For now, set state directly (SDK may not have SetStatusFiltersSet yet)
	yamlCfg.State.Status = "Filters-Set"
	// Note: IsRetrySweep and MaxKnownDepth fields may need to be set once SDK is updated
	// These fields should be in StateConfig: IsRetrySweep *bool, MaxKnownDepth *int

	if err := migration.SaveMigrationConfig(configPath, yamlCfg); err != nil {
		return SweepResponse{
			Success: false,
			Error:   fmt.Sprintf("failed to update checkpoint state: %v", err),
		}, fmt.Errorf("failed to save migration config: %w", err)
	}

	m.logger.Info().
		Str("migration_id", migrationID).
		Str("checkpoint_status", "Filters-Set").
		Int("max_known_depth", maxKnownDepth).
		Msg("updated checkpoint state to Filters-Set for retry sweep")

	// Load full config with adapters for sweep execution
	adapterFactory := m.createAdapterFactoryForSweep(spectraConfigPath)
	migrationCfg, err := migration.LoadMigrationConfigFromYAML(configPath, adapterFactory)
	if err != nil {
		return SweepResponse{
			Success: false,
			Error:   fmt.Sprintf("failed to load migration config with adapters: %v", err),
		}, fmt.Errorf("failed to load migration config: %w", err)
	}

	// Get or open DB instance
	dbInstance := m.migrationsMgr.GetDB(migrationID)
	if dbInstance == nil {
		// Try to open DB
		dbPath := migrationCfg.Database.Path
		if dbPath == "" {
			return SweepResponse{
				Success: false,
				Error:   "database path not found in config",
			}, fmt.Errorf("database path not found in config")
		}

		// Open database using migration.SetupDatabase (same pattern as ExcludeNode)
		var errOpen error
		dbInstance, _, errOpen = migration.SetupDatabase(migration.DatabaseConfig{
			Path:           dbPath,
			RemoveExisting: false,
		})
		if errOpen != nil {
			return SweepResponse{
				Success: false,
				Error:   "failed to open database",
			}, fmt.Errorf("failed to open database: %w", errOpen)
		}
	}

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

	// Use background context for shutdown (HTTP request context gets canceled when handler returns)
	sweepCfg.ShutdownContext = context.Background()

	// Start sweep in goroutine (hybrid async pattern)
	go func() {
		stats, err := migration.RunRetrySweep(sweepCfg)
		if err != nil {
			m.logger.Error().
				Err(err).
				Str("migration_id", migrationID).
				Msg("retry sweep failed")
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

		// Clear path review changes flag on successful sweep completion
		if err := m.markPathReviewChanges(migrationID, false); err != nil {
			m.logger.Warn().Err(err).Str("migration_id", migrationID).Msg("failed to clear path review changes flag")
		}
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

// createAdapterFactoryForSweep creates an adapter factory for reconstructing adapters from YAML config
// This mirrors the logic in migrations.Manager.createAdapterFactory
func (m *Manager) createAdapterFactoryForSweep(spectraConfigOverridePath string) migration.AdapterFactory {
	return func(serviceType string, serviceCfg migration.ServiceConfigYAML, serviceConfigs map[string]interface{}) (fstypes.FSAdapter, error) {
		switch strings.ToLower(serviceType) {
		case "spectra":
			// Use override config if provided, otherwise try to extract from serviceConfigs
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
	// Get DB instance
	dbInstance := m.migrationsMgr.GetDB(migrationID)
	if dbInstance == nil {
		// Try to get DB path and open it
		metaMgr := metadata.NewManager(m.cfg.Runtime.DataDir)
		meta, err := metaMgr.GetMigrationMetadata(migrationID)
		if err != nil {
			return PendingWorkResponse{}, ErrMigrationNotFound
		}

		// Derive database path from config path
		dbPath := strings.TrimSuffix(meta.ConfigPath, ".yaml") + ".db"
		if dbPath == ".db" {
			dbPath, err = database.ResolveDatabasePath(m.cfg.Runtime.DataDir, "", migrationID)
			if err != nil {
				return PendingWorkResponse{}, fmt.Errorf("failed to resolve database path: %w", err)
			}
		}

		// Check if database file exists
		if _, err := os.Stat(dbPath); os.IsNotExist(err) {
			return PendingWorkResponse{}, ErrMigrationNotFound
		}

		// Open database for read
		var errOpen error
		dbInstance, _, errOpen = migration.SetupDatabase(migration.DatabaseConfig{
			Path:           dbPath,
			RemoveExisting: false,
		})
		if errOpen != nil {
			return PendingWorkResponse{}, fmt.Errorf("failed to open database: %w", errOpen)
		}
		defer func() {
			if err := dbInstance.Close(); err != nil {
				m.logger.Warn().Err(err).Str("migration_id", migrationID).Msg("failed to close database after checking pending work")
			}
		}()
	}

	// Count pending exclusions
	exclusionsCount, err := database.CountPendingExclusions(ctx, m.logger, dbInstance)
	if err != nil {
		return PendingWorkResponse{}, fmt.Errorf("failed to count pending exclusions: %w", err)
	}

	// Count pending retries
	retriesCount, err := database.CountPendingRetries(ctx, m.logger, dbInstance)
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
		HasPendingExclusions:   exclusionsCount > 0,
		HasPendingRetries:      retriesCount > 0,
		HasPathReviewChanges:   hasUnsavedChanges,
		PendingExclusionsCount: exclusionsCount,
		PendingRetriesCount:    retriesCount,
	}, nil
}

// MarkNodeForRetry marks a failed node for retry
func (m *Manager) MarkNodeForRetry(ctx context.Context, migrationID string, nodeID string) (*MarkRetryResponse, error) {
	// Get DB instance
	dbInstance := m.migrationsMgr.GetDB(migrationID)
	if dbInstance == nil {
		// Try to get DB path and open it
		metaMgr := metadata.NewManager(m.cfg.Runtime.DataDir)
		meta, err := metaMgr.GetMigrationMetadata(migrationID)
		if err != nil {
			return &MarkRetryResponse{
				Success: false,
				Error:   "migration not found",
			}, ErrMigrationNotFound
		}

		// Derive database path from config path
		dbPath := strings.TrimSuffix(meta.ConfigPath, ".yaml") + ".db"
		if dbPath == ".db" {
			dbPath, err = database.ResolveDatabasePath(m.cfg.Runtime.DataDir, "", migrationID)
			if err != nil {
				return &MarkRetryResponse{
					Success: false,
					Error:   "failed to resolve database path",
				}, fmt.Errorf("failed to resolve database path: %w", err)
			}
		}

		// Check if database file exists
		if _, err := os.Stat(dbPath); os.IsNotExist(err) {
			return &MarkRetryResponse{
				Success: false,
				Error:   "migration database not found",
			}, ErrMigrationNotFound
		}

		// Open database for update
		var errOpen error
		dbInstance, _, errOpen = migration.SetupDatabase(migration.DatabaseConfig{
			Path:           dbPath,
			RemoveExisting: false,
		})
		if errOpen != nil {
			return &MarkRetryResponse{
				Success: false,
				Error:   "failed to open database",
			}, fmt.Errorf("failed to open database: %w", errOpen)
		}
		defer func() {
			if err := dbInstance.Close(); err != nil {
				m.logger.Warn().Err(err).Str("migration_id", migrationID).Msg("failed to close database after marking node for retry")
			}
		}()
	}

	// Mark node for retry
	err := database.MarkNodeForRetry(ctx, m.logger, dbInstance, nodeID)
	if err != nil {
		m.logger.Error().Err(err).Str("migration_id", migrationID).Str("node_id", nodeID).Msg("failed to mark node for retry")
		return &MarkRetryResponse{
			Success: false,
			Error:   err.Error(),
		}, err
	}

	// Mark that user made changes during path review
	if err := m.markPathReviewChanges(migrationID, true); err != nil {
		m.logger.Warn().Err(err).Str("migration_id", migrationID).Msg("failed to mark path review changes")
		// Don't fail the retry marking, just log the warning
	}

	return &MarkRetryResponse{
		Success: true,
	}, nil
}

// UnmarkNodeForRetry unmarks a pending node for retry (changes status back to failed)
func (m *Manager) UnmarkNodeForRetry(ctx context.Context, migrationID string, nodeID string) (*MarkRetryResponse, error) {
	// Get DB instance
	dbInstance := m.migrationsMgr.GetDB(migrationID)
	if dbInstance == nil {
		// Try to get DB path and open it
		metaMgr := metadata.NewManager(m.cfg.Runtime.DataDir)
		meta, err := metaMgr.GetMigrationMetadata(migrationID)
		if err != nil {
			return &MarkRetryResponse{
				Success: false,
				Error:   "migration not found",
			}, ErrMigrationNotFound
		}

		// Derive database path from config path
		dbPath := strings.TrimSuffix(meta.ConfigPath, ".yaml") + ".db"
		if dbPath == ".db" {
			dbPath, err = database.ResolveDatabasePath(m.cfg.Runtime.DataDir, "", migrationID)
			if err != nil {
				return &MarkRetryResponse{
					Success: false,
					Error:   "failed to resolve database path",
				}, fmt.Errorf("failed to resolve database path: %w", err)
			}
		}

		// Check if database file exists
		if _, err := os.Stat(dbPath); os.IsNotExist(err) {
			return &MarkRetryResponse{
				Success: false,
				Error:   "migration database not found",
			}, ErrMigrationNotFound
		}

		// Open database for update
		var errOpen error
		dbInstance, _, errOpen = migration.SetupDatabase(migration.DatabaseConfig{
			Path:           dbPath,
			RemoveExisting: false,
		})
		if errOpen != nil {
			return &MarkRetryResponse{
				Success: false,
				Error:   "failed to open database",
			}, fmt.Errorf("failed to open database: %w", errOpen)
		}
		defer func() {
			if err := dbInstance.Close(); err != nil {
				m.logger.Warn().Err(err).Str("migration_id", migrationID).Msg("failed to close database after unmarking node for retry")
			}
		}()
	}

	// Unmark node for retry
	err := database.UnmarkNodeForRetry(ctx, m.logger, dbInstance, nodeID)
	if err != nil {
		m.logger.Error().Err(err).Str("migration_id", migrationID).Str("node_id", nodeID).Msg("failed to unmark node for retry")
		return &MarkRetryResponse{
			Success: false,
			Error:   err.Error(),
		}, err
	}

	// Mark that user made changes during path review
	if err := m.markPathReviewChanges(migrationID, true); err != nil {
		m.logger.Warn().Err(err).Str("migration_id", migrationID).Msg("failed to mark path review changes")
		// Don't fail the unretry marking, just log the warning
	}

	return &MarkRetryResponse{
		Success: true,
	}, nil
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
