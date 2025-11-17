package corebridge

import (
	"context"

	"github.com/Project-Sylos/Migration-Engine/pkg/fsservices"
	"github.com/Project-Sylos/Migration-Engine/pkg/migration"
	"github.com/Project-Sylos/Sylos-API/internal/corebridge/database"
	"github.com/Project-Sylos/Sylos-API/internal/corebridge/migrations"
	"github.com/Project-Sylos/Sylos-API/internal/corebridge/roots"
	"github.com/Project-Sylos/Sylos-API/internal/corebridge/services"
	"github.com/Project-Sylos/Sylos-API/internal/corebridge/terminal"
	"github.com/Project-Sylos/Sylos-API/pkg/config"
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

func (m *Manager) ListChildren(ctx context.Context, req ListChildrenRequest) (fsservices.ListResult, error) {
	svcReq := services.ListChildrenRequest{
		ServiceID:  req.ServiceID,
		Identifier: req.Identifier,
		Role:       req.Role,
	}
	return m.serviceMgr.ListChildren(ctx, svcReq)
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
			EnableLoggingTerminal:   req.Options.EnableLoggingTerminal,
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

func (m *Manager) GetMigrationStatus(ctx context.Context, id string) (Status, error) {
	migStatus, err := m.migrationsMgr.GetMigrationStatus(ctx, id)
	if err != nil {
		return Status{}, err
	}
	return Status{
		Migration: Migration{
			ID:            migStatus.ID,
			SourceID:      migStatus.SourceID,
			DestinationID: migStatus.DestinationID,
			StartedAt:     migStatus.StartedAt,
			Status:        migStatus.Status,
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

func (m *Manager) UploadMigrationDB(ctx context.Context, filename string, data []byte, overwrite bool) (UploadMigrationDBResponse, error) {
	resp, err := database.UploadMigrationDB(ctx, m.logger, m.cfg.Runtime.MigrationDBStorageDir, filename, data, overwrite)
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
