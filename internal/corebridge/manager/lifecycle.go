package manager

import (
	"context"
	"fmt"
	"os"
	"time"

	"codeberg.org/Sylos/Migration-Engine/pkg/migration"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge/metadata"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge/roots"
)

func (m *Manager) StartMigration(ctx context.Context, req corebridge.StartMigrationRequest) (corebridge.Migration, error) {
	mig, err := m.ensureMigration(ctx, req.MigrationID)
	if err != nil {
		return corebridge.Migration{}, err
	}

	plan := m.rootsMgr.GetPlan(mig.ID)
	if plan == nil || !plan.HasSource || !plan.HasDestination {
		return corebridge.Migration{}, fmt.Errorf("roots not fully configured for migration %s", mig.ID)
	}

	cfg := m.buildTraversalConfig(req.Options, plan)
	if _, err := mig.AddRoots(plan.SourceRoot, plan.DestinationRoot); err != nil && mig.Phase() == migration.PhaseCreated {
		return corebridge.Migration{}, fmt.Errorf("failed to add roots: %w", err)
	}

	now := time.Now().UTC()
	m.mu.Lock()
	m.runtimeByID[mig.ID] = &runtimeMigration{
		Migration:     mig,
		SourceID:      plan.SourceDefinition.ID,
		DestinationID: plan.DestinationDefinition.ID,
		StartedAt:     now,
		Status:        corebridge.MigrationStatusRunning,
	}
	m.mu.Unlock()

	go func() {
		_, runErr := mig.StartTraversal(cfg)
		m.mu.Lock()
		defer m.mu.Unlock()
		if rec := m.runtimeByID[mig.ID]; rec != nil {
			doneAt := time.Now().UTC()
			rec.CompletedAt = &doneAt
			if runErr != nil {
				rec.Status = corebridge.MigrationStatusFailed
				rec.Error = runErr.Error()
			} else {
				rec.Status = migration.PhaseReview.String()
			}
		}
	}()

	if err := m.updateMetadataForMigrationID(mig.ID); err != nil {
		m.logger.Warn().Err(err).Str("migration_id", mig.ID).Msg("failed to update metadata")
	}

	return corebridge.Migration{
		ID:            mig.ID,
		SourceID:      plan.SourceDefinition.ID,
		DestinationID: plan.DestinationDefinition.ID,
		StartedAt:     now,
		Status:        corebridge.MigrationStatusRunning,
	}, nil
}

func (m *Manager) buildTraversalConfig(opts corebridge.MigrationOptions, plan *roots.RootPlan) migration.Config {
	workerCount := opts.WorkerCount
	if workerCount <= 0 {
		workerCount = m.cfg.Runtime.DefaultWorkerCount
	}
	if workerCount <= 0 {
		workerCount = 10
	}

	maxRetries := opts.MaxRetries
	if maxRetries <= 0 {
		maxRetries = m.cfg.Runtime.DefaultMaxRetries
	}
	if maxRetries <= 0 {
		maxRetries = 3
	}

	coordinatorLead := opts.CoordinatorLead
	if coordinatorLead <= 0 {
		coordinatorLead = m.cfg.Runtime.DefaultCoordinatorLead
	}
	if coordinatorLead <= 0 {
		coordinatorLead = 4
	}

	logAddress := opts.LogAddress
	if logAddress == "" {
		logAddress = m.cfg.Runtime.LogAddress
	}
	logLevel := opts.LogLevel
	if logLevel == "" {
		logLevel = m.cfg.Runtime.LogLevel
	}
	if logLevel == "" {
		logLevel = "info"
	}
	skipListener := true
	if opts.SkipListener != nil {
		skipListener = *opts.SkipListener
	}

	return migration.Config{
		Source: migration.Service{
			Name:    plan.SourceDefinition.ID,
			Adapter: plan.SourceAdapter,
			Root:    plan.SourceRoot,
		},
		Destination: migration.Service{
			Name:    plan.DestinationDefinition.ID,
			Adapter: plan.DestinationAdapter,
			Root:    plan.DestinationRoot,
		},
		WorkerCount:     workerCount,
		MaxRetries:      maxRetries,
		CoordinatorLead: coordinatorLead,
		LogAddress:      logAddress,
		LogLevel:        logLevel,
		SkipListener:    skipListener,
		StartupDelay:    time.Duration(opts.StartupDelaySec) * time.Second,
		ProgressTick:    time.Duration(opts.ProgressTickMillis) * time.Millisecond,
		Verification: migration.VerifyOptions{
			AllowPending:  opts.Verification.AllowPending,
			AllowNotOnSrc: opts.Verification.AllowNotOnSrc,
		},
	}
}

func (m *Manager) ensureMigration(_ context.Context, requestedID string) (*migration.Migration, error) {
	if requestedID != "" {
		existing, err := m.GetMigration(context.TODO(), requestedID)
		if err != nil && err != corebridge.ErrMigrationNotFound {
			return nil, err
		}
		if existing != nil {
			return existing, nil
		}
		// Requested ID was not found. Do not create a new migration (that would get a new ID
		// and then roots would not match). Caller must create the migration first (e.g. via SetRoot).
		return nil, fmt.Errorf("migration %q not found: ensure roots were set first so the migration exists", requestedID)
	}
	created, err := m.engineMgr.CreateMigration(migration.CreateMigrationConfig{
		Name: "migration",
	})
	if err != nil {
		return nil, err
	}
	// Per-migration flow: create folder and load so the engine creates the DB at migrationDir/{id}.db and attaches it.
	dir, err := m.migrationDirFor(created.ID)
	if err != nil {
		return nil, err
	}
	_ = os.MkdirAll(dir, 0755)
	loaded, err := m.GetMigration(context.TODO(), created.ID)
	if err != nil {
		return nil, err
	}
	return loaded, nil
}

func (m *Manager) updateMetadataForMigrationID(migrationID string) error {
	metaMgr := metadata.NewManager(m.cfg.Runtime.DataDir)
	meta, err := metaMgr.GetMigrationMetadata(migrationID)
	plan := m.rootsMgr.GetPlan(migrationID)
	dbPath := ""
	if plan != nil {
		dbPath = plan.DatabasePath
	}
	if err != nil {
		meta = metadata.MigrationMetadata{
			ID:             migrationID,
			Name:           migrationID,
			DatabasePath:   dbPath,
			IsNewMigration: false,
		}
	} else if meta.DatabasePath == "" && dbPath != "" {
		meta.DatabasePath = dbPath
	}
	return metaMgr.UpdateMigrationMetadata(meta)
}

func (m *Manager) ChangePhase(ctx context.Context, migrationID string, phase string, req corebridge.StartMigrationRequest) (corebridge.Migration, error) {
	if phase != "traversal" && phase != "copy" {
		return corebridge.Migration{}, fmt.Errorf("invalid phase: %s (must be 'traversal' or 'copy')", phase)
	}
	mig, err := m.GetMigration(context.TODO(), migrationID)
	if err != nil {
		return corebridge.Migration{}, err
	}

	go func() {
		if phase == "copy" {
			_, _ = mig.StartCopy()
			return
		}
		plan := m.rootsMgr.GetPlan(migrationID)
		if plan == nil || !plan.HasSource || !plan.HasDestination {
			return
		}
		cfg := m.buildTraversalConfig(req.Options, plan)
		_, _ = mig.StartTraversal(cfg)
	}()

	return corebridge.Migration{
		ID:      migrationID,
		Status:  "running",
		Success: true,
	}, nil
}

func (m *Manager) GetMigrationStatus(ctx context.Context, id string) (corebridge.Status, error) {
	mig, err := m.GetMigration(context.TODO(), id)
	if err != nil {
		return corebridge.Status{}, err
	}

	m.mu.RLock()
	rec := m.runtimeByID[id]
	m.mu.RUnlock()

	sourceID := ""
	destinationID := ""
	startedAt := time.Time{}
	status := mig.Phase().String()
	errText := ""
	var completedAt *time.Time
	if rec != nil {
		sourceID = rec.SourceID
		destinationID = rec.DestinationID
		startedAt = rec.StartedAt
		if rec.Status != "" {
			status = rec.Status
		}
		errText = rec.Error
		completedAt = rec.CompletedAt
	}

	return corebridge.Status{
		Migration: corebridge.Migration{
			ID:            id,
			SourceID:      sourceID,
			DestinationID: destinationID,
			StartedAt:     startedAt,
			Status:        status,
		},
		CompletedAt: completedAt,
		Error:       errText,
	}, nil
}

func (m *Manager) LoadMigration(ctx context.Context, migrationID string) (corebridge.Migration, error) {
	mig, err := m.GetMigration(context.TODO(), migrationID)
	if err != nil {
		return corebridge.Migration{}, err
	}
	return corebridge.Migration{
		ID:     migrationID,
		Status: mig.Phase().String(),
	}, nil
}

func (m *Manager) StopMigration(ctx context.Context, migrationID string) (corebridge.Status, error) {
	mig, err := m.GetMigration(context.TODO(), migrationID)
	if err != nil {
		return corebridge.Status{}, err
	}
	stopResult, err := mig.Stop()
	if err != nil {
		return corebridge.Status{}, err
	}

	st, statusErr := m.GetMigrationStatus(ctx, migrationID)
	if statusErr != nil {
		return corebridge.Status{
			Migration: corebridge.Migration{
				ID:     migrationID,
				Status: stopResult.Phase.String(),
			},
		}, nil
	}
	if stopResult.Stopped {
		st.Status = corebridge.MigrationStatusSuspended
	}
	return st, nil
}
