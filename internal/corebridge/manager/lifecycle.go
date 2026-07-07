package manager

import (
	"context"
	"fmt"
	"os"
	"time"

	"codeberg.org/Sylos/Migration-Engine/pkg/migration"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge/apidb"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge/roots"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge/services"
	fstypes "codeberg.org/Sylos/Sylos-FS/pkg/types"
)

func (m *Manager) StartMigration(ctx context.Context, req corebridge.StartMigrationRequest) (corebridge.Migration, error) {
	mig, err := m.ensureMigration(ctx, req.MigrationID)
	if err != nil {
		return corebridge.Migration{}, err
	}
	if err := m.ensureFSAdaptersRehydrated(mig.ID); err != nil {
		return corebridge.Migration{}, fmt.Errorf("restore filesystem credentials: %w", err)
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
		Status:        migration.PhaseTraversing,
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
				rec.Status = mig.Phase()
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
		Status:        migration.PhaseTraversing,
	}, nil
}

func (m *Manager) buildTraversalConfig(opts corebridge.MigrationOptions, plan *roots.RootPlan) migration.Config {
	// WorkerCount 0 lets Migration-Engine pick provider-profile defaults (merged across src/dst).
	workerCount := opts.WorkerCount

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
		Source:          migrationService(plan.SourceDefinition, plan.SourceAdapter, plan.SourceRoot, plan.SourceConnectionID),
		Destination:     migrationService(plan.DestinationDefinition, plan.DestinationAdapter, plan.DestinationRoot, plan.DestinationConnectionID),
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

func migrationService(def services.ServiceDefinition, adapter fstypes.FSAdapter, root fstypes.Folder, connectionID string) migration.Service {
	svc := migration.Service{
		Name:    def.ID,
		Adapter: adapter,
		Root:    root,
	}
	switch def.Type {
	case services.ServiceTypeCloud:
		svc.ProviderID = services.CloudProviderID(def)
		if connectionID != "" {
			svc.BackendGroupID = "conn:" + connectionID
		}
	case services.ServiceTypeLocal:
		svc.ProviderID = string(services.ServiceTypeLocal)
	case services.ServiceTypeSpectra:
		svc.ProviderID = string(services.ServiceTypeSpectra)
	}
	return svc
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
	if _, err := m.apiDB.EnsureMigrationKey(created.ID); err != nil {
		return nil, err
	}
	loaded, err := m.GetMigration(context.TODO(), created.ID)
	if err != nil {
		return nil, err
	}
	return loaded, nil
}

func (m *Manager) updateMetadataForMigrationID(migrationID string) error {
	rec, err := m.getMigrationRecord(migrationID)
	plan := m.rootsMgr.GetPlan(migrationID)
	dbPath := ""
	if plan != nil {
		dbPath = plan.DatabasePath
	}
	if err != nil || rec.ID == "" {
		rec = apidb.MigrationRecord{
			ID:             migrationID,
			Name:           migrationID,
			DatabasePath:   dbPath,
			IsNewMigration: false,
		}
	} else if rec.DatabasePath == "" && dbPath != "" {
		rec.DatabasePath = dbPath
	}
	return m.upsertMigrationRecord(rec)
}

func (m *Manager) ChangePhase(ctx context.Context, migrationID string, phase string, req corebridge.StartMigrationRequest) (corebridge.Migration, error) {
	if phase != "traversal" && phase != "copy" {
		return corebridge.Migration{}, fmt.Errorf("invalid phase: %s (must be 'traversal' or 'copy')", phase)
	}
	mig, err := m.GetMigration(context.TODO(), migrationID)
	if err != nil {
		return corebridge.Migration{}, err
	}
	if err := m.ensureFSAdaptersRehydrated(migrationID); err != nil {
		return corebridge.Migration{}, fmt.Errorf("restore filesystem credentials: %w", err)
	}
	plan := m.rootsMgr.GetPlan(migrationID)

	now := time.Now().UTC()
	m.mu.Lock()
	rec := m.runtimeByID[migrationID]
	if rec == nil {
		sourceID, destID := "", ""
		if plan != nil {
			sourceID = plan.SourceDefinition.ID
			destID = plan.DestinationDefinition.ID
		}
		rec = &runtimeMigration{
			Migration:     mig,
			SourceID:      sourceID,
			DestinationID: destID,
			StartedAt:     now,
			Status:        "",
			Error:         "",
		}
		m.runtimeByID[migrationID] = rec
	}
	if phase == "copy" {
		rec.Status = migration.PhaseCopying
		rec.CompletedAt = nil
		rec.Error = ""
	} else {
		rec.Status = migration.PhaseTraversing
		rec.CompletedAt = nil
		rec.Error = ""
	}
	m.mu.Unlock()

	go func() {
		var runErr error
		if phase == "copy" {
			if plan == nil || !plan.HasSource || !plan.HasDestination {
				runErr = fmt.Errorf("roots not configured for migration %s", migrationID)
			} else {
				cfg := m.buildTraversalConfig(req.Options, plan)
				_, runErr = mig.StartCopy(cfg)
			}
		} else {
			if plan == nil || !plan.HasSource || !plan.HasDestination {
				runErr = fmt.Errorf("roots not configured for migration %s", migrationID)
			} else {
				cfg := m.buildTraversalConfig(req.Options, plan)
				_, runErr = mig.StartTraversal(cfg)
			}
		}
		// Forced-sync: update runtime cache from engine state when goroutine ends.
		doneAt := time.Now().UTC()
		m.mu.Lock()
		defer m.mu.Unlock()
		if rec := m.runtimeByID[migrationID]; rec != nil {
			rec.CompletedAt = &doneAt
			if runErr != nil {
				rec.Status = corebridge.MigrationStatusFailed
				rec.Error = runErr.Error()
			} else {
				rec.Status = mig.Phase()
			}
		}
	}()

	status := migration.PhaseCopying
	if phase != "copy" {
		status = migration.PhaseTraversing
	}
	return corebridge.Migration{
		ID:      migrationID,
		Status:  status,
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
	status := mig.Phase()
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
	// Engine wins once soft suspend has persisted (runtime cache may still show *-in-progress).
	if mig.Phase() == migration.PhaseTraversalSuspended || mig.Phase() == migration.PhaseCopySuspended {
		status = mig.Phase()
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
		Live:        mig.IsLive(),
	}, nil
}

func (m *Manager) LoadMigration(ctx context.Context, migrationID string) (corebridge.Migration, error) {
	mig, err := m.GetMigration(context.TODO(), migrationID)
	if err != nil {
		return corebridge.Migration{}, err
	}
	return corebridge.Migration{
		ID:     migrationID,
		Status: mig.Phase(),
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

	const stopGracePeriod = 30 * time.Second
	if stopResult.SoftSuspendRequested {
		deadline := time.Now().Add(stopGracePeriod)
		for mig.IsLive() && time.Now().Before(deadline) {
			time.Sleep(200 * time.Millisecond)
		}
		if mig.IsLive() {
			forceResult, forceErr := mig.ForceStop()
			if forceErr != nil {
				return corebridge.Status{}, forceErr
			}
			stopResult = forceResult
		}
	}

	st, statusErr := m.GetMigrationStatus(ctx, migrationID)
	if statusErr != nil {
		return corebridge.Status{
			Migration: corebridge.Migration{
				ID:     migrationID,
				Status: stopResult.Phase,
			},
			Live:                 mig.IsLive(),
			SoftSuspendRequested: stopResult.SoftSuspendRequested,
			Stopped:              stopResult.Stopped,
		}, nil
	}
	st.Live = mig.IsLive()
	st.SoftSuspendRequested = stopResult.SoftSuspendRequested
	st.Stopped = stopResult.Stopped
	if stopResult.ForceStopped {
		st.Status = corebridge.MigrationStatusSuspended
	}
	// Soft suspend: keep engine phase until drain finishes (e.g. still traversal-in-progress); do not force generic "suspended".
	// Hard cancel / other stopped paths: surface legacy suspended label for clients that expect it.
	if stopResult.Stopped && !stopResult.SoftSuspendRequested && !stopResult.ForceStopped {
		st.Status = corebridge.MigrationStatusSuspended
	}
	return st, nil
}
