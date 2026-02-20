package manager

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"codeberg.org/Sylos/Migration-Engine/pkg/migration"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge/database"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge/metadata"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge/migrations"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge/services"
)

func (m *Manager) StartMigration(ctx context.Context, req corebridge.StartMigrationRequest) (corebridge.Migration, error) {
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
		return corebridge.Migration{}, err
	}
	return corebridge.Migration{
		ID:            mig.ID,
		SourceID:      mig.SourceID,
		DestinationID: mig.DestinationID,
		StartedAt:     mig.StartedAt,
		Status:        mig.Status,
	}, nil
}

func (m *Manager) ChangePhase(ctx context.Context, migrationID string, phase string, req corebridge.StartMigrationRequest) (corebridge.Migration, error) {
	if phase != "traversal" && phase != "copy" {
		return corebridge.Migration{}, fmt.Errorf("invalid phase: %s (must be 'traversal' or 'copy')", phase)
	}

	pendingWork, err := m.CheckPendingWork(ctx, migrationID)
	if err != nil {
		return corebridge.Migration{}, fmt.Errorf("failed to check pending work: %w", err)
	}

	if phase == "copy" && pendingWork.HasPendingRetries {
		return corebridge.Migration{}, fmt.Errorf("cannot start copy phase: there are pending retries. Please run traversal phase first")
	}

	metaMgr := metadata.NewManager(m.cfg.Runtime.DataDir)
	meta, err := metaMgr.GetMigrationMetadata(migrationID)
	if err != nil {
		return corebridge.Migration{}, fmt.Errorf("failed to get migration metadata: %w", err)
	}

	dbPath := strings.TrimSuffix(meta.ConfigPath, ".yaml") + ".db"
	if dbPath == ".db" {
		dbPath, err = database.ResolveDatabasePath(m.cfg.Runtime.DataDir, "", migrationID)
		if err != nil {
			return corebridge.Migration{}, fmt.Errorf("failed to resolve database path: %w", err)
		}
	}

	runningTasks := m.bgTaskMgr.GetRunningTasks(migrationID)
	if len(runningTasks) > 0 {
		return corebridge.Migration{}, fmt.Errorf("cannot change phase: there are %d running background tasks. Please wait for them to complete", len(runningTasks))
	}

	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		return corebridge.Migration{}, fmt.Errorf("migration DB file not found. Migration must be in 'Awaiting-Path-Review' status before changing phase")
	}

	if err := m.migrationsMgr.KillDuckDBConnection(migrationID); err != nil {
		m.logger.Warn().Err(err).Str("migration_id", migrationID).Msg("failed to close DuckDB connection (may not be open), proceeding anyway")
	}
	if err := m.migrationsMgr.CloseDB(migrationID); err != nil {
		m.logger.Warn().Err(err).Str("migration_id", migrationID).Msg("failed to close DB connection (may not be open), proceeding anyway")
	}

	record := m.migrationsMgr.GetRecord(migrationID)
	if record == nil {
		record = &migrations.MigrationRecord{ID: migrationID}
	}

	overridePath, exists, _ := services.LoadSpectraConfigOverride(m.cfg.Runtime.DataDir, migrationID)
	var spectraConfigPath string
	if exists {
		spectraConfigPath = overridePath
	}

	yamlCfg, err := migration.LoadMigrationConfig(meta.ConfigPath)
	if err != nil {
		return corebridge.Migration{}, fmt.Errorf("failed to load migration config: %w", err)
	}

	if phase == "copy" {
		yamlCfg.State.Status = "Preparing-For-Copy"
		if err := migration.SaveMigrationConfig(meta.ConfigPath, yamlCfg); err != nil {
			return corebridge.Migration{}, fmt.Errorf("failed to update status to Preparing-For-Copy: %w", err)
		}
		m.logger.Info().
			Str("migration_id", migrationID).
			Msg("updated status to Preparing-For-Copy")
	}

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

	go func() {
		m.logger.Info().
			Str("migration_id", migrationID).
			Str("phase", phase).
			Msg("starting migration phase")

		if phase == "copy" {
			err := m.migrationsMgr.RunCopyPhase(record, dbPath, meta.ConfigPath, migOpts, spectraConfigPath)
			if err != nil {
				m.logger.Error().
					Err(err).
					Str("migration_id", migrationID).
					Str("phase", phase).
					Msg("failed to start copy phase")
			}
		} else {
			_, err := m.StartMigration(ctx, req)
			if err != nil {
				m.logger.Error().
					Err(err).
					Str("migration_id", migrationID).
					Str("phase", phase).
					Msg("failed to start migration phase")
			}
		}
	}()

	return corebridge.Migration{
		ID:      migrationID,
		Status:  "preparing",
		Success: true,
	}, nil
}

func (m *Manager) GetMigrationStatus(ctx context.Context, id string) (corebridge.Status, error) {
	migStatus, err := m.migrationsMgr.GetMigrationStatus(ctx, id)
	if err != nil {
		return corebridge.Status{}, err
	}

	var status string
	metaMgr := metadata.NewManager(m.cfg.Runtime.DataDir)
	meta, err := metaMgr.GetMigrationMetadata(id)
	if err == nil && meta.ConfigPath != "" {
		yamlCfg, err := migration.LoadMigrationConfig(meta.ConfigPath)
		if err == nil && yamlCfg.State.Status != "" {
			status = yamlCfg.State.Status
		}
	}

	if status == "" {
		status = migStatus.Status
	}

	return corebridge.Status{
		Migration: corebridge.Migration{
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

func (m *Manager) LoadMigration(ctx context.Context, migrationID string) (corebridge.Migration, error) {
	metaMgr := metadata.NewManager(m.cfg.Runtime.DataDir)
	meta, err := metaMgr.GetMigrationMetadata(migrationID)
	if err != nil {
		return corebridge.Migration{}, fmt.Errorf("migration metadata not found: %w", err)
	}

	if meta.ConfigPath == "" {
		return corebridge.Migration{}, fmt.Errorf("migration %s has no config path", migrationID)
	}

	mg, err := m.migrationsMgr.LoadMigrationFromConfigPath(ctx, migrationID, meta.ConfigPath)
	if err != nil {
		return corebridge.Migration{}, err
	}

	return corebridge.Migration{
		ID:            mg.ID,
		SourceID:      mg.SourceID,
		DestinationID: mg.DestinationID,
		StartedAt:     mg.StartedAt,
		Status:        mg.Status,
	}, nil
}

func (m *Manager) StopMigration(ctx context.Context, migrationID string) (corebridge.Status, error) {
	result, err := m.migrationsMgr.StopMigration(ctx, migrationID)
	if err != nil {
		return corebridge.Status{}, err
	}

	status, err := m.GetMigrationStatus(ctx, migrationID)
	if err != nil {
		status = corebridge.Status{
			Migration: corebridge.Migration{
				ID:     migrationID,
				Status: corebridge.MigrationStatusSuspended,
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
