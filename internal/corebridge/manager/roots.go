package manager

import (
	"context"
	"os"
	"path/filepath"

	"codeberg.org/Sylos/Migration-Engine/pkg/migration"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge/apidb"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge/database"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge/roots"
)

func (m *Manager) SetRoot(ctx context.Context, req corebridge.SetRootRequest) (corebridge.SetRootResponse, error) {
	if err := m.checkPhaseLock(req.MigrationID, "setRoot"); err != nil {
		return corebridge.SetRootResponse{}, err
	}

	migrationID := req.MigrationID
	if migrationID == "" {
		created, err := m.engineMgr.CreateMigration(migration.CreateMigrationConfig{Name: "migration"})
		if err != nil {
			return corebridge.SetRootResponse{}, err
		}
		migrationID = created.ID
		if _, err := m.apiDB.EnsureMigrationKey(migrationID); err != nil {
			return corebridge.SetRootResponse{}, err
		}
		// Per-migration flow: create folder and materialize the migration (DB + row) so Start and polling never race on pending→persist.
		migrationDir := database.GetMigrationDir(m.cfg.Runtime.DataDir, migrationID)
		_ = os.MkdirAll(migrationDir, 0755)
		if _, err := m.GetMigration(ctx, migrationID); err != nil {
			return corebridge.SetRootResponse{}, err
		}
	} else {
		migrationDir := database.GetMigrationDir(m.cfg.Runtime.DataDir, migrationID)
		if err := os.MkdirAll(migrationDir, 0o755); err != nil {
			return corebridge.SetRootResponse{}, err
		}
		absDir, err := filepath.Abs(migrationDir)
		if err != nil {
			return corebridge.SetRootResponse{}, err
		}
		engMig, err := m.getEngineMigration(migrationID)
		if err != nil && err != corebridge.ErrMigrationNotFound {
			return corebridge.SetRootResponse{}, err
		}
		if engMig == nil {
			key, keyErr := m.apiDB.EnsureMigrationKey(migrationID)
			if keyErr != nil {
				return corebridge.SetRootResponse{}, keyErr
			}
			if _, err := m.engineMgr.CreateMigration(migration.CreateMigrationConfig{
				Name:          "migration",
				MigrationDir:  absDir,
				MigrationID:   migrationID,
				EncryptionKey: key,
			}); err != nil {
				return corebridge.SetRootResponse{}, err
			}
		}
		if _, err := m.GetMigration(ctx, migrationID); err != nil {
			return corebridge.SetRootResponse{}, err
		}
	}

	rootsReq := roots.SetRootRequest{
		MigrationID:  migrationID,
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
		Config: req.Config,
	}
	resp, err := m.rootsMgr.SetRoot(ctx, rootsReq)
	if err != nil {
		return corebridge.SetRootResponse{}, err
	}

	if err := m.persistFSCredentialBinding(resp.MigrationID, req.Role); err != nil {
		m.logger.Warn().Err(err).Str("migration_id", resp.MigrationID).Msg("persist fs credential binding")
	}
	if err := m.initializePlanAdapters(resp.MigrationID); err != nil {
		m.logger.Warn().Err(err).Str("migration_id", resp.MigrationID).Msg("initialize fs adapters")
	}

	existingMeta, err := m.getMigrationRecord(resp.MigrationID)
	isNewMigration := true
	if err == nil && existingMeta.ID != "" {
		isNewMigration = existingMeta.IsNewMigration
	}

	rec := apidb.MigrationRecord{
		ID:             resp.MigrationID,
		Name:           resp.MigrationID,
		DatabasePath:   resp.DatabasePath,
		IsNewMigration: isNewMigration,
	}
	if err := m.upsertMigrationRecord(rec); err != nil {
		m.logger.Warn().Err(err).Str("migration_id", resp.MigrationID).Msg("failed to update migration metadata when setting root")
	}

	return corebridge.SetRootResponse{
		MigrationID:             resp.MigrationID,
		Role:                    resp.Role,
		Ready:                   resp.Ready,
		DatabasePath:            resp.DatabasePath,
		RootSummary:             resp.RootSummary,
		SourceConnectionID:      resp.SourceConnectionID,
		DestinationConnectionID: resp.DestinationConnectionID,
	}, nil
}
