package manager

import (
	"context"
	"os"

	"codeberg.org/Sylos/Migration-Engine/pkg/migration"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge/database"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge/metadata"
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
		// Per-migration flow: create folder and materialize the migration (DB + row) so Start and polling never race on pending→persist.
		migrationDir := database.GetMigrationDir(m.cfg.Runtime.DataDir, migrationID)
		_ = os.MkdirAll(migrationDir, 0755)
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

	metaMgr := metadata.NewManager(m.cfg.Runtime.DataDir)
	existingMeta, err := metaMgr.GetMigrationMetadata(resp.MigrationID)
	isNewMigration := true
	if err == nil {
		isNewMigration = existingMeta.IsNewMigration
	}

	meta := metadata.MigrationMetadata{
		ID:             resp.MigrationID,
		Name:           resp.MigrationID,
		DatabasePath:   resp.DatabasePath,
		IsNewMigration: isNewMigration,
	}
	if err := metaMgr.UpdateMigrationMetadata(meta); err != nil {
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
