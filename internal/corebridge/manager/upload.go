package manager

import (
	"context"

	"codeberg.org/Sylos/Sylos-API/internal/corebridge"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge/database"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge/metadata"
)

func (m *Manager) UploadMigrationDB(ctx context.Context, migrationID string, data []byte, overwrite bool) (corebridge.UploadMigrationDBResponse, error) {
	resp, err := database.UploadMigrationDB(ctx, m.logger, m.cfg.Runtime.DataDir, migrationID, data, overwrite)
	if err != nil {
		return corebridge.UploadMigrationDBResponse{}, err
	}
	if resp.Success && resp.Path != "" {
		m.persistUploadedDBMetadata(migrationID, resp.Path)
	}
	return corebridge.UploadMigrationDBResponse{
		Success: resp.Success,
		Error:   resp.Error,
		Path:    resp.Path,
	}, nil
}

func (m *Manager) UploadMigrationYAML(ctx context.Context, migrationID string, data []byte, overwrite bool) (corebridge.UploadMigrationDBResponse, error) {
	resp, err := database.UploadMigrationYAML(ctx, m.logger, m.cfg.Runtime.DataDir, migrationID, data, overwrite)
	if err != nil {
		return corebridge.UploadMigrationDBResponse{}, err
	}
	return corebridge.UploadMigrationDBResponse{
		Success: resp.Success,
		Error:   resp.Error,
		Path:    resp.Path,
	}, nil
}

func (m *Manager) UploadMigrationData(ctx context.Context, migrationID string, zipData []byte, overwrite bool) (corebridge.UploadMigrationDBResponse, error) {
	resp, err := database.UploadMigrationData(ctx, m.logger, m.cfg.Runtime.DataDir, migrationID, zipData, overwrite)
	if err != nil {
		return corebridge.UploadMigrationDBResponse{}, err
	}
	return corebridge.UploadMigrationDBResponse{
		Success: resp.Success,
		Error:   resp.Error,
		Path:    resp.Path,
	}, nil
}

func (m *Manager) UploadByType(ctx context.Context, migrationID, uploadType string, data []byte, overwrite bool) (corebridge.UploadMigrationDBResponse, error) {
	resp, err := database.UploadByType(ctx, m.logger, m.cfg.Runtime.DataDir, migrationID, uploadType, data, overwrite)
	if err != nil {
		return corebridge.UploadMigrationDBResponse{}, err
	}
	if resp.Success && uploadType == database.UploadTypeDB && resp.Path != "" {
		m.persistUploadedDBMetadata(migrationID, resp.Path)
	}
	return corebridge.UploadMigrationDBResponse{
		Success: resp.Success,
		Error:   resp.Error,
		Path:    resp.Path,
	}, nil
}

func (m *Manager) ListMigrationDBs(ctx context.Context) ([]corebridge.MigrationDBInfo, error) {
	dbs, err := database.ListMigrationDBs(ctx, m.logger, m.cfg.Runtime.DataDir)
	if err != nil {
		return nil, err
	}
	result := make([]corebridge.MigrationDBInfo, len(dbs))
	for i, db := range dbs {
		result[i] = corebridge.MigrationDBInfo{
			Filename:   db.Filename,
			Path:       db.Path,
			Size:       db.Size,
			ModifiedAt: db.ModifiedAt,
		}
	}
	return result, nil
}

func (m *Manager) persistUploadedDBMetadata(migrationID, databasePath string) {
	metaMgr := metadata.NewManager(m.cfg.Runtime.DataDir)
	meta, err := metaMgr.GetMigrationMetadata(migrationID)
	if err != nil {
		meta = metadata.MigrationMetadata{
			ID:           migrationID,
			Name:         migrationID,
			DatabasePath: databasePath,
		}
	} else {
		if meta.Name == "" {
			meta.Name = migrationID
		}
		meta.DatabasePath = databasePath
	}
	if err := metaMgr.UpdateMigrationMetadata(meta); err != nil {
		m.logger.Warn().Err(err).Str("migration_id", migrationID).Str("database_path", databasePath).Msg("failed to persist uploaded DB metadata")
	}
}
