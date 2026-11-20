package manager

import (
	"context"

	"codeberg.org/Sylos/Sylos-API/internal/corebridge"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge/apidb"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge/migrationfiles"
)

func (m *Manager) UploadByType(ctx context.Context, migrationID, uploadType string, data []byte, overwrite bool) (corebridge.UploadMigrationDBResponse, error) {
	resp, err := migrationfiles.UploadByType(ctx, m.logger, m.cfg.Runtime.MigrationDBStorageDir, migrationID, uploadType, data, overwrite)
	if err != nil {
		return corebridge.UploadMigrationDBResponse{}, err
	}
	out := corebridge.UploadMigrationDBResponse{
		Success: resp.Success,
		Error:   resp.Error,
		Path:    resp.Path,
	}
	if resp.Success && resp.Path != "" &&
		(uploadType == migrationfiles.UploadTypeDB || uploadType == migrationfiles.UploadTypeOps) {
		id := migrationID
		if uploadType == migrationfiles.UploadTypeOps {
			id = migrationfiles.NormalizeMigrationID(migrationID)
		}
		m.persistUploadedDBMetadata(id, resp.Path)
		if uploadType == migrationfiles.UploadTypeOps {
			if _, keyErr := m.apiDB.EnsureMigrationKey(id); keyErr != nil {
				m.logger.Warn().Err(keyErr).Str("migration_id", id).Msg("ensure migration key after ops import")
			}
			report, scanErr := m.scanImportedOps(id)
			if scanErr != nil {
				m.logger.Warn().Err(scanErr).Str("migration_id", id).Msg("ops import attention scan failed")
			} else {
				out.ImportReport = &report
			}
		}
	}
	return out, nil
}

func (m *Manager) ListMigrationDBs(ctx context.Context) ([]corebridge.MigrationDBInfo, error) {
	dbs, err := migrationfiles.ListMigrationDBs(ctx, m.logger, m.cfg.Runtime.MigrationDBStorageDir)
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
	rec, err := m.getMigrationRecord(migrationID)
	if err != nil || rec.ID == "" {
		rec = apidb.MigrationRecord{
			ID:           migrationID,
			Name:         migrationID,
			DatabasePath: databasePath,
		}
	} else {
		if rec.Name == "" {
			rec.Name = migrationID
		}
		rec.DatabasePath = databasePath
	}
	if err := m.upsertMigrationRecord(rec); err != nil {
		m.logger.Warn().Err(err).Str("migration_id", migrationID).Str("database_path", databasePath).Msg("failed to persist uploaded DB metadata")
	}
}
