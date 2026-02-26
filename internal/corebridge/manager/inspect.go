package manager

import (
	"context"

	"codeberg.org/Sylos/Migration-Engine/pkg/migration"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge"
)

func (m *Manager) InspectMigrationStatus(_ context.Context, migrationID string) (migration.MigrationStatus, error) {
	mig, err := m.engineMgr.GetMigration(migrationID)
	if err != nil {
		return migration.MigrationStatus{}, err
	}
	if mig == nil {
		return migration.MigrationStatus{}, corebridge.ErrMigrationNotFound
	}
	runtime := mig.GetRuntimeStatus()
	return migration.MigrationStatus{
		SrcTotal:   int(runtime.NodesDiscovered),
		DstTotal:   0,
		SrcPending: int(runtime.TasksPending),
		DstPending: 0,
		SrcFailed:  int(runtime.Errors),
		DstFailed:  0,
	}, nil
}
