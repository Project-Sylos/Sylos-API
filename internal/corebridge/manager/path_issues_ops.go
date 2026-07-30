package manager

import (
	"context"

	"codeberg.org/Sylos/Migration-Engine/pkg/migration"
)

// PathMigration loads a migration and syncs path-check providers from the root plan.
func (m *Manager) PathMigration(ctx context.Context, migrationID string) (*migration.Migration, error) {
	mig, err := m.GetMigration(ctx, migrationID)
	if err != nil {
		return nil, err
	}
	m.SyncPathCheckProviders(migrationID, mig)
	return mig, nil
}
