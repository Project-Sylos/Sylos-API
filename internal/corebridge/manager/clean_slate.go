package manager

import (
	"context"
	"fmt"

	"codeberg.org/Sylos/Sylos-API/internal/corebridge"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge/migrationfiles"
	"codeberg.org/Sylos/Sylos-API/pkg/oauthcreds"
)

// AdminActionResponse summarizes a destructive admin operation.
type AdminActionResponse struct {
	Message string `json:"message"`
}

func (m *Manager) clearMigrationRuntimeAndDisk(ctx context.Context) error {
	m.mu.Lock()
	runningIDs := make([]string, 0, len(m.runtimeByID))
	for id := range m.runtimeByID {
		runningIDs = append(runningIDs, id)
	}
	m.mu.Unlock()

	for _, id := range runningIDs {
		if _, err := m.StopMigration(ctx, id); err != nil {
			m.logger.Warn().Err(err).Str("migration_id", id).Msg("clear migrations: stop migration")
		}
	}

	m.rootsMgr.ClearAllPlans()
	if err := m.engineMgr.ResetState(); err != nil {
		return fmt.Errorf("reset migration engine: %w", err)
	}

	m.mu.Lock()
	m.runtimeByID = make(map[string]*runtimeMigration)
	m.progressByID = make(map[string]map[string]chan corebridge.ProgressEvent)
	m.mu.Unlock()

	m.connMgr.ClearAll()

	if m.apiDB != nil {
		if err := m.apiDB.DeleteAllMigrationRegistry(); err != nil {
			return fmt.Errorf("clear migration registry: %w", err)
		}
	}

	if err := migrationfiles.CleanMigrationData(m.cfg.Runtime.DataDir); err != nil {
		return fmt.Errorf("clean migration data files: %w", err)
	}

	return nil
}

// ClearAllMigrations stops active migrations and removes all migration folders, migration DB files,
// and registry entries. User accounts, cloud provider app settings, and the install master key are preserved.
func (m *Manager) ClearAllMigrations(ctx context.Context) (AdminActionResponse, error) {
	if err := m.clearMigrationRuntimeAndDisk(ctx); err != nil {
		return AdminActionResponse{}, err
	}

	m.logger.Warn().Msg("clear migrations: all migration data removed by admin request")

	return AdminActionResponse{
		Message: "All migration data has been removed. User accounts and cloud provider settings were kept.",
	}, nil
}

// WipeInstall removes all migration data plus users, cloud provider OAuth apps, and install config.
// The install master key and encrypted sylos.duckdb file are preserved; complete initial setup again afterward.
func (m *Manager) WipeInstall(ctx context.Context) (AdminActionResponse, error) {
	if err := m.clearMigrationRuntimeAndDisk(ctx); err != nil {
		return AdminActionResponse{}, err
	}

	if m.apiDB != nil {
		if err := m.apiDB.WipeInstallUserData(); err != nil {
			return AdminActionResponse{}, fmt.Errorf("wipe install user data: %w", err)
		}
	}

	m.SetOAuthCreds(oauthcreds.Config{})

	m.logger.Warn().Msg("wipe install: all migration and user data removed by admin request")

	return AdminActionResponse{
		Message: "All migration data, user accounts, and cloud provider settings have been removed. Complete setup again to continue using Sylos.",
	}, nil
}
