package manager

import (
	"context"
	"fmt"

	"codeberg.org/Sylos/Sylos-API/internal/corebridge"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge/database"
)

// CleanSlateResponse summarizes what was cleared.
type CleanSlateResponse struct {
	Message string `json:"message"`
}

// CleanSlate stops active migrations and removes all recorded migration data from disk and the API database.
// User accounts, provider OAuth app configuration, and the install master key are preserved.
func (m *Manager) CleanSlate(ctx context.Context) (CleanSlateResponse, error) {
	m.mu.Lock()
	runningIDs := make([]string, 0, len(m.runtimeByID))
	for id := range m.runtimeByID {
		runningIDs = append(runningIDs, id)
	}
	m.mu.Unlock()

	for _, id := range runningIDs {
		if _, err := m.StopMigration(ctx, id); err != nil {
			m.logger.Warn().Err(err).Str("migration_id", id).Msg("clean slate: stop migration")
		}
	}

	m.rootsMgr.ClearAllPlans()
	if err := m.engineMgr.ResetState(); err != nil {
		return CleanSlateResponse{}, fmt.Errorf("reset migration engine: %w", err)
	}

	m.mu.Lock()
	m.runtimeByID = make(map[string]*runtimeMigration)
	m.progressByID = make(map[string]map[string]chan corebridge.ProgressEvent)
	m.mu.Unlock()

	m.connMgr.ClearAll()

	if m.apiDB != nil {
		if err := m.apiDB.DeleteAllMigrationRegistry(); err != nil {
			return CleanSlateResponse{}, fmt.Errorf("clear migration registry: %w", err)
		}
	}

	if err := database.CleanMigrationData(m.cfg.Runtime.DataDir); err != nil {
		return CleanSlateResponse{}, fmt.Errorf("clean migration data files: %w", err)
	}

	m.logger.Warn().Msg("clean slate: all migration data removed by admin request")

	return CleanSlateResponse{
		Message: "All migration data has been removed. User accounts and cloud provider settings were kept.",
	}, nil
}
