package manager

import (
	"context"
	"encoding/json"
	"fmt"

	"codeberg.org/Sylos/Migration-Engine/pkg/migration"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge/services"
	fslib "codeberg.org/Sylos/Sylos-FS/pkg/fs"
)

func (m *Manager) initializePlanAdapters(migrationID string) error {
	plan := m.rootsMgr.GetPlan(migrationID)
	if plan == nil {
		return nil
	}
	mig, err := m.GetMigration(context.Background(), migrationID)
	if err != nil || mig == nil {
		return err
	}
	if plan.SourceAdapter != nil && plan.SourceDefinition.Type == services.ServiceTypeCloud {
		if err := m.persistLiveCloudCredentials(mig, plan.SourceConnectionID); err != nil {
			m.logger.Warn().Err(err).Str("migration_id", migrationID).Str("connection_id", plan.SourceConnectionID).Msg("persist source cloud credentials")
		}
		if err := m.serviceMgr.FS.InitializeCloudAdapter(plan.SourceAdapter, nil, plan.SourceConnectionID); err != nil {
			return fmt.Errorf("initialize source cloud adapter: %w", err)
		}
	}
	if plan.DestinationAdapter != nil && plan.DestinationDefinition.Type == services.ServiceTypeCloud {
		if err := m.persistLiveCloudCredentials(mig, plan.DestinationConnectionID); err != nil {
			m.logger.Warn().Err(err).Str("migration_id", migrationID).Str("connection_id", plan.DestinationConnectionID).Msg("persist destination cloud credentials")
		}
		if err := m.serviceMgr.FS.InitializeCloudAdapter(plan.DestinationAdapter, nil, plan.DestinationConnectionID); err != nil {
			return fmt.Errorf("initialize destination cloud adapter: %w", err)
		}
	}
	return nil
}

func (m *Manager) rehydrateCloudConnection(migrationID string, binding migration.FSCredentialBinding, def services.ServiceDefinition, mig *migration.Migration) error {
	stored, err := m.loadStoredCloudCredentials(migrationID, mig, binding)
	if err != nil {
		return err
	}
	credsJSON, err := json.Marshal(stored)
	if err != nil {
		return err
	}
	_, err = m.serviceMgr.FS.RegisterCloudConnection(fslib.CloudConnectionOptions{
		ProviderID:         services.CloudProviderID(def),
		ConnectionID:       binding.ConnectionID,
		CredentialsJSON:    credsJSON,
		PersistCredentials: m.cloudCredentialsPersistHook(mig, binding.ConnectionID),
	})
	return err
}
