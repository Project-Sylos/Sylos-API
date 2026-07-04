package manager

import (
	"encoding/json"
	"fmt"
	"path/filepath"

	"codeberg.org/Sylos/Migration-Engine/pkg/migration"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge/database"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge/services"
	"codeberg.org/Sylos/Sylos-FS/pkg/cloud"
	fslib "codeberg.org/Sylos/Sylos-FS/pkg/fs"
)

func (m *Manager) initializePlanAdapters(migrationID string) error {
	plan := m.rootsMgr.GetPlan(migrationID)
	if plan == nil {
		return nil
	}
	absDir, err := filepath.Abs(database.GetMigrationDir(m.cfg.Runtime.DataDir, migrationID))
	if err != nil {
		return err
	}
	mig, err := m.engineMgr.GetMigration(migrationID, absDir)
	if err != nil || mig == nil {
		return err
	}
	masterKey, err := mig.EnsureEnvelopeMasterKey()
	if err != nil {
		return err
	}
	if plan.SourceAdapter != nil && plan.SourceDefinition.Type == services.ServiceTypeCloud {
		if err := m.serviceMgr.FSManager().InitializeCloudAdapter(plan.SourceAdapter, masterKey, plan.SourceConnectionID); err != nil {
			return fmt.Errorf("initialize source cloud adapter: %w", err)
		}
	}
	if plan.DestinationAdapter != nil && plan.DestinationDefinition.Type == services.ServiceTypeCloud {
		if err := m.serviceMgr.FSManager().InitializeCloudAdapter(plan.DestinationAdapter, masterKey, plan.DestinationConnectionID); err != nil {
			return fmt.Errorf("initialize destination cloud adapter: %w", err)
		}
	}
	return nil
}

func cloudCredsRelPath(connectionID string) string {
	return cloud.CredsRelPath(connectionID)
}

func (m *Manager) rehydrateCloudConnection(migrationID string, binding migration.FSCredentialBinding, def services.ServiceDefinition, masterKey []byte) error {
	if binding.CredsConfRelPath == "" {
		binding.CredsConfRelPath = cloudCredsRelPath(binding.ConnectionID)
	}
	migrationDir, err := m.migrationDirFor(migrationID)
	if err != nil {
		return err
	}
	stored, err := cloud.ReadEncryptedCredentials(migrationDir, binding.CredsConfRelPath, masterKey, binding.ConnectionID)
	if err != nil {
		return err
	}
	credsJSON, err := json.Marshal(stored)
	if err != nil {
		return err
	}
	_, err = m.serviceMgr.RegisterCloudConnection(fslib.CloudConnectionOptions{
		ProviderID:      services.CloudProviderID(def),
		ConnectionID:    binding.ConnectionID,
		MigrationDir:    migrationDir,
		MasterKey:       masterKey,
		CredentialsJSON: credsJSON,
	})
	return err
}
