package manager

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"codeberg.org/Sylos/Migration-Engine/pkg/migration"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge/services"
	"codeberg.org/Sylos/Sylos-FS/pkg/cloud"
	fslib "codeberg.org/Sylos/Sylos-FS/pkg/fs"
	fstypes "codeberg.org/Sylos/Sylos-FS/pkg/types"
)

// persistFSCredentialBinding writes one side's binding after SetRoot.
// For cloud/SFTP connections it also persists OAuth/form credentials into the migration DB
// (tokens are often received before the migration exists, so PostProviderTokens may have skipped persist).
func (m *Manager) persistFSCredentialBinding(migrationID, role string) error {
	mig, err := m.GetMigration(context.Background(), migrationID)
	if err != nil {
		return err
	}
	if mig == nil {
		return fmt.Errorf("migration %q not found for credential persist", migrationID)
	}
	plan := m.rootsMgr.GetPlan(migrationID)
	if plan == nil {
		return nil
	}
	role = strings.ToLower(strings.TrimSpace(role))
	binding := migration.FSCredentialBinding{Role: role}
	switch role {
	case migration.FSCredentialRoleSource:
		binding.ConnectionID = plan.SourceConnectionID
		binding.ServiceID = plan.SourceDefinition.ID
		raw, err := json.Marshal(plan.SourceRoot)
		if err != nil {
			return err
		}
		binding.RootFolderJSON = string(raw)
		if plan.SourceDefinition.Type == services.ServiceTypeSpectra {
			if _, ok, _ := services.LoadSpectraConfigOverride(m.cfg.Runtime.DataDir, migrationID); ok {
				binding.CredsConfRelPath = "spectra-config.json"
			}
		}
	case migration.FSCredentialRoleDestination:
		binding.ConnectionID = plan.DestinationConnectionID
		binding.ServiceID = plan.DestinationDefinition.ID
		raw, err := json.Marshal(plan.DestinationRoot)
		if err != nil {
			return err
		}
		binding.RootFolderJSON = string(raw)
		if plan.DestinationDefinition.Type == services.ServiceTypeSpectra {
			if _, ok, _ := services.LoadSpectraConfigOverride(m.cfg.Runtime.DataDir, migrationID); ok {
				binding.CredsConfRelPath = "spectra-config.json"
			}
		}
	default:
		return nil
	}
	if err := mig.RunStore(func(s *migration.Store) error {
		return s.UpsertFSCredentialBinding(binding)
	}); err != nil {
		return err
	}
	if binding.ConnectionID != "" {
		if err := m.persistLiveCloudCredentials(mig, binding.ConnectionID); err != nil {
			m.logger.Warn().Err(err).
				Str("migration_id", migrationID).
				Str("connection_id", binding.ConnectionID).
				Msg("persist cloud credentials on set-root")
		}
		if rec, ok := m.connMgr.Get(binding.ConnectionID); ok && rec.MigrationID == "" {
			rec.MigrationID = migrationID
			m.connMgr.Set(binding.ConnectionID, rec)
		}
	}
	return nil
}

// persistLiveCloudCredentials copies StoredCredentials from the in-memory FS session into the migration DB.
func (m *Manager) persistLiveCloudCredentials(mig *migration.Migration, connectionID string) error {
	if mig == nil || connectionID == "" || m.serviceMgr == nil || m.serviceMgr.FS == nil {
		return nil
	}
	credsJSON, err := m.serviceMgr.FS.ExportCloudCredentialsJSON(connectionID)
	if err != nil {
		return err
	}
	if err := m.persistOAuthCredentials(mig, connectionID, credsJSON); err != nil {
		return err
	}
	// Attach rotation persist for providers like Box (tokens may have been registered before the migration existed).
	return m.serviceMgr.FS.SetCloudCredentialsPersist(connectionID, m.cloudCredentialsPersistHook(mig, connectionID))
}

// ensureFSAdaptersRehydrated rebuilds in-memory FS adapters when a real filesystem operation is needed.
func (m *Manager) ensureFSAdaptersRehydrated(migrationID string) error {
	mig, err := m.GetMigration(context.Background(), migrationID)
	if err != nil {
		return err
	}
	return m.rehydrateFSAdaptersIfNeeded(migrationID, mig)
}

// rehydrateFSAdaptersIfNeeded rebuilds in-memory FS adapters from the migration DB after API restart.
func (m *Manager) rehydrateFSAdaptersIfNeeded(migrationID string, mig *migration.Migration) error {
	if mig == nil {
		return nil
	}
	plan := m.rootsMgr.GetPlan(migrationID)
	if plan != nil && plan.SourceAdapter != nil && plan.DestinationAdapter != nil {
		return nil
	}
	bindings, err := mig.ListFSCredentialBindings()
	if err != nil {
		return err
	}
	if len(bindings) == 0 {
		return fmt.Errorf("no persisted FS roots/credentials for migration %s (cannot restore after restart)", migrationID)
	}
	order := []string{migration.FSCredentialRoleSource, migration.FSCredentialRoleDestination}
	spectraRegistered := make(map[string]bool)
	for _, wantRole := range order {
		for _, b := range bindings {
			if b.Role != wantRole || b.ServiceID == "" || b.RootFolderJSON == "" {
				continue
			}
			if plan != nil {
				if b.Role == migration.FSCredentialRoleSource && plan.SourceAdapter != nil {
					continue
				}
				if b.Role == migration.FSCredentialRoleDestination && plan.DestinationAdapter != nil {
					continue
				}
			}
			def, err := m.serviceMgr.GetServiceDefinition(b.ServiceID)
			if err != nil {
				m.logger.Warn().Err(err).Str("migration_id", migrationID).Str("role", b.Role).Str("service_id", b.ServiceID).Msg("rehydrate: unknown service")
				continue
			}
			if def.Type == services.ServiceTypeSpectra {
				if !spectraRegistered[b.ConnectionID] {
					path, ok, loadErr := services.LoadSpectraConfigOverride(m.cfg.Runtime.DataDir, migrationID)
					if loadErr != nil {
						m.logger.Warn().Err(loadErr).Str("migration_id", migrationID).Msg("rehydrate: load spectra config override")
						continue
					}
					if !ok {
						m.logger.Warn().Str("migration_id", migrationID).Msg("rehydrate: missing spectra-config.json for Spectra binding")
						continue
					}
					if _, regErr := m.serviceMgr.FS.RegisterSpectraSession(path, b.ConnectionID); regErr != nil {
						m.logger.Warn().Err(regErr).Str("migration_id", migrationID).Str("connection_id", b.ConnectionID).Msg("rehydrate: RegisterSpectraSession")
						continue
					}
					spectraRegistered[b.ConnectionID] = true
				}
			}
			if def.Type == services.ServiceTypeCloud {
				if regErr := m.rehydrateCloudConnection(migrationID, b, def, mig); regErr != nil {
					m.logger.Warn().Err(regErr).Str("migration_id", migrationID).Str("connection_id", b.ConnectionID).Msg("rehydrate: cloud connection")
					continue
				}
			}
			var folder fstypes.Folder
			if err := json.Unmarshal([]byte(b.RootFolderJSON), &folder); err != nil {
				m.logger.Warn().Err(err).Str("migration_id", migrationID).Str("role", b.Role).Msg("rehydrate: root folder json")
				continue
			}
			adapter, release, err := m.serviceMgr.FS.AcquireAdapter(def, folder, b.ConnectionID)
			if err != nil {
				m.logger.Warn().Err(err).Str("migration_id", migrationID).Str("role", b.Role).Msg("rehydrate: AcquireAdapter")
				continue
			}
			if err := adapter.Initialize(nil, b.ConnectionID); err != nil {
				release()
				m.logger.Warn().Err(err).Str("migration_id", migrationID).Str("role", b.Role).Msg("rehydrate: Initialize")
				continue
			}
			if err := m.rootsMgr.ApplyRehydratedSide(migrationID, b.Role, def, folder, b.ConnectionID, adapter, release); err != nil {
				release()
				m.logger.Warn().Err(err).Str("migration_id", migrationID).Str("role", b.Role).Msg("rehydrate: ApplyRehydratedSide")
			}
		}
	}
	return nil
}

func (m *Manager) persistOAuthCredentials(mig *migration.Migration, connectionID string, credsJSON []byte) error {
	if mig == nil || connectionID == "" || len(credsJSON) == 0 {
		return nil
	}
	return mig.RunStore(func(s *migration.Store) error {
		return s.UpsertOAuthCredentials(connectionID, credsJSON)
	})
}

func (m *Manager) cloudCredentialsPersistHook(mig *migration.Migration, connectionID string) func(cloud.StoredCredentials) error {
	if mig == nil || connectionID == "" {
		return nil
	}
	return func(stored cloud.StoredCredentials) error {
		credsJSON, err := json.Marshal(stored)
		if err != nil {
			return err
		}
		return m.persistOAuthCredentials(mig, connectionID, credsJSON)
	}
}

func (m *Manager) loadStoredCloudCredentials(_ string, mig *migration.Migration, binding migration.FSCredentialBinding) (cloud.StoredCredentials, error) {
	if binding.ConnectionID == "" {
		return cloud.StoredCredentials{}, fmt.Errorf("connection id required")
	}
	if mig == nil {
		return cloud.StoredCredentials{}, fmt.Errorf("no stored credentials for connection %q", binding.ConnectionID)
	}
	raw, err := mig.GetOAuthCredentials(binding.ConnectionID)
	if err != nil || len(raw) == 0 {
		return cloud.StoredCredentials{}, fmt.Errorf("no stored credentials for connection %q", binding.ConnectionID)
	}
	var stored cloud.StoredCredentials
	if err := json.Unmarshal(raw, &stored); err != nil {
		return cloud.StoredCredentials{}, fmt.Errorf("decode stored credentials: %w", err)
	}
	return stored, nil
}

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
