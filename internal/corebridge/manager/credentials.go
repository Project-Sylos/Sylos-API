package manager

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"strings"

	"codeberg.org/Sylos/Migration-Engine/pkg/migration"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge/database"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge/services"
	"codeberg.org/Sylos/Sylos-FS/pkg/cloud"
	fstypes "codeberg.org/Sylos/Sylos-FS/pkg/types"
)

// persistFSCredentialBinding writes envelope key (if needed) and one side's binding after SetRoot.
func (m *Manager) persistFSCredentialBinding(migrationID, role string) error {
	absDir, err := filepath.Abs(database.GetMigrationDir(m.cfg.Runtime.DataDir, migrationID))
	if err != nil {
		return err
	}
	mig, err := m.engineMgr.GetMigration(migrationID, absDir)
	if err != nil {
		return err
	}
	if mig == nil {
		return fmt.Errorf("migration %q not found for credential persist", migrationID)
	}
	if _, err := mig.EnsureEnvelopeMasterKey(); err != nil {
		return err
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
		if plan.SourceDefinition.Type == services.ServiceTypeCloud && plan.SourceConnectionID != "" {
			binding.CredsConfRelPath = cloud.CredsRelPath(plan.SourceConnectionID)
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
		if plan.DestinationDefinition.Type == services.ServiceTypeCloud && plan.DestinationConnectionID != "" {
			binding.CredsConfRelPath = cloud.CredsRelPath(plan.DestinationConnectionID)
		}
	default:
		return nil
	}
	return mig.UpsertFSCredentialBinding(binding)
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
	masterKey, err := mig.GetEnvelopeMasterKey()
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		return err
	}
	bindings, err := mig.ListFSCredentialBindings()
	if err != nil || len(bindings) == 0 {
		return err
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
					if _, regErr := m.serviceMgr.RegisterSpectraSession(path, b.ConnectionID); regErr != nil {
						m.logger.Warn().Err(regErr).Str("migration_id", migrationID).Str("connection_id", b.ConnectionID).Msg("rehydrate: RegisterSpectraSession")
						continue
					}
					spectraRegistered[b.ConnectionID] = true
				}
			}
			if def.Type == services.ServiceTypeCloud {
				if regErr := m.rehydrateCloudConnection(migrationID, b, def, masterKey); regErr != nil {
					m.logger.Warn().Err(regErr).Str("migration_id", migrationID).Str("connection_id", b.ConnectionID).Msg("rehydrate: cloud connection")
					continue
				}
			}
			var folder fstypes.Folder
			if err := json.Unmarshal([]byte(b.RootFolderJSON), &folder); err != nil {
				m.logger.Warn().Err(err).Str("migration_id", migrationID).Str("role", b.Role).Msg("rehydrate: root folder json")
				continue
			}
			adapter, release, err := m.serviceMgr.AcquireAdapter(def, folder, b.ConnectionID)
			if err != nil {
				m.logger.Warn().Err(err).Str("migration_id", migrationID).Str("role", b.Role).Msg("rehydrate: AcquireAdapter")
				continue
			}
			if err := adapter.Initialize(masterKey, b.ConnectionID); err != nil {
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
