package manager

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"codeberg.org/Sylos/Migration-Engine/pkg/migration"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge/migrationfiles"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge/roots"
)

func (m *Manager) SetRoot(ctx context.Context, req corebridge.SetRootRequest) (corebridge.SetRootResponse, error) {
	quiet := req.Reauth || m.migrationNeedsAttention(req.MigrationID)
	if !quiet {
		if err := m.checkPhaseLock(req.MigrationID, "setRoot"); err != nil {
			return corebridge.SetRootResponse{}, err
		}
	}

	rootsReq := roots.SetRootRequest{
		MigrationID:  req.MigrationID,
		Role:         req.Role,
		ServiceID:    req.ServiceID,
		ConnectionID: req.ConnectionID,
		Root: roots.FolderDescriptor{
			ID:           req.Root.ID,
			ParentID:     req.Root.ParentID,
			ParentPath:   req.Root.ParentPath,
			DisplayName:  req.Root.DisplayName,
			LocationPath: req.Root.LocationPath,
			LastUpdated:  req.Root.LastUpdated,
			DepthLevel:   req.Root.DepthLevel,
			Type:         req.Root.Type,
		},
		Config:      req.Config,
		ExcludedIDs: req.ExcludedIds,
		Quiet:       quiet,
	}
	// Quiet re-auth must not rewrite include trees; ignore children/excludes.
	if !quiet && len(req.Children) > 0 {
		rootsReq.Children = mapRootChildPlans(req.Children)
	}
	if err := m.rootsMgr.ValidateMigrationRoot(req.ServiceID, rootsReq.Root); err != nil {
		return corebridge.SetRootResponse{}, err
	}

	migrationID := req.MigrationID
	if migrationID == "" {
		if quiet {
			return corebridge.SetRootResponse{}, fmt.Errorf("reauth requires an existing migrationId")
		}
		created, err := m.engineMgr.CreateMigration(migration.CreateMigrationConfig{Name: "migration"})
		if err != nil {
			return corebridge.SetRootResponse{}, err
		}
		migrationID = created.ID
		if _, err := m.apiDB.EnsureMigrationKey(migrationID); err != nil {
			return corebridge.SetRootResponse{}, err
		}
		// Per-migration flow: create folder and materialize the migration (DB + row) so Start and polling never race on pending→persist.
		migrationDir := migrationfiles.MigrationDir(m.cfg.Runtime.MigrationDBStorageDir, migrationID)
		_ = os.MkdirAll(migrationDir, 0755)
		if _, err := m.GetMigration(ctx, migrationID); err != nil {
			return corebridge.SetRootResponse{}, err
		}
	} else {
		migrationDir := migrationfiles.MigrationDir(m.cfg.Runtime.MigrationDBStorageDir, migrationID)
		if err := os.MkdirAll(migrationDir, 0o755); err != nil {
			return corebridge.SetRootResponse{}, err
		}
		absDir, err := filepath.Abs(migrationDir)
		if err != nil {
			return corebridge.SetRootResponse{}, err
		}
		engMig, err := m.GetMigration(ctx, migrationID)
		if err != nil && err != corebridge.ErrMigrationNotFound {
			return corebridge.SetRootResponse{}, err
		}
		if engMig == nil {
			if quiet {
				return corebridge.SetRootResponse{}, fmt.Errorf("reauth migration %q not found", migrationID)
			}
			if _, keyErr := m.apiDB.EnsureMigrationKey(migrationID); keyErr != nil {
				return corebridge.SetRootResponse{}, keyErr
			}
			if _, err := m.engineMgr.CreateMigration(migration.CreateMigrationConfig{
				Name:         "migration",
				MigrationDir: absDir,
				MigrationID:  migrationID,
			}); err != nil {
				return corebridge.SetRootResponse{}, err
			}
		}
		if _, err := m.GetMigration(ctx, migrationID); err != nil {
			return corebridge.SetRootResponse{}, err
		}
	}

	if quiet {
		if err := m.validateQuietReauthRoot(migrationID, req.Role, req.ServiceID, req.Root.ID); err != nil {
			return corebridge.SetRootResponse{}, err
		}
	}

	phaseBefore := ""
	prevConnectionID := ""
	if quiet {
		if mig, err := m.GetMigration(ctx, migrationID); err == nil && mig != nil {
			phaseBefore = mig.Phase()
			if binding, bErr := mig.GetFSCredentialBinding(req.Role); bErr == nil && binding != nil {
				prevConnectionID = strings.TrimSpace(binding.ConnectionID)
			}
		}
	}

	rootsReq.MigrationID = migrationID
	resp, err := m.rootsMgr.SetRoot(ctx, rootsReq)
	if err != nil {
		return corebridge.SetRootResponse{}, err
	}

	if err := m.persistFSCredentialBinding(resp.MigrationID, req.Role); err != nil {
		if quiet {
			return corebridge.SetRootResponse{}, fmt.Errorf("persist reauth credentials: %w", err)
		}
		m.logger.Warn().Err(err).Str("migration_id", resp.MigrationID).Msg("persist fs credential binding")
	}
	if quiet {
		newConnectionID := strings.TrimSpace(req.ConnectionID)
		if newConnectionID == "" {
			if req.Role == "destination" {
				newConnectionID = strings.TrimSpace(resp.DestinationConnectionID)
			} else {
				newConnectionID = strings.TrimSpace(resp.SourceConnectionID)
			}
		}
		if prevConnectionID != "" && newConnectionID != "" && prevConnectionID != newConnectionID {
			m.retireReplacedConnection(resp.MigrationID, prevConnectionID)
		}
	}
	if err := m.initializePlanAdapters(resp.MigrationID); err != nil {
		m.logger.Warn().Err(err).Str("migration_id", resp.MigrationID).Msg("initialize fs adapters")
	}
	if !quiet {
		m.refreshDefaultMigrationName(ctx, resp.MigrationID)
	}

	existingMeta, err := m.getMigrationRecord(resp.MigrationID)
	isNewMigration := true
	if err == nil && existingMeta.ID != "" {
		isNewMigration = existingMeta.IsNewMigration
	}

	recName := resp.MigrationID
	if mig, migErr := m.GetMigration(ctx, resp.MigrationID); migErr == nil && mig != nil {
		if n := strings.TrimSpace(mig.GetName()); n != "" {
			recName = n
		}
	} else if existingMeta.ID != "" && existingMeta.Name != "" {
		recName = existingMeta.Name
	}
	rec := existingMeta
	rec.ID = resp.MigrationID
	rec.Name = recName
	if !quiet || strings.TrimSpace(resp.DatabasePath) != "" {
		rec.DatabasePath = resp.DatabasePath
	}
	rec.IsNewMigration = isNewMigration
	if source, destination, ok := m.cachedMigrationRoots(resp.MigrationID); ok {
		if source != nil {
			rec.SourceRoot = rootSnapshot(source)
		}
		if destination != nil {
			rec.DestinationRoot = rootSnapshot(destination)
		}
	}
	if err := m.upsertMigrationRecord(rec); err != nil {
		m.logger.Warn().Err(err).Str("migration_id", resp.MigrationID).Msg("failed to update migration metadata when setting root")
	}

	if quiet && phaseBefore != "" {
		if mig, err := m.GetMigration(ctx, resp.MigrationID); err == nil && mig != nil {
			if got := mig.Phase(); got != phaseBefore {
				m.logger.Warn().
					Str("migration_id", resp.MigrationID).
					Str("phase_before", phaseBefore).
					Str("phase_after", got).
					Msg("quiet set-root changed engine phase unexpectedly")
			}
		}
	}

	return corebridge.SetRootResponse{
		MigrationID:             resp.MigrationID,
		Role:                    resp.Role,
		Ready:                   resp.Ready,
		DatabasePath:            resp.DatabasePath,
		RootSummary:             resp.RootSummary,
		SourceConnectionID:      resp.SourceConnectionID,
		DestinationConnectionID: resp.DestinationConnectionID,
		SourceRootPrepared:      resp.SourceRootPrepared,
		DestinationRootPrepared: resp.DestinationRootPrepared,
	}, nil
}

// retireReplacedConnection drops oauth + in-memory session for a connection id that
// is no longer the FS binding after reauth minted a replacement id.
func (m *Manager) retireReplacedConnection(migrationID, oldConnectionID string) {
	oldConnectionID = strings.TrimSpace(oldConnectionID)
	if oldConnectionID == "" {
		return
	}
	if mig, err := m.GetMigration(context.Background(), migrationID); err == nil && mig != nil {
		_ = mig.RunStore(func(s *migration.Store) error {
			return s.DeleteOAuthCredentials(oldConnectionID)
		})
	}
	m.connMgr.Delete(oldConnectionID)
	if m.serviceMgr != nil && m.serviceMgr.FS != nil {
		_ = m.serviceMgr.FS.RevokeCloudConnection(oldConnectionID, "")
	}
}

// validateQuietReauthRoot requires the same service and native root id as the stored binding.
func (m *Manager) validateQuietReauthRoot(migrationID, role, serviceID, rootID string) error {
	mig, err := m.GetMigration(context.Background(), migrationID)
	if err != nil || mig == nil {
		return err
	}
	role = strings.ToLower(strings.TrimSpace(role))
	binding, err := mig.GetFSCredentialBinding(role)
	if err != nil {
		// No binding yet (incomplete import metadata): allow quiet bind.
		return nil
	}
	if binding == nil {
		return nil
	}
	if binding.ServiceID != "" && serviceID != "" && binding.ServiceID != serviceID {
		return fmt.Errorf("reauth must use the same %s provider (%s)", role, binding.ServiceID)
	}
	folder, ok := parseBindingFolder(binding.RootFolderJSON)
	if !ok || strings.TrimSpace(folder.ServiceID) == "" {
		return nil
	}
	if strings.TrimSpace(rootID) != "" && folder.ServiceID != rootID {
		return fmt.Errorf("reauth must confirm the same %s root", role)
	}
	return nil
}

func mapRootChildPlans(in []corebridge.RootChildPlan) []roots.RootChildPlan {
	if len(in) == 0 {
		return nil
	}
	out := make([]roots.RootChildPlan, len(in))
	for i, c := range in {
		out[i] = roots.RootChildPlan{
			ID:          c.ID,
			Name:        c.Name,
			Type:        c.Type,
			Size:        c.Size,
			MTime:       c.MTime,
			Excluded:    c.Excluded,
			DstOnly:     c.DstOnly,
			IncludeOnly: append([]string(nil), c.IncludeOnly...),
			Children:    mapRootChildPlans(c.Children),
		}
	}
	return out
}

func mapRootChildPlansToCore(in []roots.RootChildPlan) []corebridge.RootChildPlan {
	if len(in) == 0 {
		return nil
	}
	out := make([]corebridge.RootChildPlan, len(in))
	for i, c := range in {
		out[i] = corebridge.RootChildPlan{
			ID:          c.ID,
			Name:        c.Name,
			Type:        c.Type,
			Size:        c.Size,
			MTime:       c.MTime,
			Excluded:    c.Excluded,
			DstOnly:     c.DstOnly,
			IncludeOnly: append([]string(nil), c.IncludeOnly...),
			Children:    mapRootChildPlansToCore(c.Children),
		}
	}
	return out
}
