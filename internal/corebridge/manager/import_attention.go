package manager

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"codeberg.org/Sylos/Migration-Engine/pkg/db/oauth"
	"codeberg.org/Sylos/Migration-Engine/pkg/migration"
	"codeberg.org/Sylos/Migration-Engine/pkg/opsdb"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge/apidb"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge/services"
	fstypes "codeberg.org/Sylos/Sylos-FS/pkg/types"
)

// ErrMigrationNeedsAttention is returned when a live FS hot path is blocked after ops import
// (start/resume discovery, copy, delete, retry sweep, etc.). DB-only review stays allowed.
var ErrMigrationNeedsAttention = fmt.Errorf("migration needs attention: re-authenticate providers before running filesystem operations")

func (m *Manager) migrationNeedsAttention(migrationID string) bool {
	if m.apiDB == nil || migrationID == "" {
		return false
	}
	rec, err := m.apiDB.GetMigration(migrationID)
	if err != nil {
		return false
	}
	return rec.NeedsAttention
}

func (m *Manager) rejectIfNeedsAttention(migrationID string) error {
	if m.migrationNeedsAttention(migrationID) {
		return ErrMigrationNeedsAttention
	}
	return nil
}

// scanImportedOps strips oauth rows, probes local roots, and persists attention flags.
func (m *Manager) scanImportedOps(migrationID string) (corebridge.ImportScanReport, error) {
	report := corebridge.ImportScanReport{}
	mig, err := m.GetMigration(context.Background(), migrationID)
	if err != nil {
		return report, err
	}
	if mig == nil || mig.DB == nil || mig.DB.Ops() == nil {
		return report, fmt.Errorf("migration ops store unavailable")
	}
	ops := mig.DB.Ops()
	tokenKey, err := m.apiDB.EnsureMigrationKey(migrationID)
	if err != nil {
		return report, err
	}

	reasons := map[string]struct{}{}
	oauthIDs, err := ops.ListOAuthConnectionIDs()
	if err != nil {
		return report, err
	}
	for _, id := range oauthIDs {
		payload, ok, err := ops.GetOAuthCred(id)
		if err != nil {
			return report, err
		}
		if !ok {
			continue
		}
		if _, err := oauth.OpenOAuthCredentials(payload, tokenKey); err != nil {
			reasons[apidb.AttentionCredentialsUndecryptable] = struct{}{}
		}
	}
	if _, err := ops.DeleteAllOAuthCreds(); err != nil {
		return report, fmt.Errorf("strip oauth on import: %w", err)
	}

	bindings, err := mig.ListFSCredentialBindings()
	if err != nil {
		return report, err
	}
	var localRoots []corebridge.LocalRootProbe
	var sourceSnap, destSnap *apidb.MigrationRootSnapshot
	for _, b := range bindings {
		def, defErr := m.serviceMgr.GetServiceDefinition(b.ServiceID)
		needsCreds := defErr == nil && def.Type == services.ServiceTypeCloud
		if needsCreds {
			reasons[apidb.AttentionCredentialsMissing] = struct{}{}
		}
		folder, folderOK := parseBindingFolder(b.RootFolderJSON)
		native := ""
		if folderOK {
			native = folder.ServiceID
		}
		if defErr == nil && def.Type == services.ServiceTypeLocal {
			probe := corebridge.LocalRootProbe{
				Role:       b.Role,
				NativePath: native,
				ServiceID:  b.ServiceID,
			}
			if native != "" {
				if _, statErr := os.Stat(native); statErr == nil {
					probe.PathExistsOnHost = true
				}
			}
			localRoots = append(localRoots, probe)
		}
		snap := bindingToRootSnapshot(b, defErr == nil, def, folder, folderOK)
		switch b.Role {
		case migration.FSCredentialRoleSource:
			sourceSnap = snap
		case migration.FSCredentialRoleDestination:
			destSnap = snap
		}
	}

	reasonList := make([]string, 0, len(reasons))
	for r := range reasons {
		reasonList = append(reasonList, r)
	}
	needsAttention := len(reasonList) > 0
	report = corebridge.ImportScanReport{
		NeedsAttention:   needsAttention,
		AttentionReasons: reasonList,
		LocalRoots:       localRoots,
	}

	rec, err := m.getMigrationRecord(migrationID)
	if err != nil || rec.ID == "" {
		rec = apidb.MigrationRecord{ID: migrationID, Name: migrationID}
	}
	rec.NeedsAttention = needsAttention
	rec.AttentionReasons = reasonList
	rec.LocalRoots = make([]apidb.LocalRootProbe, len(localRoots))
	for i, p := range localRoots {
		rec.LocalRoots[i] = apidb.LocalRootProbe{
			Role:             p.Role,
			NativePath:       p.NativePath,
			PathExistsOnHost: p.PathExistsOnHost,
			ServiceID:        p.ServiceID,
		}
	}
	if sourceSnap != nil {
		rec.SourceRoot = sourceSnap
	}
	if destSnap != nil {
		rec.DestinationRoot = destSnap
	}
	if err := m.upsertMigrationRecord(rec); err != nil {
		return report, err
	}
	return report, nil
}

func parseBindingFolder(raw string) (fstypes.Folder, bool) {
	var folder fstypes.Folder
	if strings.TrimSpace(raw) == "" {
		return folder, false
	}
	if err := json.Unmarshal([]byte(raw), &folder); err != nil {
		return folder, false
	}
	return folder, true
}

func bindingToRootSnapshot(
	b migration.FSCredentialBinding,
	hasDef bool,
	def services.ServiceDefinition,
	folder fstypes.Folder,
	folderOK bool,
) *apidb.MigrationRootSnapshot {
	snap := &apidb.MigrationRootSnapshot{
		ServiceID:    b.ServiceID,
		ConnectionID: b.ConnectionID,
	}
	if hasDef {
		snap.ServiceName = def.Name
		snap.ServiceType = string(def.Type)
	}
	if folderOK {
		snap.Name = folder.DisplayName
		snap.LocationPath = folder.LocationPath
		snap.NativePath = folder.ServiceID
		snap.Type = string(folder.Type)
		snap.ParentID = folder.ParentId
	}
	return snap
}

func applyAttentionToStatus(st *corebridge.Status, rec apidb.MigrationRecord) {
	if st == nil {
		return
	}
	st.NeedsAttention = rec.NeedsAttention
	st.AttentionReasons = append([]string(nil), rec.AttentionReasons...)
	if len(rec.LocalRoots) == 0 {
		st.LocalRoots = nil
		return
	}
	st.LocalRoots = make([]corebridge.LocalRootProbe, len(rec.LocalRoots))
	for i, p := range rec.LocalRoots {
		st.LocalRoots[i] = corebridge.LocalRootProbe{
			Role:             p.Role,
			NativePath:       p.NativePath,
			PathExistsOnHost: p.PathExistsOnHost,
			ServiceID:        p.ServiceID,
		}
	}
}

// reevaluateImportAttention clears needsAttention when every cloud binding has usable oauth.
func (m *Manager) reevaluateImportAttention(migrationID string) {
	if m.apiDB == nil || migrationID == "" {
		return
	}
	rec, err := m.apiDB.GetMigration(migrationID)
	if err != nil || !rec.NeedsAttention {
		return
	}
	mig, err := m.GetMigration(context.Background(), migrationID)
	if err != nil || mig == nil {
		return
	}
	bindings, err := mig.ListFSCredentialBindings()
	if err != nil {
		return
	}
	stillMissing := false
	for _, b := range bindings {
		def, defErr := m.serviceMgr.GetServiceDefinition(b.ServiceID)
		if defErr != nil || def.Type != services.ServiceTypeCloud {
			continue
		}
		if b.ConnectionID == "" {
			stillMissing = true
			break
		}
		if _, err := mig.GetOAuthCredentials(b.ConnectionID); err != nil {
			stillMissing = true
			break
		}
	}
	if stillMissing {
		return
	}
	rec.NeedsAttention = false
	rec.AttentionReasons = nil
	_ = m.apiDB.UpsertMigration(rec)
}

// RemapLocalRootRequest is the body for import-only local ServiceID remap.
type RemapLocalRootRequest struct {
	Role string                      `json:"role"`
	Root corebridge.FolderDescriptor `json:"root"`
}

// RemapLocalRootResponse reports how many catalog rows were rewritten.
type RemapLocalRootResponse struct {
	Rewritten int    `json:"rewritten"`
	OldRoot   string `json:"oldRoot"`
	NewRoot   string `json:"newRoot"`
}

// RemapLocalRoot rewrites LocalFS ServiceIDs for one role (import wizard only).
func (m *Manager) RemapLocalRoot(ctx context.Context, migrationID string, req RemapLocalRootRequest) (RemapLocalRootResponse, error) {
	_ = ctx
	role := strings.ToLower(strings.TrimSpace(req.Role))
	if role != migration.FSCredentialRoleSource && role != migration.FSCredentialRoleDestination {
		return RemapLocalRootResponse{}, fmt.Errorf("role must be source or destination")
	}
	newRoot := opsdb.NormalizeLocalRootPath(req.Root.ID)
	if newRoot == "" {
		return RemapLocalRootResponse{}, fmt.Errorf("new root id is required")
	}
	if _, err := os.Stat(newRoot); err != nil {
		return RemapLocalRootResponse{}, fmt.Errorf("new root path not accessible: %w", err)
	}

	mig, err := m.GetMigration(context.Background(), migrationID)
	if err != nil {
		return RemapLocalRootResponse{}, err
	}
	binding, err := mig.GetFSCredentialBinding(role)
	if err != nil {
		return RemapLocalRootResponse{}, err
	}
	def, err := m.serviceMgr.GetServiceDefinition(binding.ServiceID)
	if err != nil {
		return RemapLocalRootResponse{}, err
	}
	if def.Type != services.ServiceTypeLocal {
		return RemapLocalRootResponse{}, fmt.Errorf("remap-local only applies to local filesystem roots")
	}
	oldFolder, ok := parseBindingFolder(binding.RootFolderJSON)
	if !ok || strings.TrimSpace(oldFolder.ServiceID) == "" {
		return RemapLocalRootResponse{}, fmt.Errorf("existing local root is missing")
	}
	oldRoot := opsdb.NormalizeLocalRootPath(oldFolder.ServiceID)
	side := opsdb.SideSRC
	if role == migration.FSCredentialRoleDestination {
		side = opsdb.SideDST
	}
	rewritten, err := mig.RemapLocalServiceIDs(side, oldRoot, newRoot)
	if err != nil {
		return RemapLocalRootResponse{}, err
	}

	newFolder := fstypes.Folder{
		ServiceID:    newRoot,
		ParentId:     req.Root.ParentID,
		ParentPath:   req.Root.ParentPath,
		DisplayName:  req.Root.DisplayName,
		LocationPath: req.Root.LocationPath,
		LastUpdated:  req.Root.LastUpdated,
		DepthLevel:   req.Root.DepthLevel,
		Type:         req.Root.Type,
	}
	if newFolder.LocationPath == "" {
		newFolder.LocationPath = "/"
	}
	if newFolder.Type == "" {
		newFolder.Type = "folder"
	}
	raw, err := json.Marshal(newFolder)
	if err != nil {
		return RemapLocalRootResponse{}, err
	}
	binding.RootFolderJSON = string(raw)
	if err := mig.RunStore(func(s *migration.Store) error {
		return s.UpsertFSCredentialBinding(*binding)
	}); err != nil {
		return RemapLocalRootResponse{}, err
	}

	info := &corebridge.RootInfo{
		ServiceID:    binding.ServiceID,
		ServiceName:  def.Name,
		ServiceType:  string(def.Type),
		ConnectionID: binding.ConnectionID,
		Name:         newFolder.DisplayName,
		LocationPath: newFolder.LocationPath,
		NativePath:   newFolder.ServiceID,
		Type:         newFolder.Type,
		ParentID:     newFolder.ParentId,
	}
	if role == migration.FSCredentialRoleSource {
		m.persistMigrationListHints(migrationID, "", info, nil, nil)
	} else {
		m.persistMigrationListHints(migrationID, "", nil, info, nil)
	}
	if rec, err := m.apiDB.GetMigration(migrationID); err == nil {
		updated := false
		for i := range rec.LocalRoots {
			if rec.LocalRoots[i].Role == role {
				rec.LocalRoots[i].NativePath = newFolder.ServiceID
				rec.LocalRoots[i].PathExistsOnHost = true
				updated = true
			}
		}
		if updated {
			_ = m.apiDB.UpsertMigration(rec)
		}
	}

	return RemapLocalRootResponse{
		Rewritten: rewritten,
		OldRoot:   oldRoot,
		NewRoot:   newFolder.ServiceID,
	}, nil
}
