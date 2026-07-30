package manager

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"codeberg.org/Sylos/Migration-Engine/pkg/migration"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge"
	fstypes "codeberg.org/Sylos/Sylos-FS/pkg/types"
)

// rootInfoForRole builds a RootInfo from the persisted FS credential binding for a role,
// resolving the service name/type. Returns nil when no usable binding exists.
func (m *Manager) rootInfoForRole(mig *migration.Migration, role string) *corebridge.RootInfo {
	if mig == nil {
		return nil
	}
	binding, err := mig.GetFSCredentialBinding(role)
	if err != nil || binding == nil || binding.RootFolderJSON == "" {
		return nil
	}
	var folder fstypes.Folder
	if err := json.Unmarshal([]byte(binding.RootFolderJSON), &folder); err != nil {
		return nil
	}
	info := &corebridge.RootInfo{
		ServiceID:    binding.ServiceID,
		ConnectionID: binding.ConnectionID,
		Name:         folder.DisplayName,
		LocationPath: folder.LocationPath,
		NativePath:   folder.ServiceID,
		Type:         folder.Type,
	}
	if def, err := m.serviceMgr.GetServiceDefinition(binding.ServiceID); err == nil {
		info.ServiceName = def.Name
		info.ServiceType = string(def.Type)
	}
	return info
}

// migrationRoots returns the persisted source and destination roots (either may be nil).
func (m *Manager) migrationRoots(mig *migration.Migration) (source, destination *corebridge.RootInfo) {
	return m.rootInfoForRole(mig, migration.FSCredentialRoleSource),
		m.rootInfoForRole(mig, migration.FSCredentialRoleDestination)
}

// cachedMigrationRoots returns roots from the in-memory root plan. Root bindings
// are immutable during a live phase, so status polling must not re-read DuckDB.
func (m *Manager) cachedMigrationRoots(migrationID string) (source, destination *corebridge.RootInfo, ok bool) {
	if m.rootsMgr == nil {
		return nil, nil, false
	}
	plan := m.rootsMgr.GetPlan(migrationID)
	if plan == nil {
		return nil, nil, false
	}
	if plan.HasSource {
		source = &corebridge.RootInfo{
			ServiceID:     plan.SourceDefinition.ID,
			ConnectionID:  plan.SourceConnectionID,
			ServiceName:   plan.SourceDefinition.Name,
			ServiceType:   string(plan.SourceDefinition.Type),
			Name:          plan.SourceRoot.DisplayName,
			LocationPath:  plan.SourceRoot.LocationPath,
			NativePath:    plan.SourceRoot.ServiceID,
			Type:          plan.SourceRoot.Type,
		}
	}
	if plan.HasDestination {
		destination = &corebridge.RootInfo{
			ServiceID:     plan.DestinationDefinition.ID,
			ConnectionID:  plan.DestinationConnectionID,
			ServiceName:   plan.DestinationDefinition.Name,
			ServiceType:   string(plan.DestinationDefinition.Type),
			Name:          plan.DestinationRoot.DisplayName,
			LocationPath:  plan.DestinationRoot.LocationPath,
			NativePath:    plan.DestinationRoot.ServiceID,
			Type:          plan.DestinationRoot.Type,
		}
	}
	return source, destination, true
}

// rootLabel returns a human-friendly label for a root, prefixed with the service name when known.
func rootLabel(info *corebridge.RootInfo) string {
	if info == nil {
		return ""
	}
	part := rootPathPart(info)
	if part == "" {
		return ""
	}
	if service := strings.TrimSpace(info.ServiceName); service != "" {
		return service + " · " + part
	}
	return part
}

func rootPathPart(info *corebridge.RootInfo) string {
	if info == nil {
		return ""
	}
	loc := strings.TrimSpace(info.LocationPath)
	native := strings.TrimSpace(info.NativePath)
	name := strings.TrimSpace(info.Name)
	isAtServiceRoot := loc == "" || loc == "/"

	if isAtServiceRoot {
		if info.ServiceType == "cloud" {
			if name != "" {
				return name
			}
			if native != "" && !isCloudProviderFolderID(native) {
				return native
			}
			return name
		}
		if isFilesystemNativePath(native) {
			return native
		}
		if name != "" {
			return name
		}
		if native != "" && native != "/" {
			return native
		}
		return loc
	}

	if loc != "" && loc != "/" {
		return loc
	}
	return name
}

func isCloudProviderFolderID(id string) bool {
	switch strings.TrimSpace(id) {
	case "root", "sharedWithMe", "shared_with_me":
		return true
	default:
		return false
	}
}

func isFilesystemNativePath(p string) bool {
	p = strings.TrimSpace(p)
	if p == "" || p == "/" {
		return false
	}
	if isCloudProviderFolderID(p) {
		return false
	}
	if len(p) >= 2 && p[1] == ':' {
		return true
	}
	if strings.HasPrefix(p, "/") || strings.HasPrefix(p, `\\`) {
		return true
	}
	return false
}

func isPlaceholderAutoMigrationName(name string) bool {
	name = strings.TrimSpace(name)
	return name == "root"+migrationNameArrow+"root"
}

const migrationNameArrow = " \u2192 "

// isPartialAutoMigrationName reports auto-generated names written before both roots were set.
func isPartialAutoMigrationName(name string) bool {
	name = strings.TrimSpace(name)
	return strings.HasSuffix(name, migrationNameArrow+"?") || strings.HasPrefix(name, "?"+migrationNameArrow)
}

// isUnnamedMigration reports whether a migration still carries a placeholder name
// (empty, the engine default "migration", the migration ID, or a partial auto default)
// and should get a computed default.
func isUnnamedMigration(mig *migration.Migration) bool {
	if mig == nil {
		return true
	}
	name := strings.TrimSpace(mig.GetName())
	if name == "" || name == "migration" || name == mig.ID {
		return true
	}
	if isPlaceholderAutoMigrationName(name) {
		return true
	}
	return isPartialAutoMigrationName(name)
}

// defaultMigrationName builds "{source} -> {destination}" from the roots, tolerating a missing side.
func defaultMigrationName(source, destination *corebridge.RootInfo) string {
	src := rootLabel(source)
	dst := rootLabel(destination)
	switch {
	case src != "" && dst != "":
		return src + migrationNameArrow + dst
	case src != "":
		return src + migrationNameArrow + "?"
	case dst != "":
		return "?" + migrationNameArrow + dst
	default:
		return ""
	}
}

// refreshDefaultMigrationName recomputes and persists the "{source} -> {destination}" default
// name from the persisted roots, but only while the migration is still unnamed (never overrides a user rename).
func (m *Manager) refreshDefaultMigrationName(ctx context.Context, migrationID string) {
	mig, err := m.GetMigration(ctx, migrationID)
	if err != nil || mig == nil {
		return
	}
	name := strings.TrimSpace(mig.GetName())
	if !isUnnamedMigration(mig) && !isPlaceholderAutoMigrationName(name) {
		return
	}
	source, destination := m.migrationRoots(mig)
	computed := defaultMigrationName(source, destination)
	if computed == "" {
		return
	}
	if err := mig.SetName(computed); err != nil {
		m.logger.Warn().Err(err).Str("migration_id", migrationID).Msg("set default migration name")
		return
	}
	if rec, err := m.getMigrationRecord(migrationID); err == nil && rec.ID != "" {
		rec.Name = computed
		if err := m.upsertMigrationRecord(rec); err != nil {
			m.logger.Warn().Err(err).Str("migration_id", migrationID).Msg("update migration metadata name")
		}
	}
}

// RenameMigration sets an explicit user-provided display name for a migration.
func (m *Manager) RenameMigration(ctx context.Context, migrationID, name string) error {
	name = strings.TrimSpace(name)
	if name == "" {
		return fmt.Errorf("migration name cannot be empty")
	}
	mig, err := m.GetMigration(ctx, migrationID)
	if err != nil {
		return err
	}
	if mig == nil {
		return corebridge.ErrMigrationNotFound
	}
	if err := mig.SetName(name); err != nil {
		return err
	}
	if rec, err := m.getMigrationRecord(migrationID); err == nil && rec.ID != "" {
		rec.Name = name
		if err := m.upsertMigrationRecord(rec); err != nil {
			m.logger.Warn().Err(err).Str("migration_id", migrationID).Msg("update migration metadata name")
		}
	}
	return nil
}
