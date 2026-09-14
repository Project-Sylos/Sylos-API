package manager

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"codeberg.org/Sylos/Migration-Engine/pkg/filter"
	"codeberg.org/Sylos/Migration-Engine/pkg/migration"
	"codeberg.org/Sylos/Sylos-API/internal/auth"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge/apidb"
)

const migrationRulesetFile = "ruleset.json"

func rulesetSummary(rec apidb.RulesetRecord, includeRoot bool) corebridge.RulesetSummary {
	out := corebridge.RulesetSummary{
		ID:            rec.ID,
		Name:          rec.Name,
		Description:   rec.Description,
		CreatedBy:     rec.CreatedBy,
		SchemaVersion: rec.SchemaVersion,
		CreatedAt:     rec.CreatedAt.UTC().Format(time.RFC3339),
		UpdatedAt:     rec.UpdatedAt.UTC().Format(time.RFC3339),
	}
	if includeRoot {
		out.RootGroup = rec.RootGroup
	}
	return out
}

func (m *Manager) ListRulesets(ctx context.Context, createdBy string) ([]corebridge.RulesetSummary, error) {
	if m.apiDB == nil {
		return nil, fmt.Errorf("API database not configured")
	}
	recs, err := m.apiDB.ListRulesets(strings.TrimSpace(createdBy))
	if err != nil {
		return nil, err
	}
	out := make([]corebridge.RulesetSummary, 0, len(recs))
	for _, rec := range recs {
		out = append(out, rulesetSummary(rec, true))
	}
	return out, nil
}

func (m *Manager) GetRuleset(ctx context.Context, id string) (corebridge.RulesetSummary, error) {
	if m.apiDB == nil {
		return corebridge.RulesetSummary{}, fmt.Errorf("API database not configured")
	}
	rec, err := m.apiDB.GetRuleset(id)
	if err != nil {
		return corebridge.RulesetSummary{}, err
	}
	return rulesetSummary(rec, true), nil
}

func (m *Manager) CreateRuleset(ctx context.Context, req corebridge.CreateRulesetRequest) (corebridge.RulesetSummary, error) {
	if m.apiDB == nil {
		return corebridge.RulesetSummary{}, fmt.Errorf("API database not configured")
	}
	createdBy := actorFromContext(ctx)
	rec := apidb.RulesetRecord{
		Name:          strings.TrimSpace(req.Name),
		Description:   strings.TrimSpace(req.Description),
		CreatedBy:     createdBy,
		SchemaVersion: req.SchemaVersion,
		RootGroup:     req.RootGroup,
	}
	if _, err := filter.Compile(rec.ToFilterRuleset()); err != nil {
		return corebridge.RulesetSummary{}, fmt.Errorf("invalid ruleset: %w", err)
	}
	created, err := m.apiDB.CreateRuleset(rec)
	if err != nil {
		return corebridge.RulesetSummary{}, err
	}
	return rulesetSummary(created, true), nil
}

func (m *Manager) UpdateRuleset(ctx context.Context, id string, req corebridge.UpdateRulesetRequest) (corebridge.RulesetSummary, error) {
	if m.apiDB == nil {
		return corebridge.RulesetSummary{}, fmt.Errorf("API database not configured")
	}
	rec := apidb.RulesetRecord{
		ID:            id,
		Name:          strings.TrimSpace(req.Name),
		Description:   strings.TrimSpace(req.Description),
		SchemaVersion: req.SchemaVersion,
		RootGroup:     req.RootGroup,
	}
	if _, err := filter.Compile(rec.ToFilterRuleset()); err != nil {
		return corebridge.RulesetSummary{}, fmt.Errorf("invalid ruleset: %w", err)
	}
	if err := m.apiDB.UpdateRuleset(rec); err != nil {
		return corebridge.RulesetSummary{}, err
	}
	updated, err := m.apiDB.GetRuleset(id)
	if err != nil {
		return corebridge.RulesetSummary{}, err
	}
	return rulesetSummary(updated, true), nil
}

func (m *Manager) DeleteRuleset(ctx context.Context, id string) error {
	if m.apiDB == nil {
		return fmt.Errorf("API database not configured")
	}
	return m.apiDB.DeleteRuleset(id)
}

func (m *Manager) ImportRuleset(ctx context.Context, rs filter.Ruleset) (corebridge.RulesetSummary, error) {
	if _, err := filter.Compile(rs); err != nil {
		return corebridge.RulesetSummary{}, fmt.Errorf("invalid ruleset: %w", err)
	}
	createdBy := actorFromContext(ctx)
	if strings.TrimSpace(rs.CreatedBy) == apidb.CreatedByPrepackaged {
		rs.CreatedBy = createdBy
	}
	rec := apidb.RulesetRecord{
		ID:            strings.TrimSpace(rs.RulesetID),
		Name:          strings.TrimSpace(rs.Name),
		Description:   strings.TrimSpace(rs.Description),
		CreatedBy:     createdBy,
		SchemaVersion: rs.SchemaVersion,
		RootGroup:     rs.RootGroup,
	}
	if rec.SchemaVersion == 0 {
		rec.SchemaVersion = filter.SchemaVersion
	}
	created, err := m.apiDB.CreateRuleset(rec)
	if err != nil {
		return corebridge.RulesetSummary{}, err
	}
	return rulesetSummary(created, true), nil
}

func (m *Manager) ExportRuleset(ctx context.Context, id string) (filter.Ruleset, error) {
	if m.apiDB == nil {
		return filter.Ruleset{}, fmt.Errorf("API database not configured")
	}
	rec, err := m.apiDB.GetRuleset(id)
	if err != nil {
		return filter.Ruleset{}, err
	}
	return rec.ToFilterRuleset(), nil
}

func actorFromContext(ctx context.Context) string {
	if claims, ok := auth.ClaimsFromContext(ctx); ok && strings.TrimSpace(claims.Subject) != "" {
		return claims.Subject
	}
	return "user"
}

func (m *Manager) migrationRulesetPath(migrationID string) (string, error) {
	dir, err := m.migrationDirFor(migrationID)
	if err != nil {
		return "", err
	}
	return filepath.Join(dir, migrationRulesetFile), nil
}

func (m *Manager) loadMigrationRulesetFile(migrationID string) (*filter.Ruleset, error) {
	path, err := m.migrationRulesetPath(migrationID)
	if err != nil {
		return nil, err
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	var rs filter.Ruleset
	if err := json.Unmarshal(raw, &rs); err != nil {
		return nil, fmt.Errorf("decode ruleset.json: %w", err)
	}
	return &rs, nil
}

func (m *Manager) saveMigrationRulesetFile(migrationID string, rs filter.Ruleset) error {
	path, err := m.migrationRulesetPath(migrationID)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	raw, err := json.MarshalIndent(rs, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, raw, 0o644)
}

func (m *Manager) loadCompiledMigrationRuleset(migrationID string) (*filter.CompiledRuleset, error) {
	rs, err := m.loadMigrationRulesetFile(migrationID)
	if err != nil || rs == nil {
		return nil, err
	}
	return filter.Compile(*rs)
}

func (m *Manager) applyMigrationFilterRuleset(migrationID string, mig *migration.Migration, cfg *migration.Config) error {
	compiled, err := m.loadCompiledMigrationRuleset(migrationID)
	if err != nil {
		return err
	}
	if compiled == nil {
		return nil
	}
	if mig != nil {
		if err := mig.SetFilterRuleset(compiled); err != nil {
			return err
		}
	}
	if cfg != nil {
		cfg.FilterRuleset = compiled
	}
	return nil
}

func (m *Manager) GetMigrationRuleset(ctx context.Context, migrationID string) (corebridge.MigrationRulesetResponse, error) {
	rs, err := m.loadMigrationRulesetFile(migrationID)
	if err != nil {
		return corebridge.MigrationRulesetResponse{}, err
	}
	if rs == nil {
		return corebridge.MigrationRulesetResponse{}, nil
	}
	return corebridge.MigrationRulesetResponse{Ruleset: rs}, nil
}

func (m *Manager) SetMigrationRuleset(ctx context.Context, migrationID string, req corebridge.SetMigrationRulesetRequest) error {
	rs := req.Ruleset
	if strings.TrimSpace(rs.RulesetID) == "" {
		rs.RulesetID = migrationID + "-ruleset"
	}
	if rs.SchemaVersion == 0 {
		rs.SchemaVersion = filter.SchemaVersion
	}
	compiled, err := filter.Compile(rs)
	if err != nil {
		return fmt.Errorf("invalid ruleset: %w", err)
	}
	if err := m.saveMigrationRulesetFile(migrationID, rs); err != nil {
		return err
	}
	mig, err := m.GetMigration(ctx, migrationID)
	if err != nil {
		return err
	}
	return mig.SetFilterRuleset(compiled)
}
