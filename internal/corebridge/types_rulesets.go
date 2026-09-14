package corebridge

import "codeberg.org/Sylos/Migration-Engine/pkg/filter"

// RulesetSummary is list/detail metadata for a stored ruleset.
type RulesetSummary struct {
	ID            string        `json:"id"`
	Name          string        `json:"name"`
	Description   string        `json:"description,omitempty"`
	CreatedBy     string        `json:"createdBy"`
	SchemaVersion int           `json:"schemaVersion"`
	RootGroup     filter.Group  `json:"rootGroup,omitempty"`
	CreatedAt     string        `json:"createdAt"`
	UpdatedAt     string        `json:"updatedAt"`
}

// CreateRulesetRequest creates a user ruleset.
type CreateRulesetRequest struct {
	Name          string       `json:"name"`
	Description   string       `json:"description,omitempty"`
	SchemaVersion int          `json:"schemaVersion,omitempty"`
	RootGroup     filter.Group `json:"rootGroup"`
}

// UpdateRulesetRequest updates a user ruleset.
type UpdateRulesetRequest struct {
	Name          string       `json:"name"`
	Description   string       `json:"description,omitempty"`
	SchemaVersion int          `json:"schemaVersion,omitempty"`
	RootGroup     filter.Group `json:"rootGroup"`
}

// MigrationRulesetResponse is the bound ruleset snapshot for a migration.
type MigrationRulesetResponse struct {
	Ruleset *filter.Ruleset `json:"ruleset,omitempty"`
}

// SetMigrationRulesetRequest binds a ruleset snapshot to a migration.
type SetMigrationRulesetRequest struct {
	Ruleset filter.Ruleset `json:"ruleset"`
}
