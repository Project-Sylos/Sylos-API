package apidb

import (
	"fmt"
	"strings"
	"time"
)

const (
	ScopeProvider = "provider"
	ScopeSFTPHost = "sftp_host"
)

// ScalingOverrideRow is one persisted MaxWorkers override cell.
type ScalingOverrideRow struct {
	Scope      string
	ScopeKey   string
	Mode       string
	MaxWorkers int
	UpdatedAt  time.Time
}

// ListAll returns every scaling override row.
func (d *DB) ListAll() ([]ScalingOverrideRow, error) {
	rows, err := d.sql.Query(`
		SELECT scope, scope_key, mode, max_workers, updated_at
		FROM scaling_overrides
		ORDER BY scope, scope_key, mode
	`)
	if err != nil {
		return nil, fmt.Errorf("list scaling overrides: %w", err)
	}
	defer rows.Close()
	return scanScalingOverrideRows(rows)
}

// ListByScope returns overrides for one scope (all keys).
func (d *DB) ListByScope(scope string) ([]ScalingOverrideRow, error) {
	scope = strings.TrimSpace(scope)
	rows, err := d.sql.Query(`
		SELECT scope, scope_key, mode, max_workers, updated_at
		FROM scaling_overrides
		WHERE scope = ?
		ORDER BY scope_key, mode
	`, scope)
	if err != nil {
		return nil, fmt.Errorf("list scaling overrides by scope: %w", err)
	}
	defer rows.Close()
	return scanScalingOverrideRows(rows)
}

// UpsertModes replaces all modes for (scope, key) with modes.
// An empty modes map clears the scope key.
func (d *DB) UpsertModes(scope, key string, modes map[string]int) error {
	scope = strings.TrimSpace(scope)
	key = strings.TrimSpace(key)
	if scope == "" {
		return fmt.Errorf("scope is required")
	}
	if key == "" {
		return fmt.Errorf("scope key is required")
	}
	if len(modes) == 0 {
		return d.DeleteScope(scope, key)
	}

	tx, err := d.sql.Begin()
	if err != nil {
		return fmt.Errorf("begin scaling override upsert: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	if _, err := tx.Exec(`DELETE FROM scaling_overrides WHERE scope = ? AND scope_key = ?`, scope, key); err != nil {
		return fmt.Errorf("clear scaling override scope: %w", err)
	}
	now := time.Now().UTC()
	for mode, maxWorkers := range modes {
		mode = strings.TrimSpace(mode)
		if mode == "" {
			return fmt.Errorf("mode is required")
		}
		if _, err := tx.Exec(`
			INSERT INTO scaling_overrides (scope, scope_key, mode, max_workers, updated_at)
			VALUES (?, ?, ?, ?, ?)
		`, scope, key, mode, maxWorkers, now); err != nil {
			return fmt.Errorf("insert scaling override %s/%s/%s: %w", scope, key, mode, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit scaling override upsert: %w", err)
	}
	return nil
}

// DeleteScope removes all modes for (scope, key).
func (d *DB) DeleteScope(scope, key string) error {
	scope = strings.TrimSpace(scope)
	key = strings.TrimSpace(key)
	if scope == "" {
		return fmt.Errorf("scope is required")
	}
	if key == "" {
		return fmt.Errorf("scope key is required")
	}
	if _, err := d.sql.Exec(`DELETE FROM scaling_overrides WHERE scope = ? AND scope_key = ?`, scope, key); err != nil {
		return fmt.Errorf("delete scaling override scope: %w", err)
	}
	return nil
}

// DeleteMode removes one mode for (scope, key).
func (d *DB) DeleteMode(scope, key, mode string) error {
	scope = strings.TrimSpace(scope)
	key = strings.TrimSpace(key)
	mode = strings.TrimSpace(mode)
	if scope == "" {
		return fmt.Errorf("scope is required")
	}
	if key == "" {
		return fmt.Errorf("scope key is required")
	}
	if mode == "" {
		return fmt.Errorf("mode is required")
	}
	if _, err := d.sql.Exec(
		`DELETE FROM scaling_overrides WHERE scope = ? AND scope_key = ? AND mode = ?`,
		scope, key, mode,
	); err != nil {
		return fmt.Errorf("delete scaling override mode: %w", err)
	}
	return nil
}

type scalingOverrideScanner interface {
	Next() bool
	Scan(dest ...any) error
	Err() error
}

func scanScalingOverrideRows(rows scalingOverrideScanner) ([]ScalingOverrideRow, error) {
	var out []ScalingOverrideRow
	for rows.Next() {
		var row ScalingOverrideRow
		if err := rows.Scan(&row.Scope, &row.ScopeKey, &row.Mode, &row.MaxWorkers, &row.UpdatedAt); err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	return out, rows.Err()
}
