package apidb

import (
	"fmt"
	"strings"
	"time"

	badger "github.com/dgraph-io/badger/v4"
)

const (
	ScopeProvider = "provider"
	ScopeSFTPHost = "sftp_host"
)

// ScalingOverrideRow is one persisted MaxWorkers override cell.
type ScalingOverrideRow struct {
	Scope      string    `json:"scope"`
	ScopeKey   string    `json:"scopeKey"`
	Mode       string    `json:"mode"`
	MaxWorkers int       `json:"maxWorkers"`
	UpdatedAt  time.Time `json:"updatedAt"`
}

// ListAll returns every scaling override row.
func (d *DB) ListAll() ([]ScalingOverrideRow, error) {
	var out []ScalingOverrideRow
	err := d.view(func(txn *badger.Txn) error {
		recs, err := listPrefixJSON[ScalingOverrideRow](txn, prefixScale, nil)
		if err != nil {
			return err
		}
		out = recs
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("list scaling overrides: %w", err)
	}
	sortScalingOverrides(out)
	return out, nil
}

// ListByScope returns overrides for one scope (all keys).
func (d *DB) ListByScope(scope string) ([]ScalingOverrideRow, error) {
	scope = strings.TrimSpace(scope)
	all, err := d.ListAll()
	if err != nil {
		return nil, err
	}
	var out []ScalingOverrideRow
	for _, row := range all {
		if row.Scope == scope {
			out = append(out, row)
		}
	}
	sortScalingOverrides(out)
	return out, nil
}

// UpsertModes replaces all modes for (scope, key) with modes.
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
	now := time.Now().UTC()
	return d.update(func(txn *badger.Txn) error {
		prefix := keyScalePrefix(scope, key)
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			if err := txn.Delete(it.Item().KeyCopy(nil)); err != nil {
				return fmt.Errorf("clear scaling override scope: %w", err)
			}
		}
		for mode, maxWorkers := range modes {
			mode = strings.TrimSpace(mode)
			if mode == "" {
				return fmt.Errorf("mode is required")
			}
			row := ScalingOverrideRow{
				Scope:      scope,
				ScopeKey:   key,
				Mode:       mode,
				MaxWorkers: maxWorkers,
				UpdatedAt:  now,
			}
			if err := putJSON(txn, keyScale(scope, key, mode), row); err != nil {
				return fmt.Errorf("insert scaling override %s/%s/%s: %w", scope, key, mode, err)
			}
		}
		return nil
	})
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
	return d.update(func(txn *badger.Txn) error {
		return deletePrefix(txn, string(keyScalePrefix(scope, key)))
	})
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
	return d.update(func(txn *badger.Txn) error {
		return deleteKey(txn, keyScale(scope, key, mode))
	})
}

func sortScalingOverrides(rows []ScalingOverrideRow) {
	for i := 0; i < len(rows); i++ {
		for j := i + 1; j < len(rows); j++ {
			a := rows[i].Scope + rows[i].ScopeKey + rows[i].Mode
			b := rows[j].Scope + rows[j].ScopeKey + rows[j].Mode
			if b < a {
				rows[i], rows[j] = rows[j], rows[i]
			}
		}
	}
}
