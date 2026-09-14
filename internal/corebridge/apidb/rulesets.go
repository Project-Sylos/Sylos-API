package apidb

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"codeberg.org/Sylos/Migration-Engine/pkg/filter"
	badger "github.com/dgraph-io/badger/v4"
)

// RulesetRecord is one saved filter ruleset.
type RulesetRecord struct {
	ID            string        `json:"id"`
	Name          string        `json:"name"`
	Description   string        `json:"description"`
	CreatedBy     string        `json:"createdBy"`
	SchemaVersion int           `json:"schemaVersion"`
	RootGroup     filter.Group  `json:"rootGroup"`
	CreatedAt     time.Time     `json:"createdAt"`
	UpdatedAt     time.Time     `json:"updatedAt"`
}

// SeedBuiltinRulesets inserts the prepackaged library and the seeded defaults.
func (d *DB) SeedBuiltinRulesets() error {
	for _, id := range retiredPrepackagedIDs {
		if err := d.deleteRulesetIfPrepackaged(id); err != nil {
			return fmt.Errorf("retire prepackaged ruleset %s: %w", id, err)
		}
	}
	defs := append(PrepackagedRulesetDefinitions(), DefaultRulesetDefinitions()...)
	for _, def := range defs {
		rec, err := d.GetRuleset(def.RulesetID)
		if errors.Is(err, ErrNotFound) {
			if err := d.insertRulesetRecord(rulesetRecordFromFilter(def)); err != nil {
				return err
			}
			continue
		}
		if err != nil {
			return fmt.Errorf("check builtin ruleset %s: %w", def.RulesetID, err)
		}
		now := time.Now().UTC()
		if def.CreatedBy == CreatedByPrepackaged {
			if err := d.refreshBuiltinRuleset(def, rec.CreatedAt, now); err != nil {
				return err
			}
			continue
		}
		if !rec.UpdatedAt.Equal(rec.CreatedAt) {
			continue
		}
		if err := d.refreshBuiltinRuleset(def, now, now); err != nil {
			return err
		}
	}
	return nil
}

func (d *DB) deleteRulesetIfPrepackaged(id string) error {
	rec, err := d.GetRuleset(id)
	if errors.Is(err, ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	if rec.CreatedBy != CreatedByPrepackaged {
		return nil
	}
	return d.update(func(txn *badger.Txn) error {
		return deleteKey(txn, keyRuleset(id))
	})
}

func (d *DB) refreshBuiltinRuleset(def filter.Ruleset, createdAt, updatedAt time.Time) error {
	rec := rulesetRecordFromFilter(def)
	rec.CreatedAt = createdAt
	rec.UpdatedAt = updatedAt
	return d.update(func(txn *badger.Txn) error {
		return putJSON(txn, keyRuleset(rec.ID), rec)
	})
}

// DefaultRulesetForUser resolves the user's stored starter ruleset preference.
func (d *DB) DefaultRulesetForUser(userID string) (*filter.Ruleset, error) {
	id := DefaultAutoApplyRulesetID
	rec, err := d.GetUser(strings.TrimSpace(userID))
	if err != nil && !errors.Is(err, ErrNotFound) {
		return nil, fmt.Errorf("read preferences for %s: %w", userID, err)
	}
	if err == nil && rec.Preferences != "" {
		var prefs struct {
			DefaultRulesetID *string `json:"defaultRulesetId"`
		}
		if json.Unmarshal([]byte(rec.Preferences), &prefs) == nil && prefs.DefaultRulesetID != nil {
			id = strings.TrimSpace(*prefs.DefaultRulesetID)
		}
	}
	if id == "" {
		return nil, nil
	}
	rsRec, err := d.GetRuleset(id)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return nil, nil
		}
		return nil, err
	}
	rs := rsRec.ToFilterRuleset()
	return &rs, nil
}

func rulesetRecordFromFilter(rs filter.Ruleset) RulesetRecord {
	now := time.Now().UTC()
	id := strings.TrimSpace(rs.RulesetID)
	if id == "" {
		id = newRulesetID()
	}
	createdBy := strings.TrimSpace(rs.CreatedBy)
	if createdBy == "" {
		createdBy = "user"
	}
	schemaVersion := rs.SchemaVersion
	if schemaVersion == 0 {
		schemaVersion = filter.SchemaVersion
	}
	return RulesetRecord{
		ID:            id,
		Name:          rs.Name,
		Description:   rs.Description,
		CreatedBy:     createdBy,
		SchemaVersion: schemaVersion,
		RootGroup:     rs.RootGroup,
		CreatedAt:     now,
		UpdatedAt:     now,
	}
}

func (d *DB) insertRulesetRecord(rec RulesetRecord) error {
	if rec.CreatedAt.IsZero() {
		rec.CreatedAt = time.Now().UTC()
	}
	if rec.UpdatedAt.IsZero() {
		rec.UpdatedAt = rec.CreatedAt
	}
	return d.PutRuleset(rec)
}

// PutRuleset writes a ruleset record, replacing any existing row with the same id.
func (d *DB) PutRuleset(rec RulesetRecord) error {
	if rec.CreatedAt.IsZero() {
		rec.CreatedAt = time.Now().UTC()
	}
	if rec.UpdatedAt.IsZero() {
		rec.UpdatedAt = rec.CreatedAt
	}
	return d.update(func(txn *badger.Txn) error {
		return putJSON(txn, keyRuleset(rec.ID), rec)
	})
}

func (d *DB) CreateRuleset(rec RulesetRecord) (RulesetRecord, error) {
	if strings.TrimSpace(rec.ID) == "" {
		rec.ID = newRulesetID()
	}
	if rec.SchemaVersion == 0 {
		rec.SchemaVersion = filter.SchemaVersion
	}
	now := time.Now().UTC()
	rec.CreatedAt = now
	rec.UpdatedAt = now
	if err := d.insertRulesetRecord(rec); err != nil {
		return RulesetRecord{}, err
	}
	return rec, nil
}

func (d *DB) UpdateRuleset(rec RulesetRecord) error {
	rec.UpdatedAt = time.Now().UTC()
	return d.update(func(txn *badger.Txn) error {
		var existing RulesetRecord
		if err := getJSON(txn, keyRuleset(rec.ID), &existing); err != nil {
			return err
		}
		if existing.CreatedBy == CreatedByPrepackaged {
			return fmt.Errorf("cannot modify prepackaged ruleset")
		}
		return putJSON(txn, keyRuleset(rec.ID), rec)
	})
}

func (d *DB) DeleteRuleset(id string) error {
	return d.update(func(txn *badger.Txn) error {
		var existing RulesetRecord
		if err := getJSON(txn, keyRuleset(id), &existing); err != nil {
			return err
		}
		if existing.CreatedBy == CreatedByPrepackaged {
			return fmt.Errorf("cannot delete prepackaged ruleset")
		}
		return deleteKey(txn, keyRuleset(id))
	})
}

func (d *DB) GetRuleset(id string) (RulesetRecord, error) {
	var rec RulesetRecord
	err := d.view(func(txn *badger.Txn) error {
		return getJSON(txn, keyRuleset(id), &rec)
	})
	return rec, err
}

func (d *DB) ListRulesets(createdBy string) ([]RulesetRecord, error) {
	var out []RulesetRecord
	err := d.view(func(txn *badger.Txn) error {
		recs, err := listPrefixJSON[RulesetRecord](txn, prefixRuleset, nil)
		if err != nil {
			return err
		}
		if createdBy != "" {
			filtered := recs[:0]
			for _, rec := range recs {
				if rec.CreatedBy == createdBy {
					filtered = append(filtered, rec)
				}
			}
			recs = filtered
		}
		sortRulesetsByName(recs)
		out = recs
		return nil
	})
	return out, err
}

func sortRulesetsByName(recs []RulesetRecord) {
	for i := 0; i < len(recs); i++ {
		for j := i + 1; j < len(recs); j++ {
			a := recs[i].Name + recs[i].ID
			b := recs[j].Name + recs[j].ID
			if b < a {
				recs[i], recs[j] = recs[j], recs[i]
			}
		}
	}
}

func (rec RulesetRecord) ToFilterRuleset() filter.Ruleset {
	return filter.Ruleset{
		RulesetID:     rec.ID,
		Name:          rec.Name,
		Description:   rec.Description,
		CreatedBy:     rec.CreatedBy,
		SchemaVersion: rec.SchemaVersion,
		RootGroup:     rec.RootGroup,
	}
}

func newRulesetID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}
