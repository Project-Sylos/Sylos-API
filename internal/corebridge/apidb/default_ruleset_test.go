package apidb

import (
	"testing"
	"time"

	"codeberg.org/Sylos/Migration-Engine/pkg/filter"
	badger "github.com/dgraph-io/badger/v4"
)

func openTestDB(t *testing.T) *DB {
	t.Helper()
	key := make([]byte, 32)
	for i := range key {
		key[i] = byte(i + 11)
	}
	db, err := Open(t.TempDir(), key)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func insertTestUser(t *testing.T, db *DB, id, prefsJSON string) {
	t.Helper()
	err := db.CreateUser(UserRecord{
		ID:           id,
		Username:     id,
		PasswordHash: "hash",
		Role:         "admin",
		CreatedAt:    time.Now().UTC(),
		Preferences:  prefsJSON,
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestSeedBuiltinRulesetsKeepsEdits(t *testing.T) {
	db := openTestDB(t)

	rec, err := db.GetRuleset(DefaultAutoApplyRulesetID)
	if err != nil {
		t.Fatalf("default ruleset was not seeded: %v", err)
	}
	if rec.CreatedBy != CreatedByDefault {
		t.Fatalf("created_by = %q, want %q so the row stays editable", rec.CreatedBy, CreatedByDefault)
	}

	rec.Name = "My junk list"
	rec.RootGroup = filter.Group{Op: filter.OpOR, Children: []filter.Child{
		{Condition: &filter.Condition{ID: "ini", Field: filter.FieldName, Operator: filter.OpEQ, Value: "desktop.ini", AppliesTo: filter.AppliesFile}},
	}}
	if err := db.UpdateRuleset(rec); err != nil {
		t.Fatalf("default ruleset should be editable: %v", err)
	}

	if err := db.SeedBuiltinRulesets(); err != nil {
		t.Fatal(err)
	}
	after, err := db.GetRuleset(DefaultAutoApplyRulesetID)
	if err != nil {
		t.Fatal(err)
	}
	if after.Name != "My junk list" {
		t.Fatalf("reseeding overwrote the edited default: name = %q", after.Name)
	}
}

// writeStaleRuleset rewrites a row the way an older binary would have seeded it,
// leaving the timestamps alone so the row still counts as untouched.
func writeStaleRuleset(t *testing.T, db *DB, id string) {
	t.Helper()
	rec, err := db.GetRuleset(id)
	if err != nil {
		t.Fatal(err)
	}
	rec.Name = "Older shipped copy"
	rec.RootGroup = filter.Group{Op: filter.OpOR, Children: []filter.Child{
		{Condition: &filter.Condition{ID: "thumbs", Field: filter.FieldName, Operator: filter.OpEQ, Value: "Thumbs.db", AppliesTo: filter.AppliesFile}},
		{Condition: &filter.Condition{ID: "tmp", Field: filter.FieldName, Operator: filter.OpGlob, Value: "*.tmp", AppliesTo: filter.AppliesFile}},
	}}
	if err := db.update(func(txn *badger.Txn) error {
		return putJSON(txn, keyRuleset(id), rec)
	}); err != nil {
		t.Fatal(err)
	}
}

func TestSeedBuiltinRulesetsRefreshesShippedCopies(t *testing.T) {
	db := openTestDB(t)
	for _, id := range []string{"prepack-common-junk", DefaultAutoApplyRulesetID} {
		writeStaleRuleset(t, db, id)
	}

	if err := db.SeedBuiltinRulesets(); err != nil {
		t.Fatal(err)
	}

	want := len(commonJunkGroup().Children)
	for _, id := range []string{"prepack-common-junk", DefaultAutoApplyRulesetID} {
		after, err := db.GetRuleset(id)
		if err != nil {
			t.Fatal(err)
		}
		if after.Name == "Older shipped copy" {
			t.Fatalf("%s: kept the stale name", id)
		}
		if len(after.RootGroup.Children) != want {
			t.Fatalf("%s: root group has %d children, want the shipped %d",
				id, len(after.RootGroup.Children), want)
		}
	}
}

func TestDefaultRulesetForUser(t *testing.T) {
	db := openTestDB(t)
	custom, err := db.CreateRuleset(RulesetRecord{
		Name:      "Only spreadsheets",
		CreatedBy: "someone",
		RootGroup: filter.Group{Op: filter.OpAND, Children: []filter.Child{
			{Condition: &filter.Condition{ID: "tmp", Field: filter.FieldName, Operator: filter.OpGlob, Value: "*.tmp", AppliesTo: filter.AppliesFile}},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}

	insertTestUser(t, db, "never-chose", `{"theme":"obsidian"}`)
	insertTestUser(t, db, "turned-it-off", `{"theme":"obsidian","defaultRulesetId":""}`)
	insertTestUser(t, db, "picked-one", `{"theme":"obsidian","defaultRulesetId":"`+custom.ID+`"}`)
	insertTestUser(t, db, "stale-choice", `{"theme":"obsidian","defaultRulesetId":"deleted-long-ago"}`)

	cases := []struct {
		user   string
		wantID *string
	}{
		{"never-chose", strPtr(DefaultAutoApplyRulesetID)},
		{"no-such-user", strPtr(DefaultAutoApplyRulesetID)},
		{"turned-it-off", nil},
		{"picked-one", strPtr(custom.ID)},
		{"stale-choice", nil},
	}
	for _, tc := range cases {
		got, err := db.DefaultRulesetForUser(tc.user)
		if err != nil {
			t.Fatalf("%s: %v", tc.user, err)
		}
		if tc.wantID == nil {
			if got != nil {
				t.Fatalf("%s: want no starter ruleset, got %q", tc.user, got.RulesetID)
			}
			continue
		}
		if got == nil {
			t.Fatalf("%s: want %q, got no starter ruleset", tc.user, *tc.wantID)
		}
		if got.RulesetID != *tc.wantID {
			t.Fatalf("%s: want %q, got %q", tc.user, *tc.wantID, got.RulesetID)
		}
	}
}

func strPtr(s string) *string {
	return &s
}
