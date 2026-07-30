package apidb

import "testing"

func TestScalingOverridesCRUD(t *testing.T) {
	dir := t.TempDir()
	key := make([]byte, 32)
	for i := range key {
		key[i] = byte(i + 5)
	}
	db, err := Open(dir, key)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })

	if err := db.UpsertModes(ScopeProvider, "box", map[string]int{
		"traversal": 16,
		"copy_files": 8,
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.UpsertModes(ScopeSFTPHost, "host-1", map[string]int{
		"traversal": 4,
	}); err != nil {
		t.Fatal(err)
	}

	all, err := db.ListAll()
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 3 {
		t.Fatalf("ListAll len=%d want 3", len(all))
	}

	byScope, err := db.ListByScope(ScopeProvider)
	if err != nil {
		t.Fatal(err)
	}
	if len(byScope) != 2 {
		t.Fatalf("ListByScope provider len=%d want 2", len(byScope))
	}

	if err := db.DeleteMode(ScopeProvider, "box", "copy_files"); err != nil {
		t.Fatal(err)
	}
	byScope, err = db.ListByScope(ScopeProvider)
	if err != nil {
		t.Fatal(err)
	}
	if len(byScope) != 1 || byScope[0].Mode != "traversal" {
		t.Fatalf("after DeleteMode = %+v", byScope)
	}

	if err := db.UpsertModes(ScopeProvider, "box", nil); err != nil {
		t.Fatal(err)
	}
	byScope, err = db.ListByScope(ScopeProvider)
	if err != nil {
		t.Fatal(err)
	}
	if len(byScope) != 0 {
		t.Fatalf("empty upsert should clear, got %+v", byScope)
	}

	if err := db.DeleteScope(ScopeSFTPHost, "host-1"); err != nil {
		t.Fatal(err)
	}
	all, err = db.ListAll()
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 0 {
		t.Fatalf("after deletes ListAll=%+v", all)
	}
}
