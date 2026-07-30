package manager

import (
	"testing"

	"codeberg.org/Sylos/Migration-Engine/pkg/scaling/profile"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge/apidb"
)

func TestValidateWorkerCapModesBounds(t *testing.T) {
	t.Parallel()

	_, err := validateWorkerCapModes(map[string]int{"traversal": 0})
	if err == nil {
		t.Fatal("expected error for max_workers < 1")
	}
	_, err = validateWorkerCapModes(map[string]int{"traversal": profile.AbsoluteMaxWorkers + 1})
	if err == nil {
		t.Fatal("expected error above AbsoluteMaxWorkers")
	}
	_, err = validateWorkerCapModes(map[string]int{"nope": 8})
	if err == nil {
		t.Fatal("expected error for invalid mode")
	}
	got, err := validateWorkerCapModes(map[string]int{"traversal": 1, "copy_files": profile.AbsoluteMaxWorkers})
	if err != nil {
		t.Fatal(err)
	}
	if got["traversal"] != 1 || got["copy_files"] != profile.AbsoluteMaxWorkers {
		t.Fatalf("got=%v", got)
	}
}

func TestResolveWorkerCapOverridesSFTPHostBeatsProvider(t *testing.T) {
	t.Parallel()

	db, err := apidb.Open(t.TempDir(), testAPIKey(t))
	if err != nil {
		t.Fatalf("open api db: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	if err := db.UpsertModes(apidb.ScopeProvider, "sftp", map[string]int{
		"traversal":  64,
		"copy_files": 32,
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.UpsertModes(apidb.ScopeSFTPHost, "host-a", map[string]int{
		"traversal": 8,
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.UpsertModes(apidb.ScopeProvider, "local", map[string]int{
		"traversal": 16,
	}); err != nil {
		t.Fatal(err)
	}

	mgr := &Manager{apiDB: db}

	// Host id present: use sftp_host for src (not provider fallback); provider for dst; min per mode.
	got := mgr.ResolveWorkerCapOverrides("sftp", "local", "host-a", "")
	if got.MaxFor(profile.WorkerCapTraversal) != 8 {
		t.Fatalf("traversal=%d want 8 (min of host 8 and local 16)", got.MaxFor(profile.WorkerCapTraversal))
	}
	if got.MaxFor(profile.WorkerCapCopyFiles) != 0 {
		t.Fatalf("copy_files=%d want 0 (host replaces provider; host has no copy_files)", got.MaxFor(profile.WorkerCapCopyFiles))
	}

	// No host id: provider-level sftp vs local.
	got = mgr.ResolveWorkerCapOverrides("sftp", "local", "", "")
	if got.MaxFor(profile.WorkerCapTraversal) != 16 {
		t.Fatalf("provider-only traversal=%d want 16", got.MaxFor(profile.WorkerCapTraversal))
	}
	if got.MaxFor(profile.WorkerCapCopyFiles) != 32 {
		t.Fatalf("provider-only copy_files=%d want 32", got.MaxFor(profile.WorkerCapCopyFiles))
	}
}

func TestListScalingDefaultsIncludesSFTP(t *testing.T) {
	t.Parallel()
	mgr := &Manager{}
	defaults := mgr.ListScalingDefaults()
	for _, id := range []string{"generic", "local", "spectra", "sftp", "box"} {
		if _, ok := defaults[id]; !ok {
			t.Fatalf("missing default for %q", id)
		}
	}
	if defaults["sftp"].ProviderID != "sftp" {
		t.Fatalf("sftp provider id = %q", defaults["sftp"].ProviderID)
	}
}

func TestSaveScalingOverridesEmptyClears(t *testing.T) {
	t.Parallel()

	db, err := apidb.Open(t.TempDir(), testAPIKey(t))
	if err != nil {
		t.Fatalf("open api db: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	mgr := &Manager{apiDB: db}
	if err := mgr.SaveScalingOverrides(apidb.ScopeProvider, "box", map[string]int{"traversal": 10}); err != nil {
		t.Fatal(err)
	}
	if err := mgr.SaveScalingOverrides(apidb.ScopeProvider, "box", nil); err != nil {
		t.Fatal(err)
	}
	rows, err := db.ListByScope(apidb.ScopeProvider)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 0 {
		t.Fatalf("expected cleared overrides, got %+v", rows)
	}
}
