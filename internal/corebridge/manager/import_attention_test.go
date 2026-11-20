package manager

import (
	"context"
	"errors"
	"testing"

	"codeberg.org/Sylos/Migration-Engine/pkg/migration"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge/apidb"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge/roots"
	"github.com/rs/zerolog"
)

func TestColdMigrationStatusIncludesAttention(t *testing.T) {
	t.Parallel()
	db, err := apidb.Open(t.TempDir(), testAPIKey(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })

	if err := db.UpsertMigration(apidb.MigrationRecord{
		ID:               "mig-attn",
		Name:             "Needs auth",
		NeedsAttention:   true,
		AttentionReasons: []string{apidb.AttentionCredentialsMissing},
		LocalRoots: []apidb.LocalRootProbe{{
			Role: "source", NativePath: "/old/path", PathExistsOnHost: false, ServiceID: "local",
		}},
	}); err != nil {
		t.Fatal(err)
	}

	mgr := &Manager{
		apiDB:       db,
		engineMgr:   migration.NewMigrationManager(),
		rootsMgr:    roots.NewManager(zerolog.Nop(), t.TempDir(), nil, nil),
		runtimeByID: map[string]*runtimeMigration{},
	}
	resp, err := mgr.ListAllMigrations(context.Background(), corebridge.ListMigrationsRequest{Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.Migrations) != 1 {
		t.Fatalf("len=%d", len(resp.Migrations))
	}
	got := resp.Migrations[0]
	if !got.NeedsAttention {
		t.Fatal("expected needsAttention")
	}
	if len(got.AttentionReasons) != 1 || got.AttentionReasons[0] != apidb.AttentionCredentialsMissing {
		t.Fatalf("reasons=%v", got.AttentionReasons)
	}
	if len(got.LocalRoots) != 1 || got.LocalRoots[0].NativePath != "/old/path" {
		t.Fatalf("localRoots=%v", got.LocalRoots)
	}
}

func TestRejectIfNeedsAttention(t *testing.T) {
	t.Parallel()
	db, err := apidb.Open(t.TempDir(), testAPIKey(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if err := db.UpsertMigration(apidb.MigrationRecord{
		ID:             "blocked",
		NeedsAttention: true,
	}); err != nil {
		t.Fatal(err)
	}
	mgr := &Manager{apiDB: db}
	if err := mgr.rejectIfNeedsAttention("blocked"); !errors.Is(err, ErrMigrationNeedsAttention) {
		t.Fatalf("err=%v", err)
	}
	if err := mgr.ensureFSAdaptersRehydrated("blocked"); !errors.Is(err, ErrMigrationNeedsAttention) {
		t.Fatalf("ensureFSAdaptersRehydrated err=%v", err)
	}
	if err := db.UpsertMigration(apidb.MigrationRecord{ID: "blocked", NeedsAttention: false}); err != nil {
		t.Fatal(err)
	}
	if err := mgr.rejectIfNeedsAttention("blocked"); err != nil {
		t.Fatalf("cleared attention still blocked: %v", err)
	}
}
