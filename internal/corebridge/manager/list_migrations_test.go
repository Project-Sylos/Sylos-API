package manager

import (
	"context"
	"testing"
	"time"

	"codeberg.org/Sylos/Migration-Engine/pkg/migration"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge/apidb"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge/roots"
	"github.com/rs/zerolog"
)

// Listing migrations for the resume page must not open any per-migration
// ops DB: a Badger open takes an exclusive directory lock and replays the
// store. Cold migrations are summarized from the registry record instead.
func TestListAllMigrationsColdRecordsDoNotOpenOpsDB(t *testing.T) {
	t.Parallel()

	db, err := apidb.Open(t.TempDir(), testAPIKey(t))
	if err != nil {
		t.Fatalf("open api db: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	created := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	if err := db.UpsertMigration(apidb.MigrationRecord{
		ID:        "mig-cold",
		Name:      "Cold migration",
		CreatedAt: created,
		Phase:     "traversal-suspended",
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
	if resp.Total != 1 || len(resp.Migrations) != 1 {
		t.Fatalf("total=%d migrations=%d want 1", resp.Total, len(resp.Migrations))
	}
	got := resp.Migrations[0]
	if got.ID != "mig-cold" || got.Name != "Cold migration" {
		t.Fatalf("got id=%q name=%q", got.ID, got.Name)
	}
	if got.Status != "traversal-suspended" {
		t.Fatalf("status=%q want last known phase from registry", got.Status)
	}
	if !got.StartedAt.Equal(created) {
		t.Fatalf("startedAt=%v want registry createdAt %v", got.StartedAt, created)
	}

	loaded, err := mgr.engineMgr.ListMigrations("")
	if err != nil {
		t.Fatal(err)
	}
	if len(loaded) != 0 {
		t.Fatalf("engine loaded %d migrations; list must not open ops DBs", len(loaded))
	}
}

// A full status read persists the observed phase so later lists can show
// it without opening the ops DB. Unchanged phases must not rewrite.
func TestPersistLastKnownPhase(t *testing.T) {
	t.Parallel()

	db, err := apidb.Open(t.TempDir(), testAPIKey(t))
	if err != nil {
		t.Fatalf("open api db: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	if err := db.UpsertMigration(apidb.MigrationRecord{ID: "mig-1", Name: "One"}); err != nil {
		t.Fatal(err)
	}
	mgr := &Manager{apiDB: db}

	mgr.persistLastKnownPhase("mig-1", "copy-suspended")
	rec, err := db.GetMigration("mig-1")
	if err != nil {
		t.Fatal(err)
	}
	if rec.Phase != "copy-suspended" {
		t.Fatalf("phase=%q want copy-suspended", rec.Phase)
	}

	mgr.persistLastKnownPhase("mig-1", "")
	mgr.persistLastKnownPhase("missing", "copy-suspended")
	rec, err = db.GetMigration("mig-1")
	if err != nil {
		t.Fatal(err)
	}
	if rec.Phase != "copy-suspended" {
		t.Fatalf("phase=%q want unchanged copy-suspended", rec.Phase)
	}
	if _, err := db.GetMigration("missing"); err == nil {
		t.Fatal("persistLastKnownPhase created a registry record for an unknown migration")
	}
}

// Cold list entries must surface registry root snapshots so sidebar glyphs
// keep service type (e.g. local cyan) after process restart.
func TestListAllMigrationsColdRecordsIncludeRootSnapshots(t *testing.T) {
	t.Parallel()

	db, err := apidb.Open(t.TempDir(), testAPIKey(t))
	if err != nil {
		t.Fatalf("open api db: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	if err := db.UpsertMigration(apidb.MigrationRecord{
		ID:   "mig-roots",
		Name: "With roots",
		SourceRoot: &apidb.MigrationRootSnapshot{
			ServiceID:   "local",
			ServiceType: "local",
			Name:        "SSD",
		},
		DestinationRoot: &apidb.MigrationRootSnapshot{
			ServiceID:   "local",
			ServiceType: "local",
			Name:        "Backup",
		},
		Phase: "awaiting-traversal-review",
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
		t.Fatalf("migrations=%d want 1", len(resp.Migrations))
	}
	got := resp.Migrations[0]
	if got.SourceRoot == nil || got.SourceRoot.ServiceType != "local" {
		t.Fatalf("sourceRoot=%+v want serviceType=local", got.SourceRoot)
	}
	if got.DestinationRoot == nil || got.DestinationRoot.ServiceType != "local" {
		t.Fatalf("destinationRoot=%+v want serviceType=local", got.DestinationRoot)
	}
	if got.SourceID != "local" || got.DestinationID != "local" {
		t.Fatalf("sourceID=%q destinationID=%q", got.SourceID, got.DestinationID)
	}
}
