package manager

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"codeberg.org/Sylos/Migration-Engine/pkg/migration"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge/apidb"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge/roots"
	"codeberg.org/Sylos/Sylos-API/pkg/config"
	"github.com/rs/zerolog"
)

func TestDeleteMigrationRemovesRegistryAndDir(t *testing.T) {
	t.Parallel()

	dataDir := t.TempDir()
	db, err := apidb.Open(dataDir, testAPIKey(t))
	if err != nil {
		t.Fatalf("open api db: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	id := "mig-delete-me"
	migDir := filepath.Join(dataDir, id)
	opsDir := filepath.Join(migDir, id+".ops")
	if err := os.MkdirAll(opsDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(migDir, id+".db"), []byte{}, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := db.UpsertMigration(apidb.MigrationRecord{
		ID:           id,
		Name:         "Delete Me",
		DatabasePath: filepath.Join(migDir, id+".db"),
		CreatedAt:    time.Now().UTC(),
		Phase:        "awaiting-copy-review",
	}); err != nil {
		t.Fatal(err)
	}

	mgr := &Manager{
		logger: zerolog.Nop(),
		cfg: config.Config{Runtime: config.RuntimeConfig{
			DataDir:               dataDir,
			MigrationDBStorageDir: dataDir,
		}},
		apiDB:        db,
		engineMgr:    migration.NewMigrationManager(),
		rootsMgr:     roots.NewManager(zerolog.Nop(), dataDir, nil, nil),
		runtimeByID:  map[string]*runtimeMigration{},
		progressByID: map[string]map[string]chan corebridge.ProgressEvent{},
	}

	resp, err := mgr.DeleteMigration(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	if resp.ID != id {
		t.Fatalf("id=%q", resp.ID)
	}

	if _, err := db.GetMigration(id); err == nil {
		t.Fatal("expected registry row gone")
	}
	if _, err := os.Stat(migDir); !os.IsNotExist(err) {
		t.Fatalf("migration dir still present, err=%v", err)
	}
}

func TestDeleteMigrationUsesStoredDatabasePath(t *testing.T) {
	t.Parallel()

	dataDir := t.TempDir()
	storageDir := filepath.Join(t.TempDir(), "migration-dbs")
	db, err := apidb.Open(dataDir, testAPIKey(t))
	if err != nil {
		t.Fatalf("open api db: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	id := "mig-stored-path"
	actualDir := filepath.Join(t.TempDir(), "elsewhere", id)
	if err := os.MkdirAll(filepath.Join(actualDir, id+".ops"), 0o755); err != nil {
		t.Fatal(err)
	}
	dbPath := filepath.Join(actualDir, id+".db")
	if err := os.WriteFile(dbPath, []byte{}, 0o644); err != nil {
		t.Fatal(err)
	}
	wrongDir := filepath.Join(storageDir, id)
	if err := os.MkdirAll(wrongDir, 0o755); err != nil {
		t.Fatal(err)
	}

	if err := db.UpsertMigration(apidb.MigrationRecord{
		ID:           id,
		Name:         "Stored Path",
		DatabasePath: dbPath,
		CreatedAt:    time.Now().UTC(),
	}); err != nil {
		t.Fatal(err)
	}

	mgr := &Manager{
		logger: zerolog.Nop(),
		cfg: config.Config{Runtime: config.RuntimeConfig{
			DataDir:               dataDir,
			MigrationDBStorageDir: storageDir,
		}},
		apiDB:        db,
		engineMgr:    migration.NewMigrationManager(),
		rootsMgr:     roots.NewManager(zerolog.Nop(), storageDir, nil, nil),
		runtimeByID:  map[string]*runtimeMigration{},
		progressByID: map[string]map[string]chan corebridge.ProgressEvent{},
	}

	if _, err := mgr.DeleteMigration(context.Background(), id); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(actualDir); !os.IsNotExist(err) {
		t.Fatalf("stored folder still present, err=%v", err)
	}
	if _, err := os.Stat(wrongDir); err != nil {
		t.Fatalf("configured storage folder should be left alone: %v", err)
	}
}

func TestDeleteMigrationRequiresStoredDatabasePath(t *testing.T) {
	t.Parallel()

	dataDir := t.TempDir()
	db, err := apidb.Open(dataDir, testAPIKey(t))
	if err != nil {
		t.Fatalf("open api db: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	id := "mig-no-path"
	if err := db.UpsertMigration(apidb.MigrationRecord{
		ID:        id,
		Name:      "No Path",
		CreatedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatal(err)
	}

	mgr := &Manager{
		logger: zerolog.Nop(),
		cfg: config.Config{Runtime: config.RuntimeConfig{
			DataDir:               dataDir,
			MigrationDBStorageDir: dataDir,
		}},
		apiDB:        db,
		engineMgr:    migration.NewMigrationManager(),
		rootsMgr:     roots.NewManager(zerolog.Nop(), dataDir, nil, nil),
		runtimeByID:  map[string]*runtimeMigration{},
		progressByID: map[string]map[string]chan corebridge.ProgressEvent{},
	}

	if _, err := mgr.DeleteMigration(context.Background(), id); err == nil {
		t.Fatal("expected error when databasePath is missing")
	}
}
