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
	"codeberg.org/Sylos/Sylos-API/internal/corebridge/connections"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge/roots"
	"codeberg.org/Sylos/Sylos-API/pkg/config"
	"github.com/rs/zerolog"
)

func TestClearAllMigrationsUsesStoredDatabasePath(t *testing.T) {
	t.Parallel()

	dataDir := t.TempDir()
	storageDir := filepath.Join(t.TempDir(), "migration-dbs")
	if err := os.MkdirAll(storageDir, 0o755); err != nil {
		t.Fatal(err)
	}
	db, err := apidb.Open(dataDir, testAPIKey(t))
	if err != nil {
		t.Fatalf("open api db: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	id := "mig-clear-stored"
	actualDir := filepath.Join(t.TempDir(), "elsewhere", id)
	if err := os.MkdirAll(filepath.Join(actualDir, id+".ops"), 0o755); err != nil {
		t.Fatal(err)
	}
	dbPath := filepath.Join(actualDir, id+".db")
	if err := os.WriteFile(dbPath, []byte{}, 0o644); err != nil {
		t.Fatal(err)
	}

	if err := db.UpsertMigration(apidb.MigrationRecord{
		ID:           id,
		Name:         "Clear Stored",
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
		connMgr:      connections.NewManager(),
		runtimeByID:  map[string]*runtimeMigration{},
		progressByID: map[string]map[string]chan corebridge.ProgressEvent{},
	}

	if _, err := mgr.ClearAllMigrations(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(actualDir); !os.IsNotExist(err) {
		t.Fatalf("stored folder still present, err=%v", err)
	}
	if _, err := db.GetMigration(id); err == nil {
		t.Fatal("expected registry row gone")
	}
}
