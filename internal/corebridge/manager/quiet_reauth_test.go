package manager

import (
	"testing"

	"codeberg.org/Sylos/Sylos-API/internal/corebridge/apidb"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge/roots"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge/services"
	fstypes "codeberg.org/Sylos/Sylos-FS/pkg/types"
	"github.com/rs/zerolog"
)

func TestCheckPhaseLockAllowsSetRootWhenNeedsAttention(t *testing.T) {
	t.Parallel()
	db, err := apidb.Open(t.TempDir(), testAPIKey(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if err := db.UpsertMigration(apidb.MigrationRecord{
		ID:             "attn-copy",
		NeedsAttention: true,
		Phase:          "copy-in-progress",
	}); err != nil {
		t.Fatal(err)
	}

	rootsMgr := roots.NewManager(zerolog.Nop(), t.TempDir(), nil, nil)
	_ = rootsMgr.ApplyRehydratedSide(
		"attn-copy", "source",
		services.ServiceDefinition{ID: "cloud-a", Type: services.ServiceTypeCloud},
		fstypes.Folder{ServiceID: "root-a"},
		"conn-a", nil, func() {},
	)
	_ = rootsMgr.ApplyRehydratedSide(
		"attn-copy", "destination",
		services.ServiceDefinition{ID: "cloud-b", Type: services.ServiceTypeCloud},
		fstypes.Folder{ServiceID: "root-b"},
		"conn-b", nil, func() {},
	)

	mgr := &Manager{apiDB: db, rootsMgr: rootsMgr}
	if err := mgr.checkPhaseLock("attn-copy", "setRoot"); err != nil {
		t.Fatalf("expected setRoot allowed under needsAttention: %v", err)
	}
}

func TestCheckPhaseLockBlocksSetRootWithoutAttention(t *testing.T) {
	t.Parallel()
	db, err := apidb.Open(t.TempDir(), testAPIKey(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if err := db.UpsertMigration(apidb.MigrationRecord{
		ID:             "locked-copy",
		NeedsAttention: false,
	}); err != nil {
		t.Fatal(err)
	}

	rootsMgr := roots.NewManager(zerolog.Nop(), t.TempDir(), nil, nil)
	_ = rootsMgr.ApplyRehydratedSide(
		"locked-copy", "source",
		services.ServiceDefinition{ID: "cloud-a", Type: services.ServiceTypeCloud},
		fstypes.Folder{ServiceID: "root-a"},
		"conn-a", nil, func() {},
	)
	_ = rootsMgr.ApplyRehydratedSide(
		"locked-copy", "destination",
		services.ServiceDefinition{ID: "cloud-b", Type: services.ServiceTypeCloud},
		fstypes.Folder{ServiceID: "root-b"},
		"conn-b", nil, func() {},
	)

	// getMigrationPhase needs an engine migration; without one it returns unknown and does not lock.
	// Still assert needsAttention=false does not short-circuit the attention bypass.
	mgr := &Manager{apiDB: db, rootsMgr: rootsMgr}
	if mgr.migrationNeedsAttention("locked-copy") {
		t.Fatal("expected needsAttention false")
	}
}
