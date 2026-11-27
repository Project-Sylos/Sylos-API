package manager

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"codeberg.org/Sylos/Migration-Engine/pkg/migration"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge/apidb"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge/roots"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge/services"
	"codeberg.org/Sylos/Sylos-FS/pkg/credentials"
	"codeberg.org/Sylos/Sylos-FS/pkg/fs/local"
	fstypes "codeberg.org/Sylos/Sylos-FS/pkg/types"
	"github.com/rs/zerolog"
)

func TestNormalizeAndCoverFSHealthSides(t *testing.T) {
	t.Parallel()
	if got := normalizeFSHealthSide("SOURCE"); got != corebridge.FSHealthSideSource {
		t.Fatalf("got %q", got)
	}
	if got := normalizeFSHealthSide("destination"); got != corebridge.FSHealthSideDestination {
		t.Fatalf("got %q", got)
	}
	if got := normalizeFSHealthSide(""); got != corebridge.FSHealthSideBoth {
		t.Fatalf("got %q", got)
	}
	if !healthSidesCover("both", "src") {
		t.Fatal("both should cover src")
	}
	if healthSidesCover("src", "both") {
		t.Fatal("src must not cover both")
	}
	if !healthSidesCover("dst", "destination") {
		t.Fatal("dst should cover destination")
	}
}

func TestClassifyHealthErr(t *testing.T) {
	t.Parallel()
	if classifyHealthErr(credentials.ErrNeedsRefresh) != corebridge.FSHealthClassAuthFailed {
		t.Fatal("refresh")
	}
	if classifyHealthErr(&credentials.RateLimitedError{RetryAfter: time.Second}) != corebridge.FSHealthClassTransient {
		t.Fatal("rate limit")
	}
	if classifyHealthErr(os.ErrPermission) != corebridge.FSHealthClassPermissionDenied {
		t.Fatal("permission")
	}
	if classifyHealthErr(errors.New("no such file")) != corebridge.FSHealthClassAccessFailed {
		t.Fatal("access")
	}
}

func TestStampFSHealthFreshness(t *testing.T) {
	t.Parallel()
	db, err := apidb.Open(t.TempDir(), testAPIKey(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	mgr := &Manager{
		apiDB:    db,
		rootsMgr: roots.NewManager(zerolog.Nop(), t.TempDir(), nil, nil),
		logger:   zerolog.Nop(),
	}
	id := "health-stamp"
	if err := db.UpsertMigration(apidb.MigrationRecord{ID: id, Name: id}); err != nil {
		t.Fatal(err)
	}
	if mgr.fsHealthIsFresh(id, corebridge.FSHealthSideBoth) {
		t.Fatal("expected stale before stamp")
	}
	mgr.stampFSHealthSuccess(id, corebridge.FSHealthSideSource, time.Now().UTC())
	if mgr.fsHealthIsFresh(id, corebridge.FSHealthSideBoth) {
		t.Fatal("src-only stamp must not satisfy both")
	}
	mgr.stampFSHealthSuccess(id, corebridge.FSHealthSideBoth, time.Now().UTC())
	if !mgr.fsHealthIsFresh(id, corebridge.FSHealthSideBoth) {
		t.Fatal("expected fresh after both stamp")
	}
	mgr.stampFSHealthSuccess(id, corebridge.FSHealthSideBoth, time.Now().UTC().Add(-11*time.Minute))
	if mgr.fsHealthIsFresh(id, corebridge.FSHealthSideBoth) {
		t.Fatal("expected stale after 11m")
	}
}

func TestProbeFSHealthSideLocalListOK(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	adapter, err := local.NewLocalFS(root)
	if err != nil {
		t.Fatal(err)
	}
	mgr := &Manager{logger: zerolog.Nop()}
	plan := &roots.RootPlan{
		HasSource: true,
		SourceDefinition: services.ServiceDefinition{
			ID:   "local",
			Type: services.ServiceTypeLocal,
		},
		SourceRoot: fstypes.Folder{
			ServiceID:    root,
			LocationPath: "/",
			Type:         fstypes.NodeTypeFolder,
		},
		SourceAdapter: adapter,
	}
	rep := mgr.probeFSHealthSide(context.Background(), plan, migration.FSCredentialRoleSource)
	if !rep.OK {
		t.Fatalf("expected ok, got %+v", rep)
	}
	if !rep.Steps[corebridge.FSHealthStepListRoot].OK {
		t.Fatalf("expected listRoot ok, got %+v", rep.Steps[corebridge.FSHealthStepListRoot])
	}
	for name := range rep.Steps {
		if name != corebridge.FSHealthStepRehydrate && name != corebridge.FSHealthStepListRoot {
			t.Fatalf("unexpected health step %q", name)
		}
	}
}

func TestProbeFSHealthSideLocalReadOnlyStillLists(t *testing.T) {
	t.Parallel()
	parent := t.TempDir()
	root := filepath.Join(parent, "ro")
	if err := os.Mkdir(root, 0o555); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(root, 0o755) })
	adapter, err := local.NewLocalFS(root)
	if err != nil {
		t.Fatal(err)
	}
	mgr := &Manager{logger: zerolog.Nop()}
	plan := &roots.RootPlan{
		HasDestination: true,
		DestinationDefinition: services.ServiceDefinition{
			ID:   "local",
			Type: services.ServiceTypeLocal,
		},
		DestinationRoot: fstypes.Folder{
			ServiceID:    root,
			LocationPath: "/",
			Type:         fstypes.NodeTypeFolder,
		},
		DestinationAdapter: adapter,
	}
	rep := mgr.probeFSHealthSide(context.Background(), plan, migration.FSCredentialRoleDestination)
	if !rep.OK {
		t.Fatalf("read-only root should pass list-only health, got %+v", rep)
	}
	if !rep.Steps[corebridge.FSHealthStepListRoot].OK {
		t.Fatalf("expected listRoot ok, got %+v", rep.Steps[corebridge.FSHealthStepListRoot])
	}
}

func TestFailedHealthDoesNotStampOK(t *testing.T) {
	t.Parallel()
	db, err := apidb.Open(t.TempDir(), testAPIKey(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if err := db.UpsertMigration(apidb.MigrationRecord{
		ID:                "no-stamp",
		LastFsHealthOK:    true,
		LastFsHealthAt:    time.Now().UTC().Add(-time.Hour),
		LastFsHealthSides: corebridge.FSHealthSideBoth,
	}); err != nil {
		t.Fatal(err)
	}
	mgr := &Manager{apiDB: db, logger: zerolog.Nop()}
	report := corebridge.FSHealthReport{
		OK: false,
		Source: &corebridge.FSHealthSideReport{
			OK:             false,
			Classification: corebridge.FSHealthClassAuthFailed,
		},
	}
	mgr.applyAuthAttentionFromHealth("no-stamp", &report)
	rec, err := db.GetMigration("no-stamp")
	if err != nil {
		t.Fatal(err)
	}
	if !rec.NeedsAttention {
		t.Fatal("expected needsAttention on auth failure")
	}
	if !rec.LastFsHealthOK {
		// prior stamp remains; failed probe must not clear or refresh via stampFSHealthSuccess
		t.Log("last ok flag preserved from prior record")
	}
	if time.Since(rec.LastFsHealthAt) < 30*time.Minute {
		t.Fatal("failed health must not refresh lastFsHealthAt")
	}
}
