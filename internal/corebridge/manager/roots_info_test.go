package manager

import (
	"testing"

	"codeberg.org/Sylos/Migration-Engine/pkg/migration"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge"
)

func TestDefaultMigrationName(t *testing.T) {
	src := &corebridge.RootInfo{NativePath: "/home/loganm"}
	dst := &corebridge.RootInfo{NativePath: "/mnt/nas-home"}

	got := defaultMigrationName(src, dst)
	want := "/home/loganm \u2192 /mnt/nas-home"
	if got != want {
		t.Fatalf("full name = %q, want %q", got, want)
	}

	got = defaultMigrationName(src, nil)
	want = "/home/loganm \u2192 ?"
	if got != want {
		t.Fatalf("partial source name = %q, want %q", got, want)
	}
}

func TestIsUnnamedMigrationPartialAutoName(t *testing.T) {
	mig := &migration.Migration{
		ID:   "abc123",
		Name: "/home/loganm \u2192 ?",
	}

	if !isUnnamedMigration(mig) {
		t.Fatal("partial auto name should still be treated as unnamed")
	}

	mig.Name = "/home/loganm \u2192 /mnt/nas-home"
	if isUnnamedMigration(mig) {
		t.Fatal("complete auto name should not be treated as unnamed")
	}

	mig.Name = "My backup"
	if isUnnamedMigration(mig) {
		t.Fatal("user rename should not be treated as unnamed")
	}
}

func TestRootLabelPrefersNativePath(t *testing.T) {
	info := &corebridge.RootInfo{
		NativePath:   "/mnt/nas-home",
		LocationPath: "/",
		Name:         "nas-home",
	}
	if got := rootLabel(info); got != "/mnt/nas-home" {
		t.Fatalf("rootLabel = %q, want /mnt/nas-home", got)
	}
}

func TestRootLabelCloudDriveRoot(t *testing.T) {
	info := &corebridge.RootInfo{
		ServiceName:  "Google Drive",
		ServiceType:  "cloud",
		NativePath:   "root",
		LocationPath: "/",
		Name:         "My Drive",
	}
	got := rootLabel(info)
	want := "Google Drive \u00b7 My Drive"
	if got != want {
		t.Fatalf("rootLabel = %q, want %q", got, want)
	}
}

func TestRootLabelDropboxAccountRoot(t *testing.T) {
	info := &corebridge.RootInfo{
		ServiceName:  "Dropbox",
		ServiceType:  "cloud",
		NativePath:   "root",
		LocationPath: "/",
		Name:         "Logan's Dropbox",
	}
	got := rootLabel(info)
	want := "Dropbox \u00b7 Logan's Dropbox"
	if got != want {
		t.Fatalf("rootLabel = %q, want %q", got, want)
	}
}

func TestDefaultMigrationNameCloudRoots(t *testing.T) {
	src := &corebridge.RootInfo{
		ServiceName:  "Google Drive",
		ServiceType:  "cloud",
		NativePath:   "root",
		LocationPath: "/",
		Name:         "My Drive",
	}
	dst := &corebridge.RootInfo{
		ServiceName:  "Dropbox",
		ServiceType:  "cloud",
		NativePath:   "root",
		LocationPath: "/",
		Name:         "Logan's Dropbox",
	}
	got := defaultMigrationName(src, dst)
	want := "Google Drive \u00b7 My Drive \u2192 Dropbox \u00b7 Logan's Dropbox"
	if got != want {
		t.Fatalf("cloud migration name = %q, want %q", got, want)
	}
}

func TestIsPlaceholderAutoMigrationName(t *testing.T) {
	if !isPlaceholderAutoMigrationName("root \u2192 root") {
		t.Fatal("expected root → root to be a placeholder name")
	}
	if isPlaceholderAutoMigrationName("Google Drive \u00b7 My Drive \u2192 Dropbox \u00b7 Logan's Dropbox") {
		t.Fatal("expected real cloud name not to be a placeholder")
	}
}
