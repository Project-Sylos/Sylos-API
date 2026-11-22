package oauthbuiltin

import (
	"testing"

	"codeberg.org/Sylos/Sylos-API/internal/corebridge/apidb"
)

func TestSeedMissingAppsSkipsEmptyAndDoesNotOverwrite(t *testing.T) {
	dir := t.TempDir()
	key := make([]byte, 32)
	for i := range key {
		key[i] = byte(i + 1)
	}
	db, err := apidb.Open(dir, key)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })

	prevID, prevSecret := DropboxClientID, DropboxClientSecret
	t.Cleanup(func() {
		DropboxClientID, DropboxClientSecret = prevID, prevSecret
	})

	DropboxClientID = ""
	DropboxClientSecret = ""
	n, err := SeedMissingApps(db)
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("seeded=%d want 0", n)
	}

	DropboxClientID = "dbx-id"
	DropboxClientSecret = "dbx-secret"
	n, err = SeedMissingApps(db)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("seeded=%d want 1", n)
	}
	app, err := db.GetProviderOAuthApp("dropbox")
	if err != nil {
		t.Fatal(err)
	}
	if app.ClientID != "dbx-id" || !app.IsDefault {
		t.Fatalf("app=%+v", app)
	}

	DropboxClientID = "other"
	DropboxClientSecret = "other-secret"
	n, err = SeedMissingApps(db)
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("overwrite seeded=%d want 0", n)
	}
	app, err = db.GetProviderOAuthApp("dropbox")
	if err != nil {
		t.Fatal(err)
	}
	if app.ClientID != "dbx-id" {
		t.Fatalf("overwrote client id to %q", app.ClientID)
	}
}
