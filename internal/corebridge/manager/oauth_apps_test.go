package manager

import (
	"context"
	"testing"

	"codeberg.org/Sylos/Sylos-API/internal/corebridge/apidb"
)

func testAPIKey(t *testing.T) []byte {
	t.Helper()
	key := make([]byte, 32)
	for i := range key {
		key[i] = byte(i + 1)
	}
	return key
}

func TestResolveOAuthAppCredentialsUsesStoredSecret(t *testing.T) {
	t.Parallel()

	db, err := apidb.Open(t.TempDir(), testAPIKey(t))
	if err != nil {
		t.Fatalf("open api db: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	if err := db.UpsertProviderOAuthApp(apidb.ProviderOAuthApp{
		ProviderID:   "google_drive",
		ClientID:     "stored-client-id",
		ClientSecret: "stored-client-secret",
		DisplayName:  "Google Drive",
	}); err != nil {
		t.Fatalf("upsert oauth app: %v", err)
	}

	stored, err := db.GetProviderOAuthApp("google_drive")
	if err != nil {
		t.Fatalf("get oauth app: %v", err)
	}
	if stored.ClientSecret != "stored-client-secret" {
		t.Fatalf("stored secret mismatch: %+v", stored)
	}

	mgr := &Manager{apiDB: db}
	creds, err := mgr.resolveOAuthAppCredentials("google_drive", TestOAuthAppRequest{
		ClientID: "stored-client-id",
	})
	if err != nil {
		t.Fatalf("resolve credentials: %v", err)
	}
	if creds.ClientID != "stored-client-id" || creds.ClientSecret != "stored-client-secret" {
		t.Fatalf("unexpected creds: %+v", creds)
	}

	creds, err = mgr.resolveOAuthAppCredentials("google_drive", TestOAuthAppRequest{})
	if err != nil {
		t.Fatalf("resolve stored credentials: %v", err)
	}
	if creds.ClientSecret != "stored-client-secret" {
		t.Fatalf("expected stored secret, got %+v", creds)
	}
}

func TestResolveOAuthAppCredentialsRequiresBothFields(t *testing.T) {
	t.Parallel()

	db, err := apidb.Open(t.TempDir(), testAPIKey(t))
	if err != nil {
		t.Fatalf("open api db: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	mgr := &Manager{apiDB: db}
	_, err = mgr.resolveOAuthAppCredentials("dropbox", TestOAuthAppRequest{ClientID: "only-id"})
	if err == nil {
		t.Fatal("expected error when client secret is missing and nothing is stored")
	}
}

func TestSaveOAuthAppAlsoAppliesToMicrosoftSibling(t *testing.T) {
	t.Parallel()

	db, err := apidb.Open(t.TempDir(), testAPIKey(t))
	if err != nil {
		t.Fatalf("open api db: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	mgr := &Manager{apiDB: db}
	if err := mgr.SaveOAuthApp(context.Background(), "onedrive", SaveOAuthAppRequest{
		ClientID:     "ms-client",
		ClientSecret: "ms-secret",
		TenantID:     "03087318-6294-4852-a7e9-8dfa58d92899",
		DisplayName:  "OneDrive",
		AlsoApplyTo:  "sharepoint",
	}); err != nil {
		t.Fatalf("save onedrive: %v", err)
	}

	od, err := db.GetProviderOAuthApp("onedrive")
	if err != nil {
		t.Fatalf("get onedrive: %v", err)
	}
	sp, err := db.GetProviderOAuthApp("sharepoint")
	if err != nil {
		t.Fatalf("get sharepoint: %v", err)
	}
	if od.TenantID != "03087318-6294-4852-a7e9-8dfa58d92899" {
		t.Fatalf("onedrive tenant=%q", od.TenantID)
	}
	if sp.ClientID != "ms-client" || sp.ClientSecret != "ms-secret" || sp.TenantID != od.TenantID {
		t.Fatalf("sharepoint not copied: %+v", sp)
	}
}
