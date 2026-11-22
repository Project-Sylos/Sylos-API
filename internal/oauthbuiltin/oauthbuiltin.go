package oauthbuiltin

import (
	"errors"
	"strings"
	"time"

	"codeberg.org/Sylos/Sylos-API/internal/corebridge/apidb"
)

// Build-time credentials for Sylos-owned OAuth apps (non-Google).
// The Sylos tree keeps oauth.env (enc:… values) and oauth.key. make build reveals
// them into ldflags. When vars are empty at process start, ApplyObscuredEnvFile
// fills them from those files if present. Empty values skip seeding that provider.
var (
	DropboxClientID        string
	DropboxClientSecret    string
	BoxClientID            string
	BoxClientSecret        string
	OneDriveClientID       string
	OneDriveClientSecret   string
	OneDriveTenantID       string
	SharePointClientID     string
	SharePointClientSecret string
	SharePointTenantID     string
)

type appSpec struct {
	ProviderID   string
	DisplayName  string
	ClientID     string
	ClientSecret string
	TenantID     string
}

// SeedMissingApps inserts builtin provider OAuth apps when the registry row is absent.
// Existing admin-saved rows are never overwritten.
func SeedMissingApps(db *apidb.DB) (int, error) {
	if db == nil {
		return 0, nil
	}
	specs := []appSpec{
		{ProviderID: "dropbox", DisplayName: "Dropbox", ClientID: DropboxClientID, ClientSecret: DropboxClientSecret},
		{ProviderID: "box", DisplayName: "Box", ClientID: BoxClientID, ClientSecret: BoxClientSecret},
		{ProviderID: "onedrive", DisplayName: "OneDrive", ClientID: OneDriveClientID, ClientSecret: OneDriveClientSecret, TenantID: OneDriveTenantID},
		{ProviderID: "sharepoint", DisplayName: "SharePoint", ClientID: SharePointClientID, ClientSecret: SharePointClientSecret, TenantID: SharePointTenantID},
	}
	seeded := 0
	for _, spec := range specs {
		id := strings.TrimSpace(spec.ClientID)
		secret := strings.TrimSpace(spec.ClientSecret)
		if id == "" || secret == "" {
			continue
		}
		existing, err := db.GetProviderOAuthApp(spec.ProviderID)
		if err == nil && strings.TrimSpace(existing.ClientID) != "" {
			continue
		}
		if err != nil && !errors.Is(err, apidb.ErrNotFound) {
			return seeded, err
		}
		app := apidb.ProviderOAuthApp{
			ProviderID:           spec.ProviderID,
			ClientID:             id,
			ClientSecret:         secret,
			TenantID:             strings.TrimSpace(spec.TenantID),
			DisplayName:          spec.DisplayName,
			IsDefault:            true,
			UpdatedAt:            time.Now().UTC(),
			HealthStatus:         "unknown",
			HealthMonitorEnabled: true,
		}
		if err := db.UpsertProviderOAuthApp(app); err != nil {
			return seeded, err
		}
		seeded++
	}
	return seeded, nil
}
