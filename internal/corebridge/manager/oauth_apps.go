package manager

import (
	"context"
	"fmt"
	"strings"
	"time"

	"codeberg.org/Sylos/Sylos-API/internal/corebridge/apidb"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge/oauth"
	"codeberg.org/Sylos/Sylos-API/pkg/oauthcreds"
)

// OAuthAppSummary is returned to the UI (no client secret).
type OAuthAppSummary struct {
	ProviderID  string `json:"providerId"`
	DisplayName string `json:"displayName"`
	Configured  bool   `json:"configured"`
	ClientID    string `json:"clientId,omitempty"`
}

type SaveOAuthAppRequest struct {
	ClientID     string `json:"clientId"`
	ClientSecret string `json:"clientSecret"`
	DisplayName  string `json:"displayName"`
}

type TestOAuthAppRequest struct {
	ClientID     string `json:"clientId"`
	ClientSecret string `json:"clientSecret"`
}

func (m *Manager) ListOAuthApps(_ context.Context) ([]OAuthAppSummary, error) {
	known := []struct {
		id   string
		name string
	}{
		{"google_drive", "Google Drive"},
		{"dropbox", "Dropbox"},
	}
	out := make([]OAuthAppSummary, 0, len(known))
	for _, k := range known {
		summary := OAuthAppSummary{
			ProviderID:  k.id,
			DisplayName: k.name,
		}
		if m.apiDB != nil {
			if app, err := m.apiDB.GetProviderOAuthApp(k.id); err == nil {
				summary.Configured = app.ClientID != "" && app.ClientSecret != ""
				summary.ClientID = app.ClientID
				if app.DisplayName != "" {
					summary.DisplayName = app.DisplayName
				}
			}
		}
		out = append(out, summary)
	}
	return out, nil
}

func (m *Manager) SaveOAuthApp(_ context.Context, providerID string, req SaveOAuthAppRequest) error {
	if m.apiDB == nil {
		return fmt.Errorf("API database not configured")
	}
	clientID := strings.TrimSpace(req.ClientID)
	if clientID == "" {
		return fmt.Errorf("clientId is required")
	}

	clientSecret := strings.TrimSpace(req.ClientSecret)
	if clientSecret == "" {
		existing, err := m.apiDB.GetProviderOAuthApp(providerID)
		if err != nil || existing.ClientSecret == "" {
			return fmt.Errorf("clientSecret is required")
		}
		clientSecret = existing.ClientSecret
	}

	displayName := req.DisplayName
	if displayName == "" {
		switch providerID {
		case "google_drive":
			displayName = "Google Drive"
		case "dropbox":
			displayName = "Dropbox"
		default:
			displayName = providerID
		}
	}
	if err := m.apiDB.UpsertProviderOAuthApp(apidb.ProviderOAuthApp{
		ProviderID:   providerID,
		ClientID:     clientID,
		ClientSecret: clientSecret,
		DisplayName:  displayName,
		UpdatedAt:    time.Now().UTC(),
	}); err != nil {
		return err
	}
	m.refreshOAuthCredsFromDB()
	return nil
}

func (m *Manager) TestOAuthApp(_ context.Context, providerID string, req TestOAuthAppRequest) error {
	creds, err := m.resolveOAuthAppCredentials(providerID, req)
	if err != nil {
		return err
	}
	return oauth.ValidateAppCredentials(providerID, creds)
}

func (m *Manager) resolveOAuthAppCredentials(providerID string, req TestOAuthAppRequest) (oauthcreds.ProviderCredentials, error) {
	clientID := strings.TrimSpace(req.ClientID)
	clientSecret := strings.TrimSpace(req.ClientSecret)

	if clientID == "" && m.apiDB != nil {
		if app, err := m.apiDB.GetProviderOAuthApp(providerID); err == nil {
			clientID = app.ClientID
			if clientSecret == "" {
				clientSecret = app.ClientSecret
			}
		}
	}

	if clientID == "" || clientSecret == "" {
		return oauthcreds.ProviderCredentials{}, fmt.Errorf("clientId and clientSecret are required")
	}
	return oauthcreds.ProviderCredentials{
		ClientID:     clientID,
		ClientSecret: clientSecret,
	}, nil
}

func (m *Manager) refreshOAuthCredsFromDB() {
	if m.apiDB == nil {
		return
	}
	apps, err := m.apiDB.ListProviderOAuthApps()
	if err != nil {
		return
	}
	var cfg oauthcreds.Config
	for _, app := range apps {
		creds := &oauthcreds.ProviderCredentials{
			ClientID:     app.ClientID,
			ClientSecret: app.ClientSecret,
		}
		switch app.ProviderID {
		case "google_drive":
			cfg.GoogleDrive = creds
		case "dropbox":
			cfg.Dropbox = creds
		}
	}
	m.oauthCreds = cfg
}
