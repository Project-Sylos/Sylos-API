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
	ProviderID    string     `json:"providerId"`
	DisplayName   string     `json:"displayName"`
	Configured    bool       `json:"configured"`
	ClientID      string     `json:"clientId,omitempty"`
	HealthStatus         string     `json:"healthStatus"`
	HealthError          string     `json:"healthError,omitempty"`
	LastCheckedAt        *time.Time `json:"lastCheckedAt,omitempty"`
	HealthMonitorEnabled bool       `json:"healthMonitorEnabled"`
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
			ProviderID:   k.id,
			DisplayName:  k.name,
			HealthStatus: OAuthHealthNotConfigured,
		}
		if m.apiDB != nil {
			if app, err := m.apiDB.GetProviderOAuthApp(k.id); err == nil {
				summary = oauthSummaryFromApp(app, k.name)
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
		displayName = oauthProviderDisplayName(providerID)
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
	summary, _ := m.checkOAuthProviderHealth(providerID, false)
	_ = summary
	return nil
}

func (m *Manager) DeleteOAuthApp(_ context.Context, providerID string) error {
	if m.apiDB == nil {
		return fmt.Errorf("API database not configured")
	}
	if err := m.apiDB.DeleteProviderOAuthApp(providerID); err != nil {
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
	checkErr := oauth.ValidateAppCredentials(providerID, creds)
	inUse := m.providersInUseByLiveMigrations()
	skip := inUse[providerID]
	if skip {
		_, _ = m.checkOAuthProviderHealth(providerID, true)
		return fmt.Errorf("cannot test %s while it is in use by a live migration", providerID)
	}
	if checkErr != nil {
		_, _ = m.checkOAuthProviderHealth(providerID, false)
		return checkErr
	}
	_, _ = m.checkOAuthProviderHealth(providerID, false)
	return nil
}

func (m *Manager) resolveOAuthAppCredentials(providerID string, req TestOAuthAppRequest) (oauthcreds.ProviderCredentials, error) {
	clientID := strings.TrimSpace(req.ClientID)
	clientSecret := strings.TrimSpace(req.ClientSecret)

	if m.apiDB != nil {
		if app, err := m.apiDB.GetProviderOAuthApp(providerID); err == nil {
			if clientID == "" {
				clientID = app.ClientID
			}
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
