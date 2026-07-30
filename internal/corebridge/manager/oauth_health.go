package manager

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"codeberg.org/Sylos/Sylos-API/internal/corebridge/apidb"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge/oauth"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge/services"
	"codeberg.org/Sylos/Sylos-API/pkg/oauthcreds"
)

const installConfigOAuthHealthScanMinutes = "oauth_health_scan_interval_minutes"

const (
	OAuthHealthHealthy        = "healthy"
	OAuthHealthUnhealthy      = "unhealthy"
	OAuthHealthSkipped        = "skipped"
	OAuthHealthNotConfigured  = "not_configured"
	OAuthHealthUnknown        = "unknown"
)

// OAuthHealthSummary is returned to the UI for settings badges.
type OAuthHealthSummary struct {
	HasIssues       bool `json:"hasIssues"`
	UnhealthyCount  int  `json:"unhealthyCount"`
	ConfiguredCount int  `json:"configuredCount"`
}

// OAuthHealthSettings controls background credential checks.
type OAuthHealthSettings struct {
	ScanIntervalMinutes int `json:"scanIntervalMinutes"`
}

var oauthHealthScanOptions = []int{0, 15, 30, 60, 360, 720, 1440, 10080, 43200}

func (m *Manager) GetOAuthHealthSummary(_ context.Context) (OAuthHealthSummary, error) {
	apps, err := m.ListOAuthApps(context.Background())
	if err != nil {
		return OAuthHealthSummary{}, err
	}
	var summary OAuthHealthSummary
	for _, app := range apps {
		if !app.Configured || !app.HealthMonitorEnabled {
			continue
		}
		summary.ConfiguredCount++
		if app.HealthStatus == OAuthHealthUnhealthy {
			summary.UnhealthyCount++
			summary.HasIssues = true
		}
	}
	return summary, nil
}

func (m *Manager) GetOAuthHealthSettings(_ context.Context) (OAuthHealthSettings, error) {
	settings := OAuthHealthSettings{ScanIntervalMinutes: 0}
	if m.apiDB == nil {
		return settings, nil
	}
	raw, err := m.apiDB.GetInstallConfig(installConfigOAuthHealthScanMinutes)
	if err != nil {
		return settings, nil
	}
	if n, err := strconv.Atoi(strings.TrimSpace(raw)); err == nil {
		settings.ScanIntervalMinutes = n
	}
	return settings, nil
}

func (m *Manager) SaveOAuthHealthSettings(_ context.Context, settings OAuthHealthSettings) error {
	if m.apiDB == nil {
		return nil
	}
	if !isAllowedOAuthScanInterval(settings.ScanIntervalMinutes) {
		return fmt.Errorf("invalid oauth health scan interval")
	}
	if err := m.apiDB.SetInstallConfig(installConfigOAuthHealthScanMinutes, strconv.Itoa(settings.ScanIntervalMinutes)); err != nil {
		return err
	}
	m.rescheduleOAuthHealthMonitor()
	return nil
}

func (m *Manager) rescheduleOAuthHealthMonitor() {
	m.oauthHealthRescheduleMu.Lock()
	fn := m.oauthHealthReschedule
	m.oauthHealthRescheduleMu.Unlock()
	if fn != nil {
		fn()
	}
}

func isAllowedOAuthScanInterval(minutes int) bool {
	for _, opt := range oauthHealthScanOptions {
		if opt == minutes {
			return true
		}
	}
	return false
}

// StartOAuthHealthMonitor runs checks on startup and on the configured interval.
func (m *Manager) StartOAuthHealthMonitor(ctx context.Context) {
	if m.apiDB == nil {
		return
	}
	go func() {
		m.runOAuthHealthChecks(context.Background())

		var timer *time.Timer
		var timerMu sync.Mutex
		var schedule func()
		schedule = func() {
			timerMu.Lock()
			defer timerMu.Unlock()
			if timer != nil {
				timer.Stop()
			}
			settings, err := m.GetOAuthHealthSettings(context.Background())
			if err != nil || settings.ScanIntervalMinutes <= 0 {
				return
			}
			timer = time.AfterFunc(time.Duration(settings.ScanIntervalMinutes)*time.Minute, func() {
				m.runOAuthHealthChecks(context.Background())
				schedule()
			})
		}
		schedule()
		m.oauthHealthRescheduleMu.Lock()
		m.oauthHealthReschedule = schedule
		m.oauthHealthRescheduleMu.Unlock()

		<-ctx.Done()
		timerMu.Lock()
		if timer != nil {
			timer.Stop()
		}
		timerMu.Unlock()
	}()
}

func (m *Manager) runOAuthHealthChecks(ctx context.Context) {
	if ctx.Err() != nil {
		return
	}
	inUse := m.providersInUseByLiveMigrations()
	for _, providerID := range knownOAuthProviderIDs() {
		if ctx.Err() != nil {
			return
		}
		if !m.shouldMonitorOAuthProvider(providerID) {
			continue
		}
		skip := inUse[providerID]
		_, _ = m.checkOAuthProviderHealth(providerID, skip)
	}
}

func (m *Manager) shouldMonitorOAuthProvider(providerID string) bool {
	if m.apiDB == nil {
		return false
	}
	app, err := m.apiDB.GetProviderOAuthApp(providerID)
	if err != nil {
		return false
	}
	return app.ClientID != "" && app.ClientSecret != "" && app.HealthMonitorEnabled
}

func (m *Manager) checkOAuthProviderHealth(providerID string, skipBecauseLive bool) (OAuthAppSummary, error) {
	summary := OAuthAppSummary{
		ProviderID:   providerID,
		DisplayName:  oauthProviderDisplayName(providerID),
		HealthStatus: OAuthHealthNotConfigured,
	}
	if m.apiDB == nil {
		return summary, nil
	}

	app, err := m.apiDB.GetProviderOAuthApp(providerID)
	if err != nil {
		summary.HealthStatus = OAuthHealthNotConfigured
		return summary, nil
	}
	summary.Configured = app.ClientID != "" && app.ClientSecret != ""
	summary.ClientID = app.ClientID
	if app.DisplayName != "" {
		summary.DisplayName = app.DisplayName
	}
	if !summary.Configured {
		summary.HealthStatus = OAuthHealthNotConfigured
		return summary, nil
	}

	now := time.Now().UTC()
	if skipBecauseLive {
		summary.HealthStatus = OAuthHealthSkipped
		summary.HealthError = "Skipped while provider is in use by a live migration"
		summary.LastCheckedAt = &now
		_ = m.apiDB.UpdateProviderOAuthHealth(providerID, summary.HealthStatus, now, summary.HealthError)
		return summary, nil
	}

	creds := oauthcreds.ProviderCredentials{
		ClientID:     app.ClientID,
		ClientSecret: app.ClientSecret,
		TenantID:     app.TenantID,
	}
	checkErr := oauth.ValidateAppCredentials(providerID, creds)
	if checkErr != nil {
		summary.HealthStatus = OAuthHealthUnhealthy
		summary.HealthError = checkErr.Error()
	} else {
		summary.HealthStatus = OAuthHealthHealthy
		summary.HealthError = ""
		_ = m.apiDB.SetProviderOAuthHealthMonitor(providerID, true)
	}
	summary.LastCheckedAt = &now
	_ = m.apiDB.UpdateProviderOAuthHealth(providerID, summary.HealthStatus, now, summary.HealthError)
	return summary, nil
}

func (m *Manager) providersInUseByLiveMigrations() map[string]bool {
	inUse := make(map[string]bool)
	m.mu.RLock()
	defer m.mu.RUnlock()
	for _, rec := range m.runtimeByID {
		if rec == nil || rec.Migration == nil || !rec.Migration.IsLive() {
			continue
		}
		plan := m.rootsMgr.GetPlan(rec.Migration.ID)
		if plan == nil {
			continue
		}
		for _, def := range []services.ServiceDefinition{plan.SourceDefinition, plan.DestinationDefinition} {
			if def.Type == services.ServiceTypeCloud {
				inUse[services.CloudProviderID(def)] = true
			}
		}
	}
	return inUse
}

func knownOAuthProviderIDs() []string {
	return []string{"google_drive", "dropbox", "onedrive", "sharepoint", "box"}
}

func oauthProviderDisplayName(providerID string) string {
	switch providerID {
	case "google_drive":
		return "Google Drive"
	case "dropbox":
		return "Dropbox"
	case "onedrive":
		return "OneDrive"
	case "sharepoint":
		return "SharePoint"
	case "box":
		return "Box"
	default:
		return providerID
	}
}

func oauthSummaryFromApp(app apidb.ProviderOAuthApp, fallbackName string) OAuthAppSummary {
	summary := OAuthAppSummary{
		ProviderID:  app.ProviderID,
		DisplayName: fallbackName,
		Configured:  app.ClientID != "" && app.ClientSecret != "",
		ClientID:    app.ClientID,
		TenantID:    app.TenantID,
	}
	if app.DisplayName != "" {
		summary.DisplayName = app.DisplayName
	}
	if summary.Configured {
		if app.HealthStatus != "" {
			summary.HealthStatus = app.HealthStatus
		} else {
			summary.HealthStatus = OAuthHealthUnknown
		}
		summary.HealthError = app.HealthError
		summary.HealthMonitorEnabled = app.HealthMonitorEnabled
		if app.HealthCheckedAt != nil {
			t := app.HealthCheckedAt.UTC()
			summary.LastCheckedAt = &t
		}
	} else {
		summary.HealthStatus = OAuthHealthNotConfigured
	}
	return summary
}
