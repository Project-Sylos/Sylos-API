package corebridge

import "time"

type ProviderDescriptor struct {
	ID            string   `json:"id"`
	DisplayName   string   `json:"displayName"`
	ServiceID     string   `json:"serviceId"`
	AuthType      string   `json:"authType"`
	Scopes        []string `json:"scopes,omitempty"`
	OAuthClientID string   `json:"oauthClientId,omitempty"`
	OAuthTenantID string   `json:"oauthTenantId,omitempty"`
}

type ConnectionResponse struct {
	ConnectionID string `json:"connectionId"`
	ProviderID   string `json:"providerId"`
}

type OAuthTokenRequest struct {
	AccessToken  string   `json:"access_token,omitempty"`
	RefreshToken string   `json:"refresh_token"`
	ExpiresIn    int64    `json:"expires_in,omitempty"`
	Scopes       []string `json:"scopes,omitempty"`
	ClientID     string   `json:"client_id,omitempty"`
	ClientSecret string   `json:"client_secret,omitempty"`
	TokenURI     string   `json:"token_uri,omitempty"`
}

type OAuthExchangeRequest struct {
	Code        string   `json:"code"`
	RedirectURI string   `json:"redirect_uri"`
	Scopes      []string `json:"scopes,omitempty"`
	// MicrosoftAccountType is "personal" or "business" for OneDrive/SharePoint.
	// personal → consumers; business → configured tenant_id or organizations.
	MicrosoftAccountType string `json:"microsoft_account_type,omitempty"`
}

type SFTPCredentialsRequest struct {
	Host          string `json:"host"`
	Port          int    `json:"port,omitempty"`
	Username      string `json:"username"`
	Password      string `json:"password,omitempty"`
	PrivateKey    string `json:"privateKey,omitempty"`
	KeyPassphrase string `json:"keyPassphrase,omitempty"`
	HostKey       string `json:"hostKey,omitempty"`
}

type SFTPHostKeyProbeRequest struct {
	Host string `json:"host"`
	Port int    `json:"port,omitempty"`
}

type SFTPHostKeyProbeResponse struct {
	HostKey        string `json:"hostKey"`
	Fingerprint    string `json:"fingerprint"`
	Trusted        bool   `json:"trusted"`
	HostKeyChanged bool   `json:"hostKeyChanged,omitempty"`
}

// SFTPSavedHostSummary is a remembered SFTP site without decrypted secrets.
type SFTPSavedHostSummary struct {
	ID          string     `json:"id"`
	DisplayName string     `json:"displayName"`
	Host        string     `json:"host"`
	Port        int        `json:"port"`
	Username    string     `json:"username"`
	AuthMethod  string     `json:"authMethod"`
	UpdatedAt   time.Time  `json:"updatedAt"`
	LastUsedAt  *time.Time `json:"lastUsedAt,omitempty"`
}

// SFTPSavedHostDetail includes secrets for reconnecting from the credentials form.
type SFTPSavedHostDetail struct {
	SFTPSavedHostSummary
	Password      string `json:"password,omitempty"`
	PrivateKey    string `json:"privateKey,omitempty"`
	KeyPassphrase string `json:"keyPassphrase,omitempty"`
	HostKey       string `json:"hostKey,omitempty"`
}

// SFTPSavedHostUpsertRequest creates or updates a remembered SFTP site.
type SFTPSavedHostUpsertRequest struct {
	ID            string `json:"id,omitempty"`
	DisplayName   string `json:"displayName,omitempty"`
	Host          string `json:"host"`
	Port          int    `json:"port,omitempty"`
	Username      string `json:"username"`
	AuthMethod    string `json:"authMethod"`
	Password      string `json:"password,omitempty"`
	PrivateKey    string `json:"privateKey,omitempty"`
	KeyPassphrase string `json:"keyPassphrase,omitempty"`
	HostKey       string `json:"hostKey,omitempty"`
}

type ConnectionStatus struct {
	ConnectionID       string    `json:"connectionId"`
	ProviderID         string    `json:"providerId"`
	Valid              bool      `json:"valid"`
	ExpiresAt          time.Time `json:"expiresAt,omitempty"`
	AccountEmail       string    `json:"accountEmail,omitempty"`
	AccountDisplayName string    `json:"accountDisplayName,omitempty"`
}

type CreateConnectionRequest struct {
	MigrationID string `json:"migrationId,omitempty"`
}
