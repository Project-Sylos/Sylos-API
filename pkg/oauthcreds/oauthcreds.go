package oauthcreds

type ProviderCredentials struct {
	ClientID     string `json:"client_id"`
	ClientSecret string `json:"client_secret"`
	// TenantID is the Microsoft Entra directory (tenant) ID for OneDrive/SharePoint.
	// Empty means the multi-tenant "common" endpoint.
	TenantID string `json:"tenant_id,omitempty"`
}

type Config struct {
	GoogleDrive *ProviderCredentials `json:"google_drive,omitempty"`
	Dropbox     *ProviderCredentials `json:"dropbox,omitempty"`
	OneDrive    *ProviderCredentials `json:"onedrive,omitempty"`
	SharePoint  *ProviderCredentials `json:"sharepoint,omitempty"`
	Box         *ProviderCredentials `json:"box,omitempty"`
}
