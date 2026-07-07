package oauthcreds

type ProviderCredentials struct {
	ClientID     string `json:"client_id"`
	ClientSecret string `json:"client_secret"`
}

type Config struct {
	GoogleDrive *ProviderCredentials `json:"google_drive,omitempty"`
	Dropbox     *ProviderCredentials `json:"dropbox,omitempty"`
}
