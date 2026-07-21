package oauth

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"codeberg.org/Sylos/Sylos-API/pkg/oauthcreds"
)

type tokenErrorResponse struct {
	Error            string `json:"error"`
	ErrorDescription string `json:"error_description"`
}

// ValidateAppCredentials checks OAuth app credentials by attempting a token exchange
// with an invalid authorization code. Providers return invalid_grant when the client
// authenticated successfully, and invalid_client when credentials are wrong.
func ValidateAppCredentials(providerID string, creds oauthcreds.ProviderCredentials) error {
	if strings.TrimSpace(creds.ClientID) == "" || strings.TrimSpace(creds.ClientSecret) == "" {
		return fmt.Errorf("clientId and clientSecret are required")
	}

	var tokenURL string
	switch providerID {
	case "google_drive":
		tokenURL = googleTokenURL
	case "dropbox":
		tokenURL = dropboxTokenURL
	case "onedrive", "sharepoint":
		tokenURL = microsoftTokenURL
	case "box":
		tokenURL = boxTokenURL
	default:
		return fmt.Errorf("unsupported oauth provider %q", providerID)
	}

	body := url.Values{
		"code":          {"sylos_oauth_app_credential_probe"},
		"client_id":     {creds.ClientID},
		"client_secret": {creds.ClientSecret},
		"redirect_uri":  {"http://127.0.0.1/oauth/callback"},
		"grant_type":    {"authorization_code"},
	}

	req, err := http.NewRequest(http.MethodPost, tokenURL, strings.NewReader(body.Encode()))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	client := &http.Client{Timeout: 15 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("could not reach %s: %w", providerID, err)
	}
	defer resp.Body.Close()

	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return nil
	}

	var errResp tokenErrorResponse
	if err := json.Unmarshal(raw, &errResp); err != nil || errResp.Error == "" {
		return fmt.Errorf("credential check failed: %s", strings.TrimSpace(string(raw)))
	}

	switch errResp.Error {
	case "invalid_grant", "bad_verification_code":
		return nil
	case "invalid_client":
		return fmt.Errorf("invalid client ID or client secret")
	default:
		if errResp.ErrorDescription != "" {
			return fmt.Errorf("%s: %s", errResp.Error, errResp.ErrorDescription)
		}
		return fmt.Errorf("credential check failed: %s", errResp.Error)
	}
}
