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

type TokenResponse struct {
	AccessToken  string `json:"access_token"`
	RefreshToken string `json:"refresh_token"`
	ExpiresIn    int64  `json:"expires_in"`
	Scope        string `json:"scope"`
	TokenType    string `json:"token_type"`
}

var (
	googleTokenURL  = "https://oauth2.googleapis.com/token"
	dropboxTokenURL = "https://api.dropboxapi.com/oauth2/token"
)

func ExchangeAuthCode(providerID string, creds oauthcreds.ProviderCredentials, code, redirectURI string) (TokenResponse, error) {
	switch providerID {
	case "google_drive":
		return postTokenExchange(googleTokenURL, creds, code, redirectURI)
	case "dropbox":
		return postTokenExchange(dropboxTokenURL, creds, code, redirectURI)
	default:
		return TokenResponse{}, fmt.Errorf("unsupported oauth provider %q", providerID)
	}
}

func postTokenExchange(tokenURL string, creds oauthcreds.ProviderCredentials, code, redirectURI string) (TokenResponse, error) {
	body := url.Values{
		"code":          {code},
		"client_id":     {creds.ClientID},
		"client_secret": {creds.ClientSecret},
		"redirect_uri":  {redirectURI},
		"grant_type":    {"authorization_code"},
	}

	req, err := http.NewRequest(http.MethodPost, tokenURL, strings.NewReader(body.Encode()))
	if err != nil {
		return TokenResponse{}, err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return TokenResponse{}, err
	}
	defer resp.Body.Close()

	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return TokenResponse{}, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return TokenResponse{}, fmt.Errorf("token exchange failed: %s", strings.TrimSpace(string(raw)))
	}

	var tokens TokenResponse
	if err := json.Unmarshal(raw, &tokens); err != nil {
		return TokenResponse{}, err
	}
	return tokens, nil
}
