package oauth

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"codeberg.org/Sylos/Sylos-API/pkg/oauthcreds"
)

func TestValidateAppCredentials_invalidClient(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"error":"invalid_client","error_description":"Unauthorized"}`))
	}))
	defer server.Close()

	orig := googleTokenURL
	googleTokenURL = server.URL
	t.Cleanup(func() { googleTokenURL = orig })

	err := ValidateAppCredentials("google_drive", oauthcreds.ProviderCredentials{
		ClientID:     "bad",
		ClientSecret: "bad",
	})
	if err == nil {
		t.Fatal("expected error for invalid_client")
	}
}

func TestValidateAppCredentials_invalidGrantOK(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"error":"invalid_grant","error_description":"Malformed auth code."}`))
	}))
	defer server.Close()

	orig := googleTokenURL
	googleTokenURL = server.URL
	t.Cleanup(func() { googleTokenURL = orig })

	err := ValidateAppCredentials("google_drive", oauthcreds.ProviderCredentials{
		ClientID:     "ok",
		ClientSecret: "ok",
	})
	if err != nil {
		t.Fatalf("expected success for invalid_grant probe, got %v", err)
	}
}
