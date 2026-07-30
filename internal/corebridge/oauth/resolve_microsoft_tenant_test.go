package oauth

import "testing"

func TestResolveMicrosoftTenant(t *testing.T) {
	// Authority is always common while Entra apps use AzureADandPersonalMicrosoftAccount.
	cases := []struct {
		configured string
		account    string
	}{
		{"", "personal"},
		{"03087318-6294-4852-a7e9-8dfa58d92899", "personal"},
		{"", "business"},
		{"03087318-6294-4852-a7e9-8dfa58d92899", "business"},
		{"", ""},
		{"tid", ""},
	}
	for _, tc := range cases {
		got := ResolveMicrosoftTenant(tc.configured, tc.account)
		if got != "common" {
			t.Fatalf("configured=%q account=%q got %q want common", tc.configured, tc.account, got)
		}
	}
}
