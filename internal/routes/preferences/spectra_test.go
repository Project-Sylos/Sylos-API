package preferences

import "testing"

func TestShowSpectraServiceRequiresDeveloperMode(t *testing.T) {
	if ShowSpectraService(Preferences{Developer: DevPrefs{ShowSpectraService: true}}) {
		t.Fatal("expected spectra hidden when developer mode disabled")
	}
}

func TestShowSpectraServiceWhenEnabled(t *testing.T) {
	prefs := Preferences{
		Developer: DevPrefs{
			Enabled:            true,
			ShowSpectraService: true,
		},
	}
	if !ShowSpectraService(prefs) {
		t.Fatal("expected spectra visible when developer prefs enabled")
	}
}

func TestShowSpectraServiceFromJSONDefaults(t *testing.T) {
	if ShowSpectraServiceFromJSON("") {
		t.Fatal("expected spectra hidden for empty preferences")
	}
}
