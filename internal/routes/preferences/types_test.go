package preferences

import "testing"

func TestDevPrefsStallTimeoutSecs(t *testing.T) {
	zero := 0
	custom := 120
	d := DevPrefs{
		StallTimeoutTraversalSec: &zero,
		StallTimeoutDeleteSec:    &custom,
	}
	trav, del, copySec := d.StallTimeoutSecs()
	if trav != 0 {
		t.Fatalf("traversal unlimited: got %d", trav)
	}
	if del != 120 {
		t.Fatalf("delete custom: got %d", del)
	}
	if copySec != defaultFSStallTimeoutSec {
		t.Fatalf("copy default: got %d", copySec)
	}
}

func TestSanitizePreferencesClearsPreSplashForObsidian(t *testing.T) {
	prefs := Preferences{
		Theme:            "obsidian",
		PreSplashEnabled: true,
	}
	got := sanitizePreferences(prefs)
	if got.PreSplashEnabled {
		t.Fatal("expected preSplashEnabled to be cleared for obsidian theme")
	}
}

func TestValidatePreferencesRejectsPreSplashForQuartz(t *testing.T) {
	prefs := Preferences{
		Theme:            "quartz",
		PreSplashEnabled: true,
	}
	_, err := validatePreferences(prefs)
	if err == nil {
		t.Fatal("expected validation error when enabling preSplash on quartz theme")
	}
}

func TestValidatePreferencesAllowsPreSplashForNeonDark(t *testing.T) {
	prefs := Preferences{
		Theme:            "neon-dark",
		PreSplashEnabled: true,
	}
	got, err := validatePreferences(prefs)
	if err != nil {
		t.Fatalf("unexpected validation error: %v", err)
	}
	if !got.PreSplashEnabled {
		t.Fatal("expected preSplashEnabled to remain enabled for neon-dark theme")
	}
}
