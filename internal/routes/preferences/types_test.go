package preferences

import "testing"

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
