package preferences

import "encoding/json"

// ShowSpectraService reports whether Spectra should appear in service discovery for this user.
func ShowSpectraService(prefs Preferences) bool {
	prefs = sanitizePreferences(prefs)
	return prefs.Developer.Enabled && prefs.Developer.ShowSpectraService
}

// ShowSpectraServiceFromJSON parses stored preferences JSON and reports whether Spectra is visible.
// Invalid or empty JSON uses defaults (Spectra hidden).
func ShowSpectraServiceFromJSON(raw string) bool {
	if raw == "" {
		return ShowSpectraService(getDefaultPreferences())
	}
	var prefs Preferences
	if err := json.Unmarshal([]byte(raw), &prefs); err != nil {
		return ShowSpectraService(getDefaultPreferences())
	}
	return ShowSpectraService(prefs)
}
