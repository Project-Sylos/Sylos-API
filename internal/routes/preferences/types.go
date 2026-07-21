package preferences

import "fmt"

// Preferences represents the application preferences structure.
type Preferences struct {
	Theme              string    `json:"theme"`
	SidebarCollapsed   bool      `json:"sidebarCollapsed"`
	HideUnsetServices  bool      `json:"hideUnsetServices"`
	PreSplashEnabled   bool      `json:"preSplashEnabled"`
	Tips               TipsPrefs `json:"tips"`
	Developer          DevPrefs  `json:"developer"`
}

// TipsPrefs represents the tips preferences.
type TipsPrefs struct {
	Enabled    bool                   `json:"enabled"`
	Categories map[string]interface{} `json:"categories"`
}

// DevPrefs represents the developer preferences.
type DevPrefs struct {
	Enabled            bool `json:"enabled"`
	ShowSpectraService bool `json:"showSpectraService"`
}

func getDefaultPreferences() Preferences {
	return Preferences{
		Theme:            "obsidian",
		SidebarCollapsed: true,
		PreSplashEnabled: false,
		Tips: TipsPrefs{
			Enabled:    true,
			Categories: make(map[string]interface{}),
		},
		Developer: DevPrefs{
			Enabled:            false,
			ShowSpectraService: false,
		},
	}
}

func mergeWithDefaults(stored Preferences) Preferences {
	defaults := getDefaultPreferences()

	if stored.Theme == "" {
		stored.Theme = defaults.Theme
	}
	if stored.Tips.Categories == nil {
		stored.Tips.Categories = defaults.Tips.Categories
	}

	return stored
}

func themeSupportsPreSplash(theme string) bool {
	return theme == "neon-dark" || theme == "neon-light"
}

func sanitizePreferences(prefs Preferences) Preferences {
	prefs = mergeWithDefaults(prefs)
	if !themeSupportsPreSplash(prefs.Theme) {
		prefs.PreSplashEnabled = false
	}
	return prefs
}

func validatePreferences(prefs Preferences) (Preferences, error) {
	prefs = mergeWithDefaults(prefs)
	if !themeSupportsPreSplash(prefs.Theme) && prefs.PreSplashEnabled {
		return prefs, fmt.Errorf("preSplashEnabled is only allowed with neon-dark or neon-light theme")
	}
	return sanitizePreferences(prefs), nil
}
