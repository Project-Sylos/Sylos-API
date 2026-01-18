package preferences

// Preferences represents the application preferences structure.
type Preferences struct {
	Theme            string    `json:"theme"`
	SidebarCollapsed bool      `json:"sidebarCollapsed"`
	PreSplashEnabled bool      `json:"preSplashEnabled"`
	Tips             TipsPrefs `json:"tips"`
	Developer        DevPrefs  `json:"developer"`
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

// getDefaultPreferences returns default preferences values.
func getDefaultPreferences() Preferences {
	return Preferences{
		Theme:            "dark",
		SidebarCollapsed: false,
		PreSplashEnabled: true,
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
