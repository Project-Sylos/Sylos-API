package preferences

import (
	"encoding/json"
	"fmt"
	"strings"

	"codeberg.org/Sylos/Sylos-API/internal/corebridge/apidb"
)

// Preferences represents the application preferences structure.
type Preferences struct {
	Theme             string `json:"theme"`
	SidebarCollapsed  bool   `json:"sidebarCollapsed"`
	HideUnsetServices bool   `json:"hideUnsetServices"`
	PreSplashEnabled  bool   `json:"preSplashEnabled"`
	// DefaultRulesetID is a stored starter ruleset preference. Filters are applied
	// from path review search rather than bound automatically to new migrations.
	DefaultRulesetID *string   `json:"defaultRulesetId"`
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
	Enabled               bool `json:"enabled"`
	ShowSpectraService    bool `json:"showSpectraService"`
	DisableDBQueryTimeout bool `json:"disableDbQueryTimeout"`
	// FS ProgressWatchdog budgets in seconds. 0 = unlimited (no stall abort).
	// Defaults are 60 when unset in stored JSON (see mergeWithDefaults).
	StallTimeoutTraversalSec *int `json:"stallTimeoutTraversalSec,omitempty"`
	StallTimeoutDeleteSec    *int `json:"stallTimeoutDeleteSec,omitempty"`
	StallTimeoutCopySec      *int `json:"stallTimeoutCopySec,omitempty"`
}

const defaultFSStallTimeoutSec = 60

// StallTimeoutSecs returns traversal/delete/copy stall seconds (0 = unlimited).
func (d DevPrefs) StallTimeoutSecs() (traversal, delete, copy int) {
	return stallSecPtr(d.StallTimeoutTraversalSec),
		stallSecPtr(d.StallTimeoutDeleteSec),
		stallSecPtr(d.StallTimeoutCopySec)
}

func stallSecPtr(p *int) int {
	if p == nil {
		return defaultFSStallTimeoutSec
	}
	if *p < 0 {
		return defaultFSStallTimeoutSec
	}
	return *p
}

func intPtr(v int) *int { return &v }

func getDefaultPreferences() Preferences {
	defaultRulesetID := apidb.DefaultAutoApplyRulesetID
	return Preferences{
		Theme:            "obsidian",
		SidebarCollapsed: true,
		PreSplashEnabled: false,
		DefaultRulesetID: &defaultRulesetID,
		Tips: TipsPrefs{
			Enabled:    true,
			Categories: make(map[string]interface{}),
		},
		Developer: DevPrefs{
			Enabled:                  false,
			ShowSpectraService:       false,
			StallTimeoutTraversalSec: intPtr(defaultFSStallTimeoutSec),
			StallTimeoutDeleteSec:    intPtr(defaultFSStallTimeoutSec),
			StallTimeoutCopySec:      intPtr(defaultFSStallTimeoutSec),
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
	if stored.DefaultRulesetID == nil {
		stored.DefaultRulesetID = defaults.DefaultRulesetID
	}
	if stored.Developer.StallTimeoutTraversalSec == nil {
		stored.Developer.StallTimeoutTraversalSec = intPtr(defaultFSStallTimeoutSec)
	}
	if stored.Developer.StallTimeoutDeleteSec == nil {
		stored.Developer.StallTimeoutDeleteSec = intPtr(defaultFSStallTimeoutSec)
	}
	if stored.Developer.StallTimeoutCopySec == nil {
		stored.Developer.StallTimeoutCopySec = intPtr(defaultFSStallTimeoutSec)
	}

	return stored
}

func themeSupportsPreSplash(theme string) bool {
	return theme == "neon-dark" || theme == "neon-light"
}

// DisableDBQueryTimeoutFromJSON reports whether developer.disableDbQueryTimeout is set in stored prefs JSON.
func DisableDBQueryTimeoutFromJSON(raw string) bool {
	if strings.TrimSpace(raw) == "" {
		return false
	}
	var prefs Preferences
	if err := json.Unmarshal([]byte(raw), &prefs); err != nil {
		return false
	}
	return prefs.Developer.Enabled && prefs.Developer.DisableDBQueryTimeout
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
