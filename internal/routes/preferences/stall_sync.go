// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package preferences

import (
	"encoding/json"

	"codeberg.org/Sylos/Sylos-API/internal/auth/users"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge/manager"
)

// SyncFSStallTimeoutsFromUsers loads stall timeout prefs from the first user that
// has preferences JSON and applies them to the migration manager.
func SyncFSStallTimeoutsFromUsers(mgr *manager.Manager, store *users.Store) {
	if mgr == nil || store == nil {
		return
	}
	recs, err := store.List()
	if err != nil || len(recs) == 0 {
		applyStallTimeoutPrefs(mgr, getDefaultPreferences())
		return
	}
	for _, rec := range recs {
		raw, err := store.GetPreferencesJSON(rec.ID)
		if err != nil || raw == "" {
			continue
		}
		var prefs Preferences
		if json.Unmarshal([]byte(raw), &prefs) != nil {
			continue
		}
		applyStallTimeoutPrefs(mgr, sanitizePreferences(prefs))
		return
	}
	applyStallTimeoutPrefs(mgr, getDefaultPreferences())
}
