package preferences

import (
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"

	"github.com/Project-Sylos/Sylos-API/internal/routes/middleware"
)

func (h handler) getPreferences(ctx *middleware.Context) {
	prefsPath := filepath.Join(h.dataDir, "preferences.json")

	// Read preferences file
	data, err := os.ReadFile(prefsPath)
	if err != nil {
		if os.IsNotExist(err) {
			// File doesn't exist, return default preferences
			h.logger.Debug().Str("prefs_path", prefsPath).Msg("preferences file not found, returning defaults")
			defaultPrefs := getDefaultPreferences()
			ctx.Response(http.StatusOK, defaultPrefs)
			return
		}
		h.logger.Error().Err(err).Str("prefs_path", prefsPath).Msg("failed to read preferences file")
		ctx.Error(http.StatusInternalServerError, "failed to read preferences", err)
		return
	}

	var prefs Preferences
	if err := json.Unmarshal(data, &prefs); err != nil {
		h.logger.Error().Err(err).Str("prefs_path", prefsPath).Msg("failed to parse preferences JSON")
		ctx.Error(http.StatusInternalServerError, "failed to parse preferences", err)
		return
	}

	h.logger.Debug().Str("prefs_path", prefsPath).Msg("preferences loaded successfully")
	ctx.Response(http.StatusOK, prefs)
}
