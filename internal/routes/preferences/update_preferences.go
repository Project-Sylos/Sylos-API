package preferences

import (
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"

	"github.com/Project-Sylos/Sylos-API/internal/routes/middleware"
)

func (h handler) updatePreferences(ctx *middleware.Context, prefs Preferences) {
	prefsPath := filepath.Join(h.dataDir, "preferences.json")

	// Ensure data directory exists
	if err := os.MkdirAll(h.dataDir, 0755); err != nil {
		h.logger.Error().Err(err).Str("data_dir", h.dataDir).Msg("failed to create data directory")
		ctx.Error(http.StatusInternalServerError, "failed to create data directory", err)
		return
	}

	// Marshal preferences to JSON with indentation for readability
	data, err := json.MarshalIndent(prefs, "", "  ")
	if err != nil {
		h.logger.Error().Err(err).Msg("failed to serialize preferences")
		ctx.Error(http.StatusInternalServerError, "failed to serialize preferences", err)
		return
	}

	// Write atomically: write to temp file, then rename
	tmpPath := prefsPath + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0644); err != nil {
		h.logger.Error().Err(err).Str("tmp_path", tmpPath).Msg("failed to write preferences to temp file")
		ctx.Error(http.StatusInternalServerError, "failed to write preferences", err)
		return
	}

	// Atomic rename - on Windows, remove existing file first if it exists
	if _, err := os.Stat(prefsPath); err == nil {
		// File exists, remove it first (Windows requires this)
		if err := os.Remove(prefsPath); err != nil {
			_ = os.Remove(tmpPath)
			h.logger.Error().Err(err).Str("prefs_path", prefsPath).Msg("failed to remove existing preferences file")
			ctx.Error(http.StatusInternalServerError, "failed to save preferences", err)
			return
		}
	}

	if err := os.Rename(tmpPath, prefsPath); err != nil {
		// Clean up temp file if rename fails
		_ = os.Remove(tmpPath)
		h.logger.Error().Err(err).Str("tmp_path", tmpPath).Str("prefs_path", prefsPath).Msg("failed to rename temp file to preferences file")
		ctx.Error(http.StatusInternalServerError, "failed to save preferences", err)
		return
	}

	h.logger.Info().Str("prefs_path", prefsPath).Msg("preferences saved successfully")
	ctx.Response(http.StatusOK, prefs)
}
