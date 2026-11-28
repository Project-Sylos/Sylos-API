package preferences

import (
	"encoding/json"
	"net/http"

	appauth "codeberg.org/Sylos/Sylos-API/internal/auth"
	"codeberg.org/Sylos/Sylos-API/internal/routes/middleware"
)

func (h handler) getPreferences(ctx *middleware.Context) {
	claims, ok := appauth.ClaimsFromContext(ctx.Request().Context())
	if !ok {
		ctx.Error(http.StatusUnauthorized, "unauthorized", nil)
		return
	}

	raw, err := h.userStore.GetPreferencesJSON(claims.Subject)
	if err != nil {
		h.logger.Error().Err(err).Str("user_id", claims.Subject).Msg("failed to read user preferences")
		ctx.Error(http.StatusInternalServerError, "failed to read preferences", err)
		return
	}

	if raw == "" {
		h.logger.Debug().Str("user_id", claims.Subject).Msg("no saved preferences, returning defaults")
		ctx.Response(http.StatusOK, sanitizePreferences(getDefaultPreferences()))
		return
	}

	var prefs Preferences
	if err := json.Unmarshal([]byte(raw), &prefs); err != nil {
		h.logger.Warn().Err(err).Str("user_id", claims.Subject).Msg("invalid preferences JSON, returning defaults")
		ctx.Response(http.StatusOK, sanitizePreferences(getDefaultPreferences()))
		return
	}

	ctx.Response(http.StatusOK, sanitizePreferences(prefs))
}
