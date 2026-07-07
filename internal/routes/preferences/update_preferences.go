package preferences

import (
	"encoding/json"
	"net/http"

	appauth "codeberg.org/Sylos/Sylos-API/internal/auth"
	"codeberg.org/Sylos/Sylos-API/internal/routes/middleware"
)

func (h handler) updatePreferences(ctx *middleware.Context, prefs Preferences) {
	claims, ok := appauth.ClaimsFromContext(ctx.Request().Context())
	if !ok {
		ctx.Error(http.StatusUnauthorized, "unauthorized", nil)
		return
	}

	prefs = mergeWithDefaults(prefs)

	data, err := json.Marshal(prefs)
	if err != nil {
		h.logger.Error().Err(err).Msg("failed to serialize preferences")
		ctx.Error(http.StatusInternalServerError, "failed to serialize preferences", err)
		return
	}

	if err := h.userStore.SetPreferencesJSON(claims.Subject, string(data)); err != nil {
		h.logger.Error().Err(err).Str("user_id", claims.Subject).Msg("failed to save user preferences")
		ctx.Error(http.StatusInternalServerError, "failed to save preferences", err)
		return
	}

	h.logger.Info().Str("user_id", claims.Subject).Msg("preferences saved")
	ctx.Response(http.StatusOK, prefs)
}
