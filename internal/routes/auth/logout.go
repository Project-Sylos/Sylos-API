package auth

import (
	"net/http"

	appauth "codeberg.org/Sylos/Sylos-API/internal/auth"
	"codeberg.org/Sylos/Sylos-API/internal/routes/middleware"
)

func (h handler) logout(ctx *middleware.Context) {
	claims, ok := appauth.ClaimsFromContext(ctx.Request().Context())
	if ok && claims.Subject != "" {
		if err := h.userStore.RecordLogout(claims.Subject); err != nil {
			h.logger.Warn().Err(err).Str("user_id", claims.Subject).Msg("record logout audit")
		}
	}
	ctx.Response(http.StatusNoContent, nil)
}
