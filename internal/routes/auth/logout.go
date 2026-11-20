package auth

import (
	"net/http"

	appauth "codeberg.org/Sylos/Sylos-API/internal/auth"
	"codeberg.org/Sylos/Sylos-API/internal/routes/middleware"
)

// logout ends the browser session only (audit + 204).
//
// Hard guarantee: this handler must never stop migrations, release FS adapters,
// or shut down the migration engine. Live work continues in-process until the
// Sylos binary exits or a user explicitly stops a migration. JWT clear is
// entirely client-side; RecordLogout is an audit timestamp, not session
// revocation for in-flight engine work.
func (h handler) logout(ctx *middleware.Context) {
	claims, ok := appauth.ClaimsFromContext(ctx.Request().Context())
	if ok && claims.Subject != "" {
		if err := h.userStore.RecordLogout(claims.Subject); err != nil {
			h.logger.Warn().Err(err).Str("user_id", claims.Subject).Msg("record logout audit")
		}
	}
	ctx.Response(http.StatusNoContent, nil)
}
