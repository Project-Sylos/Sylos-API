package migrations

import (
	appauth "codeberg.org/Sylos/Sylos-API/internal/auth"
	"codeberg.org/Sylos/Migration-Engine/pkg/migration"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge/migrationops"
	preferencesroutes "codeberg.org/Sylos/Sylos-API/internal/routes/preferences"
	"codeberg.org/Sylos/Sylos-API/internal/routes/middleware"
)

func (h handler) applyReviewQueryPrefs(ctx *middleware.Context, mig *migration.Migration) {
	if mig == nil || h.userStore == nil {
		return
	}
	claims, ok := appauth.ClaimsFromContext(ctx.Request().Context())
	if !ok {
		return
	}
	raw, err := h.userStore.GetPreferencesJSON(claims.Subject)
	if err != nil {
		return
	}
	migrationops.ApplyDisableQueryTimeout(mig, preferencesroutes.DisableDBQueryTimeoutFromJSON(raw))
}
