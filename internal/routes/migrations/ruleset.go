package migrations

import (
	"net/http"

	"github.com/go-chi/chi/v5"

	"codeberg.org/Sylos/Sylos-API/internal/corebridge"
	"codeberg.org/Sylos/Sylos-API/internal/routes/middleware"
)

func (h handler) getMigrationRuleset(ctx *middleware.Context) {
	migrationID := chi.URLParam(ctx.Request(), "migrationID")
	resp, err := h.mgr.GetMigrationRuleset(ctx.Request().Context(), migrationID)
	if err != nil {
		ctx.Error(http.StatusInternalServerError, "failed to get migration ruleset", err)
		return
	}
	ctx.Response(http.StatusOK, resp)
}

func (h handler) setMigrationRuleset(ctx *middleware.Context, req corebridge.SetMigrationRulesetRequest) {
	migrationID := chi.URLParam(ctx.Request(), "migrationID")
	if err := h.mgr.SetMigrationRuleset(ctx.Request().Context(), migrationID, req); err != nil {
		ctx.Error(http.StatusBadRequest, "failed to set migration ruleset", err)
		return
	}
	ctx.Response(http.StatusNoContent, nil)
}
