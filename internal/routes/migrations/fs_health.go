package migrations

import (
	"net/http"

	"github.com/go-chi/chi/v5"

	"codeberg.org/Sylos/Sylos-API/internal/routes/middleware"
)

// fsHealth handles POST /api/migrations/{migrationID}/fs-health?side=src|dst|both
func (h handler) fsHealth(ctx *middleware.Context) {
	migrationID := chi.URLParam(ctx.Request(), "migrationID")
	if migrationID == "" {
		ctx.Error(http.StatusBadRequest, "migration id is required", nil)
		return
	}
	side := ctx.Request().URL.Query().Get("side")
	report, err := h.mgr.ProbeMigrationFSHealth(ctx.Request().Context(), migrationID, side)
	if err != nil {
		h.logger.Error().
			Err(err).
			Str("migration_id", migrationID).
			Msg("fs health probe failed")
		ctx.Error(http.StatusInternalServerError, err.Error(), err)
		return
	}
	ctx.Response(http.StatusOK, report)
}
