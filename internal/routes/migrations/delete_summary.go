package migrations

import (
	"net/http"

	"github.com/go-chi/chi/v5"

	"codeberg.org/Sylos/Sylos-API/internal/routes/middleware"
)

// deleteSummary handles GET /api/migrations/{migrationID}/delete-summary
func (h handler) deleteSummary(ctx *middleware.Context) {
	migrationID := chi.URLParam(ctx.Request(), "migrationID")
	if migrationID == "" {
		ctx.Error(http.StatusBadRequest, "migration id is required", nil)
		return
	}
	summary, err := h.mgr.GetDeleteSummary(ctx.Request().Context(), migrationID)
	if err != nil {
		ctx.Error(http.StatusBadRequest, err.Error(), err)
		return
	}
	ctx.Response(http.StatusOK, summary)
}
