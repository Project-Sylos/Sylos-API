package migrations

import (
	"net/http"

	"github.com/go-chi/chi/v5"

	"codeberg.org/Sylos/Sylos-API/internal/routes/middleware"
)

func (h handler) deleteMigration(ctx *middleware.Context) {
	migrationID := chi.URLParam(ctx.Request(), "migrationID")
	if migrationID == "" {
		ctx.Error(http.StatusBadRequest, "migration id is required", nil)
		return
	}

	resp, err := h.mgr.DeleteMigration(ctx.Request().Context(), migrationID)
	if err != nil {
		ctx.Error(http.StatusInternalServerError, "failed to delete migration", err)
		return
	}
	ctx.Response(http.StatusOK, resp)
}
