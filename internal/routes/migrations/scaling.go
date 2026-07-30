package migrations

import (
	"net/http"

	"github.com/go-chi/chi/v5"

	"codeberg.org/Sylos/Sylos-API/internal/corebridge"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge/manager"
	"codeberg.org/Sylos/Sylos-API/internal/routes/middleware"
)

func (h handler) getScaling(ctx *middleware.Context) {
	migrationID := chi.URLParam(ctx.Request(), "migrationID")
	view, err := h.mgr.GetMigrationScaling(ctx.Request().Context(), migrationID)
	if err == corebridge.ErrMigrationNotFound {
		ctx.Error(http.StatusNotFound, "migration not found", err)
		return
	}
	if err != nil {
		ctx.Error(http.StatusInternalServerError, "failed to get migration scaling", err)
		return
	}
	ctx.Response(http.StatusOK, view)
}

func (h handler) putScaling(ctx *middleware.Context, req manager.SetMigrationScalingRequest) {
	migrationID := chi.URLParam(ctx.Request(), "migrationID")
	view, err := h.mgr.SetMigrationScaling(ctx.Request().Context(), migrationID, req)
	if err == corebridge.ErrMigrationNotFound {
		ctx.Error(http.StatusNotFound, "migration not found", err)
		return
	}
	if err != nil {
		ctx.Error(http.StatusBadRequest, "failed to set migration scaling", err)
		return
	}
	ctx.Response(http.StatusOK, view)
}
