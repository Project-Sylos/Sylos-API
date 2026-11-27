package migrations

import (
	"errors"
	"net/http"

	"github.com/go-chi/chi/v5"

	"codeberg.org/Sylos/Sylos-API/internal/corebridge"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge/migrationops"
	"codeberg.org/Sylos/Sylos-API/internal/routes/middleware"
)

func (h handler) cancelInFlight(ctx *middleware.Context) {
	migrationID := chi.URLParam(ctx.Request(), "migrationID")
	nodeID := chi.URLParam(ctx.Request(), "nodeID")
	if migrationID == "" {
		ctx.Error(http.StatusBadRequest, "migration id is required", nil)
		return
	}
	if nodeID == "" {
		ctx.Error(http.StatusBadRequest, "node id is required", nil)
		return
	}

	mig, err := h.mgr.GetMigration(ctx.Request().Context(), migrationID)
	if err != nil {
		if errors.Is(err, corebridge.ErrMigrationNotFound) {
			ctx.Error(http.StatusNotFound, "migration not found", err)
			return
		}
		ctx.Error(http.StatusInternalServerError, "failed to get migration", err)
		return
	}
	ctx.Response(http.StatusOK, migrationops.CancelInFlightCopyFromMigration(mig, nodeID))
}
