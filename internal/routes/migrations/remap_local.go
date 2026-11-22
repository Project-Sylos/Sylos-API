package migrations

import (
	"errors"
	"net/http"

	"github.com/go-chi/chi/v5"

	"codeberg.org/Sylos/Sylos-API/internal/corebridge/manager"
	"codeberg.org/Sylos/Sylos-API/internal/routes/middleware"
)

func (h handler) remapLocal(ctx *middleware.Context, payload manager.RemapLocalRootRequest) {
	migrationID := chi.URLParam(ctx.Request(), "migrationID")
	if migrationID == "" {
		ctx.Error(http.StatusBadRequest, "migration id is required", nil)
		return
	}
	resp, err := h.mgr.RemapLocalRoot(ctx.Request().Context(), migrationID, payload)
	if err != nil {
		status := http.StatusBadRequest
		if errors.Is(err, manager.ErrMigrationNeedsAttention) {
			status = http.StatusConflict
		}
		ctx.Error(status, err.Error(), err)
		return
	}
	ctx.Response(http.StatusOK, resp)
}
