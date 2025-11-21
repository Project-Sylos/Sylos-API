package migrations

import (
	"errors"
	"net/http"

	"github.com/go-chi/chi/v5"

	"github.com/Project-Sylos/Sylos-API/internal/corebridge"
	"github.com/Project-Sylos/Sylos-API/internal/routes/middleware"
)

func (h handler) stop(ctx *middleware.Context) {
	migrationID := chi.URLParam(ctx.Request(), "migrationID")
	if migrationID == "" {
		ctx.Error(http.StatusBadRequest, "migration id is required", nil)
		return
	}

	status, err := h.core.StopMigration(ctx.Request().Context(), migrationID)
	if err != nil {
		if errors.Is(err, corebridge.ErrMigrationNotFound) {
			ctx.Error(http.StatusNotFound, "migration not found", err)
			return
		}
		ctx.Error(http.StatusInternalServerError, "failed to stop migration", err)
		return
	}

	// Return status with a message indicating the migration was suspended
	response := map[string]interface{}{
		"id":      migrationID,
		"status":  status.Status,
		"result":  status.Result,
		"message": "Migration suspended. State saved for resumption.",
	}

	ctx.Response(http.StatusOK, response)
}
