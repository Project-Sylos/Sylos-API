package migrations

import (
	"errors"
	"net/http"

	"github.com/go-chi/chi/v5"

	"codeberg.org/Sylos/Sylos-API/internal/corebridge"
	"codeberg.org/Sylos/Sylos-API/internal/routes/middleware"
)

func (h handler) stop(ctx *middleware.Context) {
	migrationID := chi.URLParam(ctx.Request(), "migrationID")
	if migrationID == "" {
		ctx.Error(http.StatusBadRequest, "migration id is required", nil)
		return
	}

	status, err := h.mgr.StopMigration(ctx.Request().Context(), migrationID)
	if err != nil {
		if errors.Is(err, corebridge.ErrMigrationNotFound) {
			ctx.Error(http.StatusNotFound, "migration not found", err)
			return
		}
		ctx.Error(http.StatusInternalServerError, "failed to stop migration", err)
		return
	}

	response := map[string]any{
		"id":                   migrationID,
		"status":               status.Status,
		"success":              status.Success,
		"alreadyStopped":       status.AlreadyStopped,
		"result":                 status.Result,
		"live":                   status.Live,
		"stopped":                status.Stopped,
		"softSuspendRequested":   status.SoftSuspendRequested,
		"completedAt":            status.CompletedAt,
		"error":                  status.Error,
	}
	if status.AlreadyStopped {
		response["message"] = "Migration is already stopped."
	} else {
		response["message"] = "Stop requested. If softSuspendRequested is true, poll status until phase is traversal-suspended or copy-suspended and live is false."
	}

	ctx.Response(http.StatusOK, response)
}
