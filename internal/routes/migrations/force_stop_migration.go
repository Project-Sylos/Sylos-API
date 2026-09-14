package migrations

import (
	"errors"
	"net/http"

	"github.com/go-chi/chi/v5"

	"codeberg.org/Sylos/Sylos-API/internal/corebridge"
	"codeberg.org/Sylos/Sylos-API/internal/routes/middleware"
)

func (h handler) forceStop(ctx *middleware.Context) {
	migrationID := chi.URLParam(ctx.Request(), "migrationID")
	if migrationID == "" {
		ctx.Error(http.StatusBadRequest, "migration id is required", nil)
		return
	}

	status, err := h.mgr.ForceStopMigration(ctx.Request().Context(), migrationID)
	if err != nil {
		if errors.Is(err, corebridge.ErrMigrationNotFound) {
			ctx.Error(http.StatusNotFound, "migration not found", err)
			return
		}
		ctx.Error(http.StatusInternalServerError, "failed to force-stop migration", err)
		return
	}

	response := map[string]any{
		"id":             migrationID,
		"status":         status.Status,
		"phase":          status.Status,
		"success":        status.Success,
		"alreadyStopped": status.AlreadyStopped,
		"live":           status.Live,
		"stopped":        status.Stopped,
		"stopProgress":   status.StopProgress,
		"completedAt":    status.CompletedAt,
		"error":          status.Error,
	}
	if status.AlreadyStopped {
		response["message"] = "Migration is already stopped."
	} else {
		response["message"] = "Migration force-stopped. It may not be resumable."
	}

	ctx.Response(http.StatusOK, response)
}
