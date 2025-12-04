package migrations

import (
	"errors"
	"net/http"

	"github.com/go-chi/chi/v5"

	"github.com/Project-Sylos/Sylos-API/internal/corebridge"
	"github.com/Project-Sylos/Sylos-API/internal/routes/middleware"
)

func (h handler) getLogs(ctx *middleware.Context, req corebridge.GetLogsRequest) {
	migrationID := chi.URLParam(ctx.Request(), "migrationID")
	if migrationID == "" {
		ctx.Error(http.StatusBadRequest, "migration id is required", nil)
		return
	}

	logs, err := h.core.GetLogs(ctx.Request().Context(), migrationID, req)
	if err != nil {
		if errors.Is(err, corebridge.ErrMigrationNotFound) {
			ctx.Error(http.StatusNotFound, "migration not found", err)
			return
		}
		ctx.Error(http.StatusInternalServerError, "failed to get logs", err)
		return
	}

	// Return 200 OK even if success=false (non-critical error, UI can handle it)
	// The response includes success, errorCode, and error fields for UI to check
	ctx.Response(http.StatusOK, logs)
}
