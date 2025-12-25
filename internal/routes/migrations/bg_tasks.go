package migrations

import (
	"net/http"

	"github.com/go-chi/chi/v5"

	"github.com/Project-Sylos/Sylos-API/internal/routes/middleware"
)

// bgTasks handles GET /api/migrations/{migrationID}/bgTasks
// Returns all background tasks for a migration (running, completed, failed)
func (h handler) bgTasks(ctx *middleware.Context) {
	migrationID := chi.URLParam(ctx.Request(), "migrationID")
	if migrationID == "" {
		ctx.Error(http.StatusBadRequest, "migration id is required", nil)
		return
	}

	tasks, err := h.core.GetBackgroundTasks(ctx.Request().Context(), migrationID)
	if err != nil {
		ctx.Error(http.StatusInternalServerError, "failed to get background tasks", err)
		return
	}

	ctx.Response(http.StatusOK, tasks)
}
