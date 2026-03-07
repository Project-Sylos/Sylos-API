package migrations

import (
	"net/http"

	"github.com/go-chi/chi/v5"

	"codeberg.org/Sylos/Sylos-API/internal/routes/middleware"
)

// bgTasks handles GET /api/migrations/{migrationID}/bgTasks
// Returns all background tasks for a migration (running, completed, failed)
func (h handler) bgTasks(ctx *middleware.Context) {
	migrationID := chi.URLParam(ctx.Request(), "migrationID")
	if migrationID == "" {
		ctx.Error(http.StatusBadRequest, "migration id is required", nil)
		return
	}

	tasks, err := h.mgr.GetBackgroundTasks(ctx.Request().Context(), migrationID)
	if err != nil {
		ctx.Error(http.StatusInternalServerError, "failed to get background tasks", err)
		return
	}

	ctx.Response(http.StatusOK, tasks)
}

// bgTasksRunning handles GET /api/migrations/{migrationID}/bgTasks/running
// Returns whether any background tasks are running and lists all running tasks
func (h handler) bgTasksRunning(ctx *middleware.Context) {
	migrationID := chi.URLParam(ctx.Request(), "migrationID")
	if migrationID == "" {
		ctx.Error(http.StatusBadRequest, "migration id is required", nil)
		return
	}

	runningTasks, err := h.mgr.GetRunningBackgroundTasks(ctx.Request().Context(), migrationID)
	if err != nil {
		ctx.Error(http.StatusInternalServerError, "failed to get running background tasks", err)
		return
	}

	response := map[string]any{
		"hasRunningTasks": len(runningTasks) > 0,
		"count":           len(runningTasks),
		"tasks":           runningTasks,
	}

	ctx.Response(http.StatusOK, response)
}

// bgTaskByID handles GET /api/migrations/{migrationID}/bgTasks/{taskID}
// Returns a specific background task by ID
func (h handler) bgTaskByID(ctx *middleware.Context) {
	migrationID := chi.URLParam(ctx.Request(), "migrationID")
	if migrationID == "" {
		ctx.Error(http.StatusBadRequest, "migration id is required", nil)
		return
	}

	taskID := chi.URLParam(ctx.Request(), "taskID")
	if taskID == "" {
		ctx.Error(http.StatusBadRequest, "task id is required", nil)
		return
	}

	task, err := h.mgr.GetBackgroundTask(ctx.Request().Context(), migrationID, taskID)
	if err != nil {
		// Check if it's a "not found" error
		if err.Error() == "task "+taskID+" not found for migration "+migrationID ||
			err.Error() == "no tasks found for migration "+migrationID {
			ctx.Error(http.StatusNotFound, "background task not found", err)
			return
		}
		ctx.Error(http.StatusInternalServerError, "failed to get background task", err)
		return
	}

	ctx.Response(http.StatusOK, task)
}
