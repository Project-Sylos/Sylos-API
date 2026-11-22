package migrations

import (
	"errors"
	"net/http"

	"github.com/go-chi/chi/v5"

	"codeberg.org/Sylos/Sylos-API/internal/corebridge"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge/manager"
	"codeberg.org/Sylos/Sylos-API/internal/routes/middleware"
)

// resume handles POST /api/migrations/{migrationID}/resume
// Resumes a stopped or failed migration in the correct phase (traversal retry sweep or copy resume).
func (h handler) resume(ctx *middleware.Context, payload corebridge.SweepConfigRequest) {
	migrationID := chi.URLParam(ctx.Request(), "migrationID")
	if migrationID == "" {
		ctx.Error(http.StatusBadRequest, "migration id is required", nil)
		return
	}

	response, err := h.mgr.ResumeMigration(ctx.Request().Context(), migrationID, payload)
	if err != nil {
		var failed *manager.FSHealthFailedError
		if errors.As(err, &failed) || response.FsHealth != nil {
			ctx.Response(http.StatusOK, response)
			return
		}
		statusCode := http.StatusInternalServerError
		if !response.Success && response.Error != "" {
			statusCode = http.StatusBadRequest
		}
		h.logger.Error().
			Err(err).
			Str("migration_id", migrationID).
			Msg("failed to resume migration")
		ctx.Response(statusCode, response)
		return
	}

	ctx.Response(http.StatusAccepted, response)
}
