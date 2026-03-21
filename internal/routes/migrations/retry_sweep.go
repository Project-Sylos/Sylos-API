package migrations

import (
	"net/http"

	"github.com/go-chi/chi/v5"

	"codeberg.org/Sylos/Sylos-API/internal/corebridge"
	"codeberg.org/Sylos/Sylos-API/internal/routes/middleware"
)

// retrySweep handles POST /api/migrations/{migrationID}/retry-sweep
// Triggers a retry sweep for a migration with optional configuration
func (h handler) retrySweep(ctx *middleware.Context, payload corebridge.SweepConfigRequest) {
	migrationID := chi.URLParam(ctx.Request(), "migrationID")
	if migrationID == "" {
		ctx.Error(http.StatusBadRequest, "migration id is required", nil)
		return
	}

	// Trigger retry sweep
	response, err := h.mgr.TriggerRetrySweep(ctx.Request().Context(), migrationID, payload)
	if err != nil {
		// Determine appropriate HTTP status code
		statusCode := http.StatusInternalServerError
		if !response.Success {
			// If response has success: false, check error message for client errors
			if response.Error != "" {
				statusCode = http.StatusBadRequest
			}
		}

		h.logger.Error().
			Err(err).
			Str("migration_id", migrationID).
			Msg("failed to trigger retry sweep")

		ctx.Response(statusCode, response)
		return
	}

	// Return success response
	ctx.Response(http.StatusAccepted, response)
}
