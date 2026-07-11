package migrations

import (
	"context"
	"net/http"
	"strings"

	"github.com/go-chi/chi/v5"

	"codeberg.org/Sylos/Sylos-API/internal/corebridge"
	"codeberg.org/Sylos/Sylos-API/internal/routes/middleware"
)

type PhaseChangeRequest struct {
	Phase string `json:"phase"` // "traversal" or "copy"
	corebridge.StartMigrationRequest
}

// changePhase handles POST /api/migrations/{migrationID}/phase-change
func (h handler) changePhase(ctx *middleware.Context, payload PhaseChangeRequest) {
	migrationID := chi.URLParam(ctx.Request(), "migrationID")
	if migrationID == "" {
		ctx.Error(http.StatusBadRequest, "migration id is required", nil)
		return
	}

	// Validate phase
	if payload.Phase == "" {
		ctx.Error(http.StatusBadRequest, "phase is required (must be 'traversal', 'copy', or 'delete')", nil)
		return
	}

	if payload.Phase != "traversal" && payload.Phase != "copy" && payload.Phase != "delete" {
		ctx.Error(http.StatusBadRequest, "invalid phase (must be 'traversal', 'copy', or 'delete')", nil)
		return
	}

	// Ensure migration ID is set in request
	if payload.MigrationID == "" {
		payload.MigrationID = migrationID
	}
	if payload.Options.MigrationID == "" {
		payload.Options.MigrationID = migrationID
	}

	// Validate synchronously before starting background operation
	// This allows us to return immediate errors to the client
	bgCtx := context.Background()
	migration, err := h.mgr.ChangePhase(bgCtx, migrationID, payload.Phase, payload.StartMigrationRequest)
	if err != nil {
		// Return error response with success: false
		h.logger.Error().
			Err(err).
			Str("migration_id", migrationID).
			Str("phase", payload.Phase).
			Msg("failed to change phase")

		// Determine appropriate HTTP status code
		statusCode := http.StatusInternalServerError
		if strings.Contains(err.Error(), "invalid phase") ||
			strings.Contains(err.Error(), "pending retries") ||
			strings.Contains(err.Error(), "running background tasks") ||
			strings.Contains(err.Error(), "DuckDB file not found") {
			statusCode = http.StatusBadRequest
		}

		ctx.Response(statusCode, corebridge.Migration{
			ID:      migrationID,
			Status:  "error",
			Success: false,
		})
		return
	}

	// Phase change accepted, ETL will run in background
	h.logger.Info().
		Str("migration_id", migration.ID).
		Str("phase", payload.Phase).
		Str("status", migration.Status).
		Msg("phase change started in background")

	// Return immediately with accepted status
	ctx.Response(http.StatusAccepted, migration)
}
