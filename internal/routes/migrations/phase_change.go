package migrations

import (
	"context"
	"net/http"

	"github.com/go-chi/chi/v5"

	"github.com/Project-Sylos/Sylos-API/internal/corebridge"
	"github.com/Project-Sylos/Sylos-API/internal/routes/middleware"
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
		ctx.Error(http.StatusBadRequest, "phase is required (must be 'traversal' or 'copy')", nil)
		return
	}

	if payload.Phase != "traversal" && payload.Phase != "copy" {
		ctx.Error(http.StatusBadRequest, "invalid phase (must be 'traversal' or 'copy')", nil)
		return
	}

	// Ensure migration ID is set in request
	if payload.MigrationID == "" {
		payload.MigrationID = migrationID
	}
	if payload.Options.MigrationID == "" {
		payload.Options.MigrationID = migrationID
	}

	// Launch phase change in goroutine and return immediately
	// Use background context since the HTTP request context will be canceled when handler returns
	go func() {
		bgCtx := context.Background()
		migration, err := h.core.ChangePhase(bgCtx, migrationID, payload.Phase, payload.StartMigrationRequest)
		if err != nil {
			// Errors are logged by the core bridge
			h.logger.Error().
				Err(err).
				Str("migration_id", migrationID).
				Str("phase", payload.Phase).
				Msg("failed to change phase in background")
			return
		}

		h.logger.Info().
			Str("migration_id", migration.ID).
			Str("phase", payload.Phase).
			Str("status", migration.Status).
			Msg("phase change started in background")
	}()

	// Return immediately with accepted status
	ctx.Response(http.StatusAccepted, corebridge.Migration{
		ID:     migrationID,
		Status: "starting", // Indicates it's being started asynchronously
	})
}
