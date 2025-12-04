package migrations

import (
	"net/http"

	"github.com/Project-Sylos/Sylos-API/internal/corebridge"
	"github.com/Project-Sylos/Sylos-API/internal/routes/middleware"
)

func (h handler) start(ctx *middleware.Context, payload corebridge.StartMigrationRequest) {
	// Do minimal validation before launching goroutine
	if payload.MigrationID == "" && payload.Options.MigrationID == "" {
		ctx.Error(http.StatusBadRequest, "migration id is required", nil)
		return
	}

	migrationID := payload.MigrationID
	if migrationID == "" {
		migrationID = payload.Options.MigrationID
	}

	// Launch migration in goroutine and return immediately
	// This ensures the HTTP handler returns quickly and doesn't block other requests
	go func() {
		migration, err := h.core.StartMigration(ctx.Request().Context(), payload)
		if err != nil {
			// Errors are logged by the core bridge
			// The migration status will reflect the error when queried
			h.logger.Error().
				Err(err).
				Str("migration_id", migrationID).
				Msg("failed to start migration in background")
			return
		}

		h.logger.Info().
			Str("migration_id", migration.ID).
			Str("status", migration.Status).
			Msg("migration started in background")
	}()

	// Return immediately with accepted status
	// The actual migration work happens in the goroutine above
	ctx.Response(http.StatusAccepted, corebridge.Migration{
		ID:     migrationID,
		Status: "starting", // Indicates it's being started asynchronously
	})
}
