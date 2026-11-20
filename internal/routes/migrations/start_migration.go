package migrations

import (
	"context"
	"errors"
	"net/http"

	"codeberg.org/Sylos/Sylos-API/internal/corebridge"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge/manager"
	"codeberg.org/Sylos/Sylos-API/internal/routes/middleware"
)

func (h handler) start(ctx *middleware.Context, payload corebridge.StartMigrationRequest) {
	if payload.MigrationID == "" && payload.Options.MigrationID == "" {
		ctx.Error(http.StatusBadRequest, "migration id is required", nil)
		return
	}

	migrationID := payload.MigrationID
	if migrationID == "" {
		migrationID = payload.Options.MigrationID
	}

	// Sync FS health gate before accepting start so UI/scripts get a structured failure.
	if health, err := h.mgr.EnsureFSHealthFresh(ctx.Request().Context(), migrationID); err != nil {
		var failed *manager.FSHealthFailedError
		if errors.As(err, &failed) {
			ctx.Response(http.StatusOK, failed.Report)
			return
		}
		if health != nil && !health.OK {
			ctx.Response(http.StatusOK, *health)
			return
		}
		ctx.Error(http.StatusBadRequest, err.Error(), err)
		return
	}

	go func() {
		bgCtx := context.Background()
		migration, err := h.mgr.StartMigration(bgCtx, payload)
		if err != nil {
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

	ctx.Response(http.StatusAccepted, corebridge.Migration{
		ID:     migrationID,
		Status: "starting",
	})
}
