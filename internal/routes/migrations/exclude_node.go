package migrations

import (
	"errors"
	"net/http"

	"github.com/go-chi/chi/v5"

	"codeberg.org/Sylos/Sylos-API/internal/corebridge"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge/migrationops"
	"codeberg.org/Sylos/Sylos-API/internal/routes/middleware"
)

// excludeNodes handles JSON body request
func (h handler) excludeNodes(ctx *middleware.Context, payload corebridge.ExclusionRequest) {
	migrationID := chi.URLParam(ctx.Request(), "migrationID")
	if migrationID == "" {
		ctx.Error(http.StatusBadRequest, "migration id is required", nil)
		return
	}
	h.logger.Info().Str("migration_id", migrationID).Interface("request", payload).Msg("excluding nodes")

	mig, err := h.mgr.GetMigration(ctx.Request().Context(), migrationID)
	if err != nil {
		if errors.Is(err, corebridge.ErrMigrationNotFound) {
			ctx.Error(http.StatusNotFound, "migration not found", err)
			return
		}
		ctx.Error(http.StatusInternalServerError, "failed to get migration", err)
		return
	}
	result, err := migrationops.SetNodesExcluded(mig, payload, true)
	if err != nil {
		ctx.Error(http.StatusInternalServerError, "failed to exclude nodes", err)
		return
	}
	if !result.Success {
		ctx.Error(http.StatusBadRequest, result.Error, nil)
		return
	}
	_ = h.mgr.MarkPathReviewChanges(ctx.Request().Context(), migrationID, true)
	ctx.Response(http.StatusOK, result)
}

// unexcludeNodes handles JSON body request
func (h handler) unexcludeNodes(ctx *middleware.Context, payload corebridge.ExclusionRequest) {
	migrationID := chi.URLParam(ctx.Request(), "migrationID")
	if migrationID == "" {
		ctx.Error(http.StatusBadRequest, "migration id is required", nil)
		return
	}
	h.logger.Info().Str("migration_id", migrationID).Interface("request", payload).Msg("unexcluding nodes")

	mig, err := h.mgr.GetMigration(ctx.Request().Context(), migrationID)
	if err != nil {
		if errors.Is(err, corebridge.ErrMigrationNotFound) {
			ctx.Error(http.StatusNotFound, "migration not found", err)
			return
		}
		ctx.Error(http.StatusInternalServerError, "failed to get migration", err)
		return
	}
	result, err := migrationops.SetNodesExcluded(mig, payload, false)
	if err != nil {
		ctx.Error(http.StatusInternalServerError, "failed to unexclude nodes", err)
		return
	}
	if !result.Success {
		ctx.Error(http.StatusBadRequest, result.Error, nil)
		return
	}
	_ = h.mgr.MarkPathReviewChanges(ctx.Request().Context(), migrationID, true)
	ctx.Response(http.StatusOK, result)
}
