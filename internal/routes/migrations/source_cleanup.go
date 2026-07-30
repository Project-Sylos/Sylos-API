package migrations

import (
	"net/http"

	"github.com/go-chi/chi/v5"

	"codeberg.org/Sylos/Sylos-API/internal/corebridge"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge/migrationops"
	"codeberg.org/Sylos/Sylos-API/internal/routes/middleware"
)

func (h handler) prepareSourceCleanup(ctx *middleware.Context, payload corebridge.PrepareSourceCleanupRequest) {
	migrationID := chi.URLParam(ctx.Request(), "migrationID")
	if migrationID == "" {
		ctx.Error(http.StatusBadRequest, "migration id is required", nil)
		return
	}

	mig, err := h.mgr.GetMigration(ctx.Request().Context(), migrationID)
	if err != nil {
		h.writeMigrationLoadError(ctx, err)
		return
	}

	result, err := migrationops.PrepareSourceCleanup(mig, payload)
	if err != nil {
		ctx.Error(http.StatusBadRequest, err.Error(), err)
		return
	}
	if !result.Success {
		ctx.Error(http.StatusBadRequest, result.Error, nil)
		return
	}
	ctx.Response(http.StatusOK, result)
}

func (h handler) handleSkipNodeDelete(ctx *middleware.Context) {
	migrationID, unescapedNodeID, ok := h.loadMigrationNode(ctx)
	if !ok {
		return
	}

	mig, err := h.mgr.GetMigration(ctx.Request().Context(), migrationID)
	if err != nil {
		h.writeMigrationLoadError(ctx, err)
		return
	}

	result, err := migrationops.SkipNodeDelete(mig, unescapedNodeID)
	if err != nil {
		ctx.Error(http.StatusBadRequest, err.Error(), err)
		return
	}
	if !result.Success {
		ctx.Response(http.StatusOK, result)
		return
	}
	ctx.Response(http.StatusOK, result)
}

func (h handler) handleUnskipNodeDelete(ctx *middleware.Context) {
	migrationID, unescapedNodeID, ok := h.loadMigrationNode(ctx)
	if !ok {
		return
	}

	mig, err := h.mgr.GetMigration(ctx.Request().Context(), migrationID)
	if err != nil {
		h.writeMigrationLoadError(ctx, err)
		return
	}

	result, err := migrationops.UnskipNodeDelete(mig, unescapedNodeID)
	if err != nil {
		ctx.Error(http.StatusBadRequest, err.Error(), err)
		return
	}
	if !result.Success {
		ctx.Response(http.StatusOK, result)
		return
	}
	ctx.Response(http.StatusOK, result)
}
