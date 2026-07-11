package migrations

import (
	"errors"
	"net/http"
	"strings"

	"github.com/go-chi/chi/v5"

	"codeberg.org/Sylos/Sylos-API/internal/corebridge"
	"codeberg.org/Sylos/Sylos-API/internal/routes/middleware"
)

func (h handler) rename(ctx *middleware.Context, payload corebridge.RenameMigrationRequest) {
	migrationID := chi.URLParam(ctx.Request(), "migrationID")
	if migrationID == "" {
		ctx.Error(http.StatusBadRequest, "migration id is required", nil)
		return
	}
	if strings.TrimSpace(payload.Name) == "" {
		ctx.Error(http.StatusBadRequest, "name is required", nil)
		return
	}

	if err := h.mgr.RenameMigration(ctx.Request().Context(), migrationID, payload.Name); err != nil {
		if errors.Is(err, corebridge.ErrMigrationNotFound) {
			ctx.Error(http.StatusNotFound, "migration not found", err)
			return
		}
		ctx.Error(http.StatusInternalServerError, "failed to rename migration", err)
		return
	}

	status, err := h.mgr.GetMigrationStatus(ctx.Request().Context(), migrationID)
	if err != nil {
		ctx.Error(http.StatusInternalServerError, "failed to get migration status", err)
		return
	}
	ctx.Response(http.StatusOK, status)
}
