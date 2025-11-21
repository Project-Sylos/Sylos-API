package migrations

import (
	"errors"
	"net/http"

	"github.com/go-chi/chi/v5"

	"github.com/Project-Sylos/Sylos-API/internal/corebridge"
	"github.com/Project-Sylos/Sylos-API/internal/routes/middleware"
)

func (h handler) load(ctx *middleware.Context) {
	migrationID := chi.URLParam(ctx.Request(), "migrationID")
	if migrationID == "" {
		ctx.Error(http.StatusBadRequest, "migration id is required", nil)
		return
	}

	migration, err := h.core.LoadMigration(ctx.Request().Context(), migrationID)
	if err != nil {
		if errors.Is(err, corebridge.ErrMigrationNotFound) {
			ctx.Error(http.StatusNotFound, "migration not found", err)
			return
		}

		ctx.Error(http.StatusInternalServerError, "failed to load migration", err)
		return
	}

	ctx.Response(http.StatusAccepted, migration)
}
