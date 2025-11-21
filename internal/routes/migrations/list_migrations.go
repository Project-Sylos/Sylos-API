package migrations

import (
	"net/http"

	"github.com/Project-Sylos/Sylos-API/internal/routes/middleware"
)

func (h handler) list(ctx *middleware.Context) {
	migrations, err := h.core.ListAllMigrations(ctx.Request().Context())
	if err != nil {
		ctx.Error(http.StatusInternalServerError, "failed to list migrations", err)
		return
	}

	ctx.Response(http.StatusOK, migrations)
}
