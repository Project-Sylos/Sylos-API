package migrations

import (
	"errors"
	"net/http"

	"github.com/Project-Sylos/Sylos-API/internal/corebridge"
	"github.com/Project-Sylos/Sylos-API/internal/routes/middleware"
)

func (h handler) listDBs(ctx *middleware.Context) {
	dbs, err := h.core.ListMigrationDBs(ctx.Request().Context())
	if err != nil {
		if errors.Is(err, corebridge.ErrMigrationNotFound) {
			ctx.Error(http.StatusNotFound, "migration DBs not found", err)
			return
		}
		ctx.Error(http.StatusInternalServerError, "failed to list migration DBs", err)
		return
	}

	ctx.Response(http.StatusOK, dbs)
}
