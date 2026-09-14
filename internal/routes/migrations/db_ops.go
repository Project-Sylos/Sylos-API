package migrations

import (
	"errors"
	"net/http"
	"strconv"

	"github.com/go-chi/chi/v5"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge/migrationops"
	"codeberg.org/Sylos/Sylos-API/internal/routes/middleware"
)

func (h handler) dbOps(ctx *middleware.Context) {
	migrationID := chi.URLParam(ctx.Request(), "migrationID")
	if migrationID == "" {
		ctx.Error(http.StatusBadRequest, "migration id is required", nil)
		return
	}

	mig, err := h.mgr.GetMigration(ctx.Request().Context(), migrationID)
	if err != nil {
		if errors.Is(err, corebridge.ErrMigrationNotFound) {
			ctx.Error(http.StatusNotFound, "migration not found", err)
			return
		}
		ctx.Error(http.StatusInternalServerError, "failed to get migration", err)
		return
	}

	q := ctx.Request().URL.Query()
	limit := 200
	if s := q.Get("limit"); s != "" {
		if n, err := strconv.Atoi(s); err == nil {
			limit = n
		}
	}
	includeSamples := q.Get("samples") == "true" || q.Get("samples") == "1"

	report, err := migrationops.DBOpsFromMigration(mig, db.DBOpsReportOptions{
		OpFilter:       q.Get("op"),
		SampleLimit:    limit,
		IncludeSamples: includeSamples,
	})
	if err != nil {
		ctx.Error(http.StatusInternalServerError, "failed to read db ops", err)
		return
	}
	ctx.Response(http.StatusOK, report)
}
