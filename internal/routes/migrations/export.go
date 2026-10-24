package migrations

import (
	"fmt"
	"net/http"

	"github.com/go-chi/chi/v5"

	"codeberg.org/Sylos/Sylos-API/internal/corebridge/migrationfiles"
	"codeberg.org/Sylos/Sylos-API/internal/routes/middleware"
)

func (h handler) exportOps(ctx *middleware.Context) {
	migrationID := chi.URLParam(ctx.Request(), "migrationID")
	if migrationID == "" {
		ctx.Error(http.StatusBadRequest, "migration id is required", nil)
		return
	}

	id := migrationfiles.NormalizeMigrationID(migrationID)
	if !migrationfiles.OpsDirExists(h.mgr.DataDir(), id) {
		ctx.Error(http.StatusNotFound, fmt.Sprintf("ops directory not found for migration %q", id), nil)
		return
	}

	filename := id + ".ops.zip"
	w := ctx.Writer()
	w.Header().Set("Content-Type", "application/zip")
	w.Header().Set("Content-Disposition", fmt.Sprintf(`attachment; filename="%s"`, filename))
	w.WriteHeader(http.StatusOK)
	ctx.MarkWritten(http.StatusOK)

	if _, err := migrationfiles.ExportMigrationOpsZip(h.mgr.DataDir(), id, w); err != nil {
		h.logger.Error().Err(err).Str("migration_id", id).Msg("failed while streaming ops export zip")
	}
}
