package migrations

import (
	"errors"
	"net/http"
	"strings"

	"github.com/Project-Sylos/Sylos-API/internal/corebridge"
	"github.com/Project-Sylos/Sylos-API/internal/routes/middleware"
)

func (h handler) start(ctx *middleware.Context, payload corebridge.StartMigrationRequest) {
	migration, err := h.core.StartMigration(ctx.Request().Context(), payload)
	if err != nil {
		if errors.Is(err, corebridge.ErrServiceNotFound) ||
			errors.Is(err, corebridge.ErrMigrationNotFound) ||
			isStartInputError(err) {
			ctx.Error(http.StatusBadRequest, err.Error(), err)
			return
		}

		ctx.Error(http.StatusInternalServerError, "failed to start migration", err)
		return
	}

	ctx.Response(http.StatusAccepted, migration)
}

func isStartInputError(err error) bool {
	msg := err.Error()
	return strings.Contains(msg, "root") ||
		strings.Contains(msg, "migration id") ||
		strings.Contains(msg, "configured")
}
