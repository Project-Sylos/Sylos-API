package migrations

import (
	"errors"
	"net/http"
	"strings"

	"codeberg.org/Sylos/Sylos-API/internal/corebridge"
	"codeberg.org/Sylos/Sylos-API/internal/routes/middleware"
	"codeberg.org/Sylos/Sylos-FS/pkg/cloud"
)

func (h handler) setRoot(ctx *middleware.Context, payload corebridge.SetRootRequest) {
	resp, err := h.mgr.SetRoot(ctx.Request().Context(), payload)
	if err != nil {
		status := http.StatusInternalServerError
		if errors.Is(err, corebridge.ErrServiceNotFound) ||
			errors.Is(err, corebridge.ErrMigrationNotFound) ||
			errors.Is(err, cloud.ErrForbiddenMigrationRoot) ||
			isUserInputError(err) {
			status = http.StatusBadRequest
		}
		if status >= http.StatusInternalServerError {
			ctx.Error(status, "failed to set migration root", err)
		} else {
			ctx.Error(status, err.Error(), err)
		}
		return
	}

	status := http.StatusOK
	if resp.Ready {
		status = http.StatusCreated
	}
	ctx.Response(status, resp)
}

func isUserInputError(err error) bool {
	msg := err.Error()
	return strings.Contains(msg, "invalid") ||
		strings.Contains(msg, "required") ||
		strings.Contains(msg, "exists") ||
		strings.Contains(msg, "mismatch")
}
