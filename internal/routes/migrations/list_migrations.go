package migrations

import (
	"net/http"
	"strconv"

	"codeberg.org/Sylos/Sylos-API/internal/corebridge"
	"codeberg.org/Sylos/Sylos-API/internal/routes/middleware"
)

func (h handler) list(ctx *middleware.Context) {
	// Parse pagination query parameters
	req := corebridge.ListMigrationsRequest{
		Offset: 0,
		Limit:  100, // Default limit
	}

	if offsetStr := ctx.Request().URL.Query().Get("offset"); offsetStr != "" {
		if offset, err := strconv.Atoi(offsetStr); err == nil && offset >= 0 {
			req.Offset = offset
		}
	}

	if limitStr := ctx.Request().URL.Query().Get("limit"); limitStr != "" {
		if limit, err := strconv.Atoi(limitStr); err == nil && limit > 0 {
			req.Limit = limit
		}
	}

	response, err := h.mgr.ListAllMigrations(ctx.Request().Context(), req)
	if err != nil {
		ctx.Error(http.StatusInternalServerError, "failed to list migrations", err)
		return
	}

	ctx.Response(http.StatusOK, response)
}
