package services

import (
	"net/http"

	"github.com/Project-Sylos/Sylos-API/internal/routes/middleware"
)

func (h handler) listServices(ctx *middleware.Context) {
	sources, err := h.core.ListSources(ctx.Request().Context())
	if err != nil {
		ctx.Error(http.StatusInternalServerError, "failed to list services", err)
		return
	}

	ctx.Response(http.StatusOK, sources)
}
