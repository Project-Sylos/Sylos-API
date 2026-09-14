package services

import (
	"errors"
	"net/http"

	"github.com/go-chi/chi/v5"

	"codeberg.org/Sylos/Sylos-API/internal/corebridge"
	"codeberg.org/Sylos/Sylos-API/internal/routes/middleware"
)

func (h handler) getHomeFolder(ctx *middleware.Context) {
	serviceID := chi.URLParam(ctx.Request(), "serviceID")
	if serviceID == "" {
		ctx.Error(http.StatusBadRequest, "service id is required", nil)
		return
	}

	folder, err := h.mgr.GetHomeFolder(ctx.Request().Context(), serviceID)
	if err != nil {
		if errors.Is(err, corebridge.ErrServiceNotFound) {
			ctx.Error(http.StatusNotFound, "service not found", err)
			return
		}
		if errors.Is(err, corebridge.ErrHomeUnsupported) {
			ctx.Error(http.StatusBadRequest, "home directory is only available for local filesystem services", err)
			return
		}
		ctx.Error(http.StatusInternalServerError, "failed to resolve home directory", err)
		return
	}

	ctx.Response(http.StatusOK, folder)
}
