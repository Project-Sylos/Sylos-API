package services

import (
	"errors"
	"net/http"

	"github.com/go-chi/chi/v5"

	"github.com/Project-Sylos/Sylos-API/internal/corebridge"
	"github.com/Project-Sylos/Sylos-API/internal/routes/middleware"
)

func (h handler) listDrives(ctx *middleware.Context) {
	serviceID := chi.URLParam(ctx.Request(), "serviceID")
	if serviceID == "" {
		ctx.Error(http.StatusBadRequest, "service id is required", nil)
		return
	}

	drives, err := h.core.ListDrives(ctx.Request().Context(), serviceID)
	if err != nil {
		if errors.Is(err, corebridge.ErrServiceNotFound) {
			ctx.Error(http.StatusNotFound, "service not found", err)
			return
		}

		ctx.Error(http.StatusInternalServerError, "failed to list drives", err)
		return
	}

	ctx.Response(http.StatusOK, drives)
}
