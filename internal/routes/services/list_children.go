package services

import (
	"net/http"

	"github.com/go-chi/chi/v5"

	"github.com/Project-Sylos/Sylos-API/internal/corebridge"
	"github.com/Project-Sylos/Sylos-API/internal/routes/middleware"
)

func (h handler) listChildren(ctx *middleware.Context) {
	serviceID := chi.URLParam(ctx.Request(), "serviceID")
	if serviceID == "" {
		ctx.Error(http.StatusBadRequest, "service id is required", nil)
		return
	}

	identifier := ctx.Request().URL.Query().Get("identifier")
	role := ctx.Request().URL.Query().Get("role")
	children, err := h.core.ListChildren(ctx.Request().Context(), corebridge.ListChildrenRequest{
		ServiceID:  serviceID,
		Identifier: identifier,
		Role:       role,
	})
	if err != nil {
		if err == corebridge.ErrServiceNotFound {
			ctx.Error(http.StatusNotFound, "service not found", err)
			return
		}

		ctx.Error(http.StatusInternalServerError, "failed to list children", err)
		return
	}

	ctx.Response(http.StatusOK, children)
}
