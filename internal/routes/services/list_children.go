package services

import (
	"net/http"
	"strconv"

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

	// Parse pagination parameters
	offset := 0
	if offsetStr := ctx.Request().URL.Query().Get("offset"); offsetStr != "" {
		if parsed, err := strconv.Atoi(offsetStr); err == nil && parsed >= 0 {
			offset = parsed
		}
	}

	limit := 100 // Default limit
	if limitStr := ctx.Request().URL.Query().Get("limit"); limitStr != "" {
		if parsed, err := strconv.Atoi(limitStr); err == nil && parsed > 0 {
			limit = parsed
		}
	}

	foldersOnly := false
	if foldersOnlyStr := ctx.Request().URL.Query().Get("foldersOnly"); foldersOnlyStr != "" {
		if parsed, err := strconv.ParseBool(foldersOnlyStr); err == nil {
			foldersOnly = parsed
		}
	}

	children, err := h.core.ListChildren(ctx.Request().Context(), corebridge.ListChildrenRequest{
		ServiceID:   serviceID,
		Identifier:  identifier,
		Role:        role,
		Offset:      offset,
		Limit:       limit,
		FoldersOnly: foldersOnly,
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
