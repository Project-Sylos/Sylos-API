package services

import (
	"net/http"

	"github.com/go-chi/chi/v5"

	"codeberg.org/Sylos/Sylos-API/internal/corebridge"
	"codeberg.org/Sylos/Sylos-API/internal/routes/httputil"
	"codeberg.org/Sylos/Sylos-API/internal/routes/middleware"
)

func (h handler) listChildren(ctx *middleware.Context) {
	serviceID := chi.URLParam(ctx.Request(), "serviceID")
	if serviceID == "" {
		ctx.Error(http.StatusBadRequest, "service id is required", nil)
		return
	}

	q := httputil.ParseListChildrenQuery(ctx.Request().URL.Query())

	children, err := h.mgr.ListChildren(ctx.Request().Context(), corebridge.ListChildrenRequest{
		ServiceID:    serviceID,
		Identifier:   q.Identifier,
		Role:         q.Role,
		ConnectionID: q.ConnectionID,
		RootType:     q.RootType,
		DriveID:      q.DriveID,
		Offset:       q.Offset,
		Limit:        q.Limit,
		FoldersOnly:  q.FoldersOnly,
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
