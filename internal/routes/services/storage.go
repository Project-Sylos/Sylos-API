package services

import (
	"errors"
	"net/http"

	"github.com/go-chi/chi/v5"

	"codeberg.org/Sylos/Sylos-API/internal/corebridge"
	"codeberg.org/Sylos/Sylos-API/internal/routes/middleware"
)

func (h handler) getStorageInfo(ctx *middleware.Context) {
	serviceID := chi.URLParam(ctx.Request(), "serviceID")
	if serviceID == "" {
		ctx.Error(http.StatusBadRequest, "service id is required", nil)
		return
	}
	q := ctx.Request().URL.Query()
	info, err := h.mgr.GetStorageInfo(ctx.Request().Context(), corebridge.GetStorageInfoRequest{
		ServiceID:    serviceID,
		Path:         q.Get("path"),
		ConnectionID: q.Get("connectionId"),
		RootType:     q.Get("rootType"),
		DriveID:      q.Get("driveId"),
		Role:         q.Get("role"),
	})
	if err != nil {
		if errors.Is(err, corebridge.ErrServiceNotFound) {
			ctx.Error(http.StatusNotFound, "service not found", err)
			return
		}
		ctx.Error(http.StatusInternalServerError, "failed to get storage info", err)
		return
	}
	ctx.Response(http.StatusOK, info)
}
