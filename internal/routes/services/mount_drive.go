package services

import (
	"errors"
	"net/http"
	"strings"

	"github.com/go-chi/chi/v5"

	"codeberg.org/Sylos/Sylos-API/internal/corebridge"
	"codeberg.org/Sylos/Sylos-API/internal/routes/middleware"
)

func (h handler) mountDrive(ctx *middleware.Context, payload corebridge.MountDriveRequest) {
	serviceID := chi.URLParam(ctx.Request(), "serviceID")
	if serviceID == "" {
		ctx.Error(http.StatusBadRequest, "service id is required", nil)
		return
	}

	device := strings.TrimSpace(payload.Device)
	if device == "" {
		ctx.Error(http.StatusBadRequest, "device is required", nil)
		return
	}

	drive, err := h.mgr.MountDrive(ctx.Request().Context(), serviceID, corebridge.MountDriveRequest{
		Device: device,
	})
	if err != nil {
		if errors.Is(err, corebridge.ErrServiceNotFound) {
			ctx.Error(http.StatusNotFound, "service not found", err)
			return
		}

		ctx.Error(http.StatusBadRequest, "failed to mount drive", err)
		return
	}

	ctx.Response(http.StatusOK, drive)
}
