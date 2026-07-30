package migrations

import (
	"errors"
	"net/http"
	"strings"

	"github.com/go-chi/chi/v5"

	"codeberg.org/Sylos/Sylos-API/internal/corebridge"
	"codeberg.org/Sylos/Sylos-API/internal/routes/middleware"
)

type pathCheckTargetRequest struct {
	PathCheckTarget string `json:"pathCheckTarget"`
	WindowsCompat   *bool  `json:"windowsCompat,omitempty"`
}

type pathCheckTargetResponse struct {
	PathCheckTarget   string `json:"pathCheckTarget"`
	PathChecksEnabled bool   `json:"pathChecksEnabled"`
	WindowsCompat     bool   `json:"windowsCompat"`
}

func (h handler) getPathCheckTarget(ctx *middleware.Context) {
	migrationID := chi.URLParam(ctx.Request(), "migrationID")
	if migrationID == "" {
		ctx.Error(http.StatusBadRequest, "migration id is required", nil)
		return
	}
	view, err := h.mgr.PathCheckTargetView(ctx.Request().Context(), migrationID)
	if err != nil {
		if errors.Is(err, corebridge.ErrMigrationNotFound) {
			ctx.Error(http.StatusNotFound, "migration not found", err)
			return
		}
		ctx.Error(http.StatusInternalServerError, "failed to get path check target", err)
		return
	}
	ctx.Response(http.StatusOK, pathCheckTargetResponse{
		PathCheckTarget:   view.PathCheckTarget,
		PathChecksEnabled: view.PathChecksEnabled,
		WindowsCompat:     view.WindowsCompat,
	})
}

func (h handler) setPathCheckTarget(ctx *middleware.Context, payload pathCheckTargetRequest) {
	migrationID := chi.URLParam(ctx.Request(), "migrationID")
	if migrationID == "" {
		ctx.Error(http.StatusBadRequest, "migration id is required", nil)
		return
	}
	target := strings.TrimSpace(payload.PathCheckTarget)
	view, err := h.mgr.UpdatePathCheckTarget(ctx.Request().Context(), migrationID, target, payload.WindowsCompat)
	if err != nil {
		if errors.Is(err, corebridge.ErrMigrationNotFound) {
			ctx.Error(http.StatusNotFound, "migration not found", err)
			return
		}
		ctx.Error(http.StatusInternalServerError, "failed to set path check target", err)
		return
	}
	ctx.Response(http.StatusOK, pathCheckTargetResponse{
		PathCheckTarget:   view.PathCheckTarget,
		PathChecksEnabled: view.PathChecksEnabled,
		WindowsCompat:     view.WindowsCompat,
	})
}
