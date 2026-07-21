package migrations

import (
	"net/http"
	"strings"

	"github.com/go-chi/chi/v5"

	"codeberg.org/Sylos/Sylos-API/internal/routes/middleware"
)

type pathCheckTargetRequest struct {
	PathCheckTarget string `json:"pathCheckTarget"`
}

type pathCheckTargetResponse struct {
	PathCheckTarget   string `json:"pathCheckTarget"`
	PathChecksEnabled bool   `json:"pathChecksEnabled"`
}

func (h handler) getPathCheckTarget(ctx *middleware.Context) {
	migrationID := chi.URLParam(ctx.Request(), "migrationID")
	if migrationID == "" {
		ctx.Error(http.StatusBadRequest, "migration id is required", nil)
		return
	}
	mig, ok := h.migrationForPathIssues(ctx, migrationID)
	if !ok {
		return
	}
	profile := h.mgr.PathCheckTarget(migrationID)
	if profile == "" {
		profile = mig.PathCheckProfile()
	}
	ctx.Response(http.StatusOK, pathCheckTargetResponse{
		PathCheckTarget:   profile,
		PathChecksEnabled: mig.PathChecksEnabled(),
	})
}

func (h handler) setPathCheckTarget(ctx *middleware.Context, payload pathCheckTargetRequest) {
	migrationID := chi.URLParam(ctx.Request(), "migrationID")
	if migrationID == "" {
		ctx.Error(http.StatusBadRequest, "migration id is required", nil)
		return
	}
	target := strings.TrimSpace(payload.PathCheckTarget)
	h.mgr.SetPathCheckTarget(migrationID, target)
	mig, ok := h.migrationForPathIssues(ctx, migrationID)
	if !ok {
		return
	}
	ctx.Response(http.StatusOK, pathCheckTargetResponse{
		PathCheckTarget:   mig.PathCheckProfile(),
		PathChecksEnabled: mig.PathChecksEnabled(),
	})
}
