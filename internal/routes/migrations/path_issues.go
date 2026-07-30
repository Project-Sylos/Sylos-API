package migrations

import (
	"errors"
	"net/http"
	"strings"

	"github.com/go-chi/chi/v5"

	"codeberg.org/Sylos/Sylos-API/internal/corebridge"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge/migrationops"
	"codeberg.org/Sylos/Sylos-API/internal/routes/middleware"
)

func (h handler) listPathIssues(ctx *middleware.Context) {
	migrationID := chi.URLParam(ctx.Request(), "migrationID")
	if migrationID == "" {
		ctx.Error(http.StatusBadRequest, "migration id is required", nil)
		return
	}
	mig, err := h.mgr.PathMigration(ctx.Request().Context(), migrationID)
	if err != nil {
		if errors.Is(err, corebridge.ErrMigrationNotFound) {
			ctx.Error(http.StatusNotFound, "migration not found", err)
			return
		}
		ctx.Error(http.StatusInternalServerError, "failed to list destination name issues", err)
		return
	}
	resp, err := migrationops.ListPathIssues(mig, 0)
	if err != nil {
		ctx.Error(http.StatusInternalServerError, "failed to list destination name issues", err)
		return
	}
	ctx.Response(http.StatusOK, resp)
}

func (h handler) validatePathIssue(ctx *middleware.Context, payload corebridge.ValidatePathProposalRequest) {
	migrationID := chi.URLParam(ctx.Request(), "migrationID")
	if migrationID == "" {
		ctx.Error(http.StatusBadRequest, "migration id is required", nil)
		return
	}
	nodeID := strings.TrimSpace(payload.NodeID)
	if nodeID == "" {
		ctx.Error(http.StatusBadRequest, "nodeId is required", nil)
		return
	}
	mig, err := h.mgr.PathMigration(ctx.Request().Context(), migrationID)
	if err != nil {
		if errors.Is(err, corebridge.ErrMigrationNotFound) {
			ctx.Error(http.StatusNotFound, "migration not found", err)
			return
		}
		ctx.Error(http.StatusInternalServerError, "failed to check destination name", err)
		return
	}
	resp, err := migrationops.ValidatePathProposal(mig, nodeID, payload.ProposedPath)
	if err != nil {
		ctx.Error(http.StatusInternalServerError, "failed to check destination name", err)
		return
	}
	ctx.Response(http.StatusOK, resp)
}

func (h handler) acceptPathIssue(ctx *middleware.Context, payload corebridge.AcceptPathRequest) {
	migrationID := chi.URLParam(ctx.Request(), "migrationID")
	nodeID := chi.URLParam(ctx.Request(), "nodeID")
	if migrationID == "" || nodeID == "" {
		ctx.Error(http.StatusBadRequest, "migration id and node id are required", nil)
		return
	}
	mig, err := h.mgr.PathMigration(ctx.Request().Context(), migrationID)
	if err != nil {
		if errors.Is(err, corebridge.ErrMigrationNotFound) {
			ctx.Error(http.StatusNotFound, "migration not found", err)
			return
		}
		ctx.Error(http.StatusInternalServerError, "failed to apply suggested destination name", err)
		return
	}
	resp, err := migrationops.AcceptPathChange(mig, nodeID, payload.ProposedPath, false)
	if err != nil {
		ctx.Error(http.StatusInternalServerError, "failed to apply suggested destination name", err)
		return
	}
	if !resp.Success {
		ctx.Response(http.StatusBadRequest, map[string]any{
			"success":   false,
			"errorCode": corebridge.ErrCodePathValidation,
			"error":     resp.Message,
			"message":   resp.Message,
		})
		return
	}
	ctx.Response(http.StatusOK, resp)
}

func (h handler) remapPathIssue(ctx *middleware.Context, payload corebridge.RemapPathRequest) {
	migrationID := chi.URLParam(ctx.Request(), "migrationID")
	nodeID := chi.URLParam(ctx.Request(), "nodeID")
	if migrationID == "" || nodeID == "" {
		ctx.Error(http.StatusBadRequest, "migration id and node id are required", nil)
		return
	}
	mig, err := h.mgr.PathMigration(ctx.Request().Context(), migrationID)
	if err != nil {
		if errors.Is(err, corebridge.ErrMigrationNotFound) {
			ctx.Error(http.StatusNotFound, "migration not found", err)
			return
		}
		ctx.Error(http.StatusInternalServerError, "failed to rename destination", err)
		return
	}
	resp, err := migrationops.AcceptPathChange(mig, nodeID, payload.ProposedPath, payload.ForceSkipValidation)
	if err != nil {
		ctx.Error(http.StatusInternalServerError, "failed to rename destination", err)
		return
	}
	if !resp.Success {
		ctx.Response(http.StatusBadRequest, map[string]any{
			"success":   false,
			"errorCode": corebridge.ErrCodePathValidation,
			"error":     resp.Message,
			"message":   resp.Message,
		})
		return
	}
	ctx.Response(http.StatusOK, resp)
}

func (h handler) acceptAllPathIssues(ctx *middleware.Context) {
	migrationID := chi.URLParam(ctx.Request(), "migrationID")
	if migrationID == "" {
		ctx.Error(http.StatusBadRequest, "migration id is required", nil)
		return
	}
	mig, err := h.mgr.PathMigration(ctx.Request().Context(), migrationID)
	if err != nil {
		if errors.Is(err, corebridge.ErrMigrationNotFound) {
			ctx.Error(http.StatusNotFound, "migration not found", err)
			return
		}
		ctx.Error(http.StatusInternalServerError, "failed to apply suggested destination names", err)
		return
	}
	resp, err := migrationops.AcceptAllPathProposals(mig)
	if err != nil {
		ctx.Error(http.StatusInternalServerError, "failed to apply suggested destination names", err)
		return
	}
	if !resp.Success {
		ctx.Response(http.StatusBadRequest, map[string]any{
			"success":   false,
			"errorCode": corebridge.ErrCodePathValidation,
			"error":     resp.Message,
			"message":   resp.Message,
		})
		return
	}
	ctx.Response(http.StatusOK, resp)
}

func (h handler) ignoreRemainingPathIssues(ctx *middleware.Context) {
	migrationID := chi.URLParam(ctx.Request(), "migrationID")
	if migrationID == "" {
		ctx.Error(http.StatusBadRequest, "migration id is required", nil)
		return
	}
	mig, err := h.mgr.PathMigration(ctx.Request().Context(), migrationID)
	if err != nil {
		if errors.Is(err, corebridge.ErrMigrationNotFound) {
			ctx.Error(http.StatusNotFound, "migration not found", err)
			return
		}
		ctx.Error(http.StatusInternalServerError, "failed to dismiss destination name warnings", err)
		return
	}
	resp, err := migrationops.IgnoreRemainingPathIssues(mig)
	if err != nil {
		ctx.Error(http.StatusInternalServerError, "failed to dismiss destination name warnings", err)
		return
	}
	ctx.Response(http.StatusOK, resp)
}

func (h handler) resetPathRemap(ctx *middleware.Context) {
	migrationID := chi.URLParam(ctx.Request(), "migrationID")
	nodeID := chi.URLParam(ctx.Request(), "nodeID")
	if migrationID == "" || nodeID == "" {
		ctx.Error(http.StatusBadRequest, "migration id and node id are required", nil)
		return
	}
	mig, err := h.mgr.PathMigration(ctx.Request().Context(), migrationID)
	if err != nil {
		if errors.Is(err, corebridge.ErrMigrationNotFound) {
			ctx.Error(http.StatusNotFound, "migration not found", err)
			return
		}
		ctx.Error(http.StatusInternalServerError, "failed to reset destination name", err)
		return
	}
	resp, err := migrationops.ResetPathRemap(mig, nodeID)
	if err != nil {
		ctx.Error(http.StatusInternalServerError, "failed to reset destination name", err)
		return
	}
	ctx.Response(http.StatusOK, resp)
}

func (h handler) ignorePathIssueSubtree(ctx *middleware.Context) {
	migrationID := chi.URLParam(ctx.Request(), "migrationID")
	nodeID := chi.URLParam(ctx.Request(), "nodeID")
	if migrationID == "" || nodeID == "" {
		ctx.Error(http.StatusBadRequest, "migration id and node id are required", nil)
		return
	}
	mig, err := h.mgr.PathMigration(ctx.Request().Context(), migrationID)
	if err != nil {
		if errors.Is(err, corebridge.ErrMigrationNotFound) {
			ctx.Error(http.StatusNotFound, "migration not found", err)
			return
		}
		ctx.Error(http.StatusInternalServerError, "failed to dismiss destination name warning", err)
		return
	}
	resp, err := migrationops.IgnorePathIssueSubtree(mig, nodeID)
	if err != nil {
		ctx.Error(http.StatusInternalServerError, "failed to dismiss destination name warning", err)
		return
	}
	ctx.Response(http.StatusOK, resp)
}

func (h handler) unignorePathIssueSubtree(ctx *middleware.Context) {
	migrationID := chi.URLParam(ctx.Request(), "migrationID")
	nodeID := chi.URLParam(ctx.Request(), "nodeID")
	if migrationID == "" || nodeID == "" {
		ctx.Error(http.StatusBadRequest, "migration id and node id are required", nil)
		return
	}
	mig, err := h.mgr.PathMigration(ctx.Request().Context(), migrationID)
	if err != nil {
		if errors.Is(err, corebridge.ErrMigrationNotFound) {
			ctx.Error(http.StatusNotFound, "migration not found", err)
			return
		}
		ctx.Error(http.StatusInternalServerError, "failed to restore destination name warning", err)
		return
	}
	resp, err := migrationops.UnignorePathIssueSubtree(mig, nodeID)
	if err != nil {
		ctx.Error(http.StatusInternalServerError, "failed to restore destination name warning", err)
		return
	}
	ctx.Response(http.StatusOK, resp)
}
