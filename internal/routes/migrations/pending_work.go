package migrations

import (
	"errors"
	"net/http"
	"net/url"
	"strings"

	"github.com/go-chi/chi/v5"

	"github.com/Project-Sylos/Sylos-API/internal/corebridge"
	"github.com/Project-Sylos/Sylos-API/internal/routes/middleware"
	"github.com/Project-Sylos/Sylos-API/pkg/common_utils"
)

// contains is a helper function to check if a string contains a substring (case-insensitive)
func contains(s, substr string) bool {
	return strings.Contains(strings.ToLower(s), strings.ToLower(substr))
}

// checkPendingWork handles GET /api/migrations/{migrationID}/pending-work
func (h handler) checkPendingWork(ctx *middleware.Context) {
	migrationID := chi.URLParam(ctx.Request(), "migrationID")
	if migrationID == "" {
		ctx.Error(http.StatusBadRequest, "migration id is required", nil)
		return
	}

	response, err := h.core.CheckPendingWork(ctx.Request().Context(), migrationID)
	if err != nil {
		if errors.Is(err, corebridge.ErrMigrationNotFound) {
			ctx.Error(http.StatusNotFound, "migration not found", err)
			return
		}
		if errors.Is(err, corebridge.ErrDatabaseNotAvailable) {
			ctx.Error(http.StatusServiceUnavailable, "database not available", err)
			return
		}

		ctx.Error(http.StatusInternalServerError, "failed to check pending work", err)
		return
	}

	ctx.Response(http.StatusOK, response)
}

// markNodeForRetry handles POST /api/migrations/{migrationID}/node/{nodeID}/mark-retry
func (h handler) markNodeForRetry(ctx *middleware.Context) {
	migrationID := chi.URLParam(ctx.Request(), "migrationID")
	if migrationID == "" {
		ctx.Error(http.StatusBadRequest, "migration id is required", nil)
		return
	}

	nodeID := chi.URLParam(ctx.Request(), "nodeID")
	if nodeID == "" {
		ctx.Error(http.StatusBadRequest, "node id is required", nil)
		return
	}

	// unescape node id
	unescapedNodeID, err := url.QueryUnescape(nodeID)
	if err != nil {
		ctx.Error(http.StatusBadRequest, "failed to unescape node id", err)
		return
	}

	// hash path before sending it to the core
	hashedPath := common_utils.HashPath(unescapedNodeID)

	result, err := h.core.MarkNodeForRetry(ctx.Request().Context(), migrationID, hashedPath)
	if err != nil {
		if errors.Is(err, corebridge.ErrMigrationNotFound) {
			ctx.Error(http.StatusNotFound, "migration not found", err)
			return
		}
		if errors.Is(err, corebridge.ErrDatabaseNotAvailable) {
			ctx.Error(http.StatusServiceUnavailable, "database not available", err)
			return
		}

		// For node not found or status errors, return 200 with success: false
		// This allows the UI to handle the error gracefully
		errMsg := err.Error()
		if contains(errMsg, "not found") || contains(errMsg, "not in failed status") {
			ctx.Response(http.StatusOK, &corebridge.MarkRetryResponse{
				Success: false,
				Error:   errMsg,
			})
			return
		}

		ctx.Error(http.StatusInternalServerError, "failed to mark node for retry", err)
		return
	}

	// Always return 200, even if success is false (e.g., node not found, wrong status)
	ctx.Response(http.StatusOK, result)
}

// unmarkNodeForRetry handles POST /api/migrations/{migrationID}/node/{nodeID}/unmark-retry
func (h handler) unmarkNodeForRetry(ctx *middleware.Context) {
	migrationID := chi.URLParam(ctx.Request(), "migrationID")
	if migrationID == "" {
		ctx.Error(http.StatusBadRequest, "migration id is required", nil)
		return
	}

	nodeID := chi.URLParam(ctx.Request(), "nodeID")
	if nodeID == "" {
		ctx.Error(http.StatusBadRequest, "node id is required", nil)
		return
	}

	// unescape node id
	unescapedNodeID, err := url.QueryUnescape(nodeID)
	if err != nil {
		ctx.Error(http.StatusBadRequest, "failed to unescape node id", err)
		return
	}

	// hash path before sending it to the core
	hashedPath := common_utils.HashPath(unescapedNodeID)

	result, err := h.core.UnmarkNodeForRetry(ctx.Request().Context(), migrationID, hashedPath)
	if err != nil {
		if errors.Is(err, corebridge.ErrMigrationNotFound) {
			ctx.Error(http.StatusNotFound, "migration not found", err)
			return
		}
		if errors.Is(err, corebridge.ErrDatabaseNotAvailable) {
			ctx.Error(http.StatusServiceUnavailable, "database not available", err)
			return
		}

		// For node not found or status errors, return 200 with success: false
		// This allows the UI to handle the error gracefully
		errMsg := err.Error()
		if contains(errMsg, "not found") || contains(errMsg, "not in pending status") {
			ctx.Response(http.StatusOK, &corebridge.MarkRetryResponse{
				Success: false,
				Error:   errMsg,
			})
			return
		}

		ctx.Error(http.StatusInternalServerError, "failed to unmark node for retry", err)
		return
	}

	// Always return 200, even if success is false (e.g., node not found, wrong status)
	ctx.Response(http.StatusOK, result)
}
