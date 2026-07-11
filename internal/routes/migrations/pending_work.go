package migrations

import (
	"errors"
	"net/http"
	"net/url"
	"strings"

	"github.com/go-chi/chi/v5"

	"codeberg.org/Sylos/Sylos-API/internal/corebridge"
	"codeberg.org/Sylos/Sylos-API/internal/routes/middleware"
)

// containsInsensitive reports whether substr appears in s, case-insensitively.
func containsInsensitive(s, substr string) bool {
	return strings.Contains(strings.ToLower(s), strings.ToLower(substr))
}

// checkPendingWork handles GET /api/migrations/{migrationID}/pending-work
func (h handler) checkPendingWork(ctx *middleware.Context) {
	migrationID := chi.URLParam(ctx.Request(), "migrationID")
	if migrationID == "" {
		ctx.Error(http.StatusBadRequest, "migration id is required", nil)
		return
	}

	response, err := h.mgr.CheckPendingWork(ctx.Request().Context(), migrationID)
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

func (h handler) handleMarkNodeForRetry(ctx *middleware.Context, kind corebridge.RetryKind) {
	migrationID, unescapedNodeID, ok := h.loadMigrationNode(ctx)
	if !ok {
		return
	}

	mig, err := h.mgr.GetMigration(ctx.Request().Context(), migrationID)
	if err != nil {
		h.writeMigrationLoadError(ctx, err)
		return
	}

	result, err := corebridge.MarkNodesForRetry(mig, kind, corebridge.MarkRetryRequest{
		NodeIDs: []string{unescapedNodeID},
	})
	if err != nil {
		errMsg := err.Error()
		if isMarkRetryBenignError(kind, errMsg) {
			ctx.Response(http.StatusOK, &corebridge.MarkRetryResponse{
				Success: false,
				Error:   errMsg,
				Deltas:  map[string]int64{},
			})
			return
		}
		ctx.Error(http.StatusInternalServerError, markRetryErrorLabel(kind), err)
		return
	}
	if result.Success {
		_ = h.mgr.MarkPathReviewChanges(ctx.Request().Context(), migrationID, true)
	}
	ctx.Response(http.StatusOK, result)
}

func (h handler) handleUnmarkNodeForRetry(ctx *middleware.Context, kind corebridge.RetryKind) {
	migrationID, unescapedNodeID, ok := h.loadMigrationNode(ctx)
	if !ok {
		return
	}

	mig, err := h.mgr.GetMigration(ctx.Request().Context(), migrationID)
	if err != nil {
		h.writeMigrationLoadError(ctx, err)
		return
	}

	result, err := corebridge.UnmarkNodeForRetry(mig, kind, unescapedNodeID)
	if err != nil {
		errMsg := err.Error()
		if isUnmarkRetryBenignError(kind, errMsg) {
			ctx.Response(http.StatusOK, &corebridge.MarkRetryResponse{
				Success: false,
				Error:   errMsg,
				Deltas:  map[string]int64{},
			})
			return
		}
		ctx.Error(http.StatusInternalServerError, unmarkRetryErrorLabel(kind), err)
		return
	}
	if result.Success {
		_ = h.mgr.MarkPathReviewChanges(ctx.Request().Context(), migrationID, true)
	}
	ctx.Response(http.StatusOK, result)
}

func (h handler) loadMigrationNode(ctx *middleware.Context) (migrationID, unescapedNodeID string, ok bool) {
	migrationID = chi.URLParam(ctx.Request(), "migrationID")
	if migrationID == "" {
		ctx.Error(http.StatusBadRequest, "migration id is required", nil)
		return "", "", false
	}

	nodeID := chi.URLParam(ctx.Request(), "nodeID")
	if nodeID == "" {
		ctx.Error(http.StatusBadRequest, "node id is required", nil)
		return "", "", false
	}

	unescapedNodeID, err := url.QueryUnescape(nodeID)
	if err != nil {
		ctx.Error(http.StatusBadRequest, "failed to unescape node id", err)
		return "", "", false
	}

	return migrationID, unescapedNodeID, true
}

func (h handler) writeMigrationLoadError(ctx *middleware.Context, err error) {
	if errors.Is(err, corebridge.ErrMigrationNotFound) {
		ctx.Error(http.StatusNotFound, "migration not found", err)
		return
	}
	if errors.Is(err, corebridge.ErrDatabaseNotAvailable) {
		ctx.Error(http.StatusServiceUnavailable, "database not available", err)
		return
	}
	ctx.Error(http.StatusInternalServerError, "failed to get migration", err)
}

func isMarkRetryBenignError(kind corebridge.RetryKind, errMsg string) bool {
	if containsInsensitive(errMsg, "not found") || containsInsensitive(errMsg, "not in failed status") {
		return true
	}
	return kind == corebridge.RetryKindCopy && containsInsensitive(errMsg, "copy retry only applies to src nodes")
}

func isUnmarkRetryBenignError(kind corebridge.RetryKind, errMsg string) bool {
	if containsInsensitive(errMsg, "not found") || containsInsensitive(errMsg, "not in pending status") {
		return true
	}
	return kind == corebridge.RetryKindCopy && containsInsensitive(errMsg, "copy retry only applies to src nodes")
}

func markRetryErrorLabel(kind corebridge.RetryKind) string {
	if kind == corebridge.RetryKindCopy {
		return "failed to mark node for copy retry"
	}
	return "failed to mark node for discovery retry"
}

func unmarkRetryErrorLabel(kind corebridge.RetryKind) string {
	if kind == corebridge.RetryKindCopy {
		return "failed to unmark node for copy retry"
	}
	return "failed to unmark node for discovery retry"
}
