package migrations

import (
	"net/http"
	"net/url"

	"github.com/go-chi/chi/v5"

	"github.com/Project-Sylos/Sylos-API/internal/corebridge"
	"github.com/Project-Sylos/Sylos-API/internal/routes/middleware"
)

// excludeNode handles URL param (for backward compatibility)
func (h handler) excludeNode(ctx *middleware.Context) {
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

	h.logger.Info().Str("migration_id", migrationID).Str("node_id", unescapedNodeID).Msg("excluding node")

	// nodeID is a ULID - pass directly without hashing
	result, err := h.core.ExcludeNode(ctx.Request().Context(), migrationID, unescapedNodeID)
	if err != nil {
		ctx.Error(http.StatusInternalServerError, "failed to exclude node", err)
		return
	}

	if !result.Success {
		ctx.Error(http.StatusBadRequest, result.Error, nil)
		return
	}

	ctx.Response(http.StatusOK, result)
}

// excludeNodes handles JSON body request
func (h handler) excludeNodes(ctx *middleware.Context, payload corebridge.ExclusionRequest) {
	migrationID := chi.URLParam(ctx.Request(), "migrationID")
	if migrationID == "" {
		ctx.Error(http.StatusBadRequest, "migration id is required", nil)
		return
	}

	h.logger.Info().Str("migration_id", migrationID).Interface("request", payload).Msg("excluding nodes")

	// Use the new ExcludeNodes method
	result, err := h.core.ExcludeNodes(ctx.Request().Context(), migrationID, payload)
	if err != nil {
		ctx.Error(http.StatusInternalServerError, "failed to exclude nodes", err)
		return
	}

	if !result.Success {
		ctx.Error(http.StatusBadRequest, result.Error, nil)
		return
	}

	ctx.Response(http.StatusOK, result)
}

// unexcludeNode handles URL param (for backward compatibility)
func (h handler) unexcludeNode(ctx *middleware.Context) {
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

	// nodeID is a ULID - pass directly without hashing
	result, err := h.core.UnexcludeNode(ctx.Request().Context(), migrationID, unescapedNodeID)
	if err != nil {
		ctx.Error(http.StatusInternalServerError, "failed to unexclude node", err)
		return
	}

	if !result.Success {
		ctx.Error(http.StatusBadRequest, result.Error, nil)
		return
	}

	ctx.Response(http.StatusOK, result)
}

// unexcludeNodes handles JSON body request
func (h handler) unexcludeNodes(ctx *middleware.Context, payload corebridge.ExclusionRequest) {
	migrationID := chi.URLParam(ctx.Request(), "migrationID")
	if migrationID == "" {
		ctx.Error(http.StatusBadRequest, "migration id is required", nil)
		return
	}

	h.logger.Info().Str("migration_id", migrationID).Interface("request", payload).Msg("unexcluding nodes")

	// Use the new UnexcludeNodes method
	result, err := h.core.UnexcludeNodes(ctx.Request().Context(), migrationID, payload)
	if err != nil {
		ctx.Error(http.StatusInternalServerError, "failed to unexclude nodes", err)
		return
	}

	if !result.Success {
		ctx.Error(http.StatusBadRequest, result.Error, nil)
		return
	}

	ctx.Response(http.StatusOK, result)
}
