package migrations

import (
	"errors"
	"net/http"
	"strconv"

	"github.com/go-chi/chi/v5"

	"github.com/Project-Sylos/Sylos-API/internal/corebridge"
	"github.com/Project-Sylos/Sylos-API/internal/routes/middleware"
)

func (h handler) listDiffs(ctx *middleware.Context) {
	migrationID := chi.URLParam(ctx.Request(), "migrationID")
	if migrationID == "" {
		ctx.Error(http.StatusBadRequest, "migration id is required", nil)
		return
	}

	// Parse query parameters
	path := ctx.Request().URL.Query().Get("path")
	if path == "" {
		path = "/"
	}

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
			if limit > 1000 {
				limit = 1000 // Max limit
			}
		}
	}

	foldersOnly := false
	if foldersOnlyStr := ctx.Request().URL.Query().Get("foldersOnly"); foldersOnlyStr != "" {
		if parsed, err := strconv.ParseBool(foldersOnlyStr); err == nil {
			foldersOnly = parsed
		}
	}

	diffs, err := h.core.ListChildrenDiffs(ctx.Request().Context(), corebridge.ListChildrenDiffsRequest{
		MigrationID: migrationID,
		Path:        path,
		Offset:      offset,
		Limit:       limit,
		FoldersOnly: foldersOnly,
	})
	if err != nil {
		if errors.Is(err, corebridge.ErrMigrationNotFound) {
			ctx.Error(http.StatusNotFound, "migration not found", err)
			return
		}
		if errors.Is(err, corebridge.ErrDatabaseNotAvailable) {
			ctx.Error(http.StatusServiceUnavailable, "database not available", err)
			return
		}

		ctx.Error(http.StatusInternalServerError, "failed to list children diffs", err)
		return
	}

	ctx.Response(http.StatusOK, diffs)
}

