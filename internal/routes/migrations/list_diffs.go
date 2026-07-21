package migrations

import (
	"errors"
	"net/http"
	"strconv"

	"github.com/go-chi/chi/v5"

	"codeberg.org/Sylos/Sylos-API/internal/corebridge"
	"codeberg.org/Sylos/Sylos-API/internal/routes/middleware"
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

	afterPath := ctx.Request().URL.Query().Get("afterPath") // Keyset cursor for next page

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

	includeDestinationOnly := parseOptionalBoolQuery(ctx.Request().URL.Query().Get("includeDestinationOnly"))

	mig, err := h.mgr.GetMigration(ctx.Request().Context(), migrationID)
	if err != nil {
		if errors.Is(err, corebridge.ErrMigrationNotFound) {
			ctx.Error(http.StatusNotFound, "migration not found", err)
			return
		}
		if errors.Is(err, corebridge.ErrDatabaseNotAvailable) {
			ctx.Error(http.StatusServiceUnavailable, "database not available", err)
			return
		}
		ctx.Error(http.StatusInternalServerError, "failed to get migration", err)
		return
	}
	diffs, err := corebridge.ListChildrenDiffs(mig, corebridge.ListChildrenDiffsRequest{
		MigrationID:            migrationID,
		Path:                   path,
		Offset:                 offset,
		Limit:                  limit,
		AfterPath:              afterPath,
		FoldersOnly:            foldersOnly,
		IncludeDestinationOnly: includeDestinationOnly,
	})
	if err != nil {
		ctx.Error(http.StatusInternalServerError, "failed to list children diffs", err)
		return
	}
	ctx.Response(http.StatusOK, diffs)
}

// diffsStats returns total and folder/file counts for children of path (separate endpoint; UI can cancel on nav).
func (h handler) diffsStats(ctx *middleware.Context) {
	migrationID := chi.URLParam(ctx.Request(), "migrationID")
	if migrationID == "" {
		ctx.Error(http.StatusBadRequest, "migration id is required", nil)
		return
	}

	path := ctx.Request().URL.Query().Get("path")
	if path == "" {
		path = "/"
	}

	foldersOnly := false
	if foldersOnlyStr := ctx.Request().URL.Query().Get("foldersOnly"); foldersOnlyStr != "" {
		if parsed, err := strconv.ParseBool(foldersOnlyStr); err == nil {
			foldersOnly = parsed
		}
	}

	includeDestinationOnly := parseOptionalBoolQuery(ctx.Request().URL.Query().Get("includeDestinationOnly"))

	mig, err := h.mgr.GetMigration(ctx.Request().Context(), migrationID)
	if err != nil {
		if errors.Is(err, corebridge.ErrMigrationNotFound) {
			ctx.Error(http.StatusNotFound, "migration not found", err)
			return
		}
		if errors.Is(err, corebridge.ErrDatabaseNotAvailable) {
			ctx.Error(http.StatusServiceUnavailable, "database not available", err)
			return
		}
		ctx.Error(http.StatusInternalServerError, "failed to get migration", err)
		return
	}
	stats, err := corebridge.GetChildrenDiffsStats(mig, path, foldersOnly, includeDestinationOnly)
	if err != nil {
		ctx.Error(http.StatusInternalServerError, "failed to get diffs stats", err)
		return
	}
	ctx.Response(http.StatusOK, stats)
}

func parseOptionalBoolQuery(raw string) *bool {
	if raw == "" {
		return nil
	}
	parsed, err := strconv.ParseBool(raw)
	if err != nil {
		return nil
	}
	return &parsed
}
