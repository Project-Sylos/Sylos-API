package migrations

import (
	"errors"
	"net/http"
	"strconv"
	"strings"

	"github.com/go-chi/chi/v5"

	"codeberg.org/Sylos/Sylos-API/internal/corebridge"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge/migrationops"
	"codeberg.org/Sylos/Sylos-API/internal/routes/middleware"
)

// search handles POST /api/migrations/{migrationID}/search
// Requires at least one narrowing filter (see migrationops.SearchRequestHasFilter).
// Search results are path-joined (same format as diff endpoint). Pagination HasMore comes from the engine;
// Total is omitted when unknown.
func (h handler) search(ctx *middleware.Context, payload corebridge.SearchRequest) {
	migrationID := chi.URLParam(ctx.Request(), "migrationID")
	if migrationID == "" {
		ctx.Error(http.StatusBadRequest, "migration id is required", nil)
		return
	}

	normalizeSearchPayload(&payload)
	if !migrationops.SearchRequestHasFilter(payload) {
		ctx.Error(http.StatusBadRequest, "search requires at least one filter", migrationops.ErrSearchRequiresFilter)
		return
	}

	// Parse pagination parameters from query string (similar to diff endpoint)
	offset := 0
	if offsetStr := ctx.Request().URL.Query().Get("offset"); offsetStr != "" {
		if parsed, err := strconv.Atoi(offsetStr); err == nil && parsed >= 0 {
			offset = parsed
		}
	}

	limit := 1000 // Default limit
	if limitStr := ctx.Request().URL.Query().Get("limit"); limitStr != "" {
		if parsed, err := strconv.Atoi(limitStr); err == nil && parsed > 0 {
			limit = parsed
			if limit > 10000 {
				limit = 10000 // Max limit (increased to handle large datasets)
			}
		}
	}

	// Extract sort options (can come from query params or request body)
	if sortField := ctx.Request().URL.Query().Get("sortField"); sortField != "" {
		// Sort from query parameters takes precedence over request body
		sortDir := ctx.Request().URL.Query().Get("sortDir")
		if sortDir == "" {
			sortDir = "asc" // Default to ascending
		}
		payload.Sort = &corebridge.SortOption{
			Field:     sortField,
			Direction: sortDir,
		}
	}

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
	diffs, err := migrationops.SearchPathReviewItems(mig, payload, offset, limit)
	if err != nil {
		if errors.Is(err, migrationops.ErrSearchRequiresFilter) {
			ctx.Error(http.StatusBadRequest, "search requires at least one filter", err)
			return
		}
		ctx.Error(http.StatusInternalServerError, "failed to search", err)
		return
	}
	ctx.Response(http.StatusOK, diffs)
}

// searchCount handles POST /api/migrations/{migrationID}/search/count
// Same body as search; returns exact total/folder/file stats via GetSearchStats.
func (h handler) searchCount(ctx *middleware.Context, payload corebridge.SearchRequest) {
	migrationID := chi.URLParam(ctx.Request(), "migrationID")
	if migrationID == "" {
		ctx.Error(http.StatusBadRequest, "migration id is required", nil)
		return
	}

	normalizeSearchPayload(&payload)
	if !migrationops.SearchRequestHasFilter(payload) {
		ctx.Error(http.StatusBadRequest, "search requires at least one filter", migrationops.ErrSearchRequiresFilter)
		return
	}

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
	stats, err := migrationops.GetSearchStats(mig, payload)
	if err != nil {
		if errors.Is(err, migrationops.ErrSearchRequiresFilter) {
			ctx.Error(http.StatusBadRequest, "search requires at least one filter", err)
			return
		}
		ctx.Error(http.StatusInternalServerError, "failed to get search count", err)
		return
	}
	ctx.Response(http.StatusOK, stats)
}

// normalizeSearchPayload lowercases path/name values for case-insensitive search.
func normalizeSearchPayload(payload *corebridge.SearchRequest) {
	for i := range payload.Conditions {
		if payload.Conditions[i].Field == "path" || payload.Conditions[i].Field == "name" {
			if valueStr, ok := payload.Conditions[i].Value.(string); ok {
				payload.Conditions[i].Value = strings.ToLower(valueStr)
			}
		}
	}
}
