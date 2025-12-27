package migrations

import (
	"errors"
	"net/http"
	"strconv"

	"github.com/go-chi/chi/v5"

	"github.com/Project-Sylos/Sylos-API/internal/corebridge"
	"github.com/Project-Sylos/Sylos-API/internal/routes/middleware"
)

// search handles POST /api/migrations/{migrationID}/search
// If no search parameters are provided (empty body or conditions array), lists all items (like diff endpoint with path="/")
// Search results are path-joined (same format as diff endpoint)
func (h handler) search(ctx *middleware.Context, payload corebridge.SearchRequest) {
	migrationID := chi.URLParam(ctx.Request(), "migrationID")
	if migrationID == "" {
		ctx.Error(http.StatusBadRequest, "migration id is required", nil)
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

	// If no search conditions provided (empty conditions), search all items
	// Use SearchPathReviewItems with empty conditions to get all items, not just root-level
	diffs, err := h.core.SearchPathReviewItems(ctx.Request().Context(), migrationID, payload, offset, limit)
	if err != nil {
		if errors.Is(err, corebridge.ErrMigrationNotFound) {
			ctx.Error(http.StatusNotFound, "migration not found", err)
			return
		}
		if errors.Is(err, corebridge.ErrDatabaseNotAvailable) {
			ctx.Error(http.StatusServiceUnavailable, "database not available", err)
			return
		}
		ctx.Error(http.StatusInternalServerError, "failed to search", err)
		return
	}
	ctx.Response(http.StatusOK, diffs)
}
