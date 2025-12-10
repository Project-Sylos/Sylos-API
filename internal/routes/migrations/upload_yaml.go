package migrations

import (
	"errors"
	"io"
	"net/http"

	"github.com/Project-Sylos/Sylos-API/internal/routes/middleware"
	"github.com/go-chi/chi/v5"
)

func (h handler) uploadYAML(ctx *middleware.Context) {
	migrationID := chi.URLParam(ctx.Request(), "migrationID")
	if migrationID == "" {
		ctx.Error(http.StatusBadRequest, "migration id is required", nil)
		return
	}

	// Parse multipart form
	if err := ctx.Request().ParseMultipartForm(100 << 20); err != nil { // 100MB max
		ctx.Error(http.StatusBadRequest, "failed to parse multipart form", err)
		return
	}

	// Get overwrite flag
	overwrite := ctx.Request().FormValue("overwrite") == "true"

	// Get file from form
	file, _, err := ctx.Request().FormFile("file")
	if err != nil {
		if errors.Is(err, http.ErrMissingFile) {
			ctx.Error(http.StatusBadRequest, "file is required", err)
			return
		}
		ctx.Error(http.StatusBadRequest, "failed to get file from form", err)
		return
	}
	defer file.Close()

	// Read file data
	data, err := io.ReadAll(file)
	if err != nil {
		ctx.Error(http.StatusInternalServerError, "failed to read file data", err)
		return
	}

	// Upload to core bridge
	response, err := h.core.UploadMigrationYAML(ctx.Request().Context(), migrationID, data, overwrite)
	if err != nil {
		ctx.Error(http.StatusInternalServerError, "failed to upload migration YAML", err)
		return
	}

	if !response.Success {
		ctx.Error(http.StatusBadRequest, response.Error, nil)
		return
	}

	ctx.Response(http.StatusOK, response)
}

