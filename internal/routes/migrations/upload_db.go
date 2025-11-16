package migrations

import (
	"errors"
	"io"
	"net/http"

	"github.com/Project-Sylos/Sylos-API/internal/routes/middleware"
)

func (h handler) uploadDB(ctx *middleware.Context) {
	// Parse multipart form
	if err := ctx.Request().ParseMultipartForm(100 << 20); err != nil { // 100MB max
		ctx.Error(http.StatusBadRequest, "failed to parse multipart form", err)
		return
	}

	// Get filename from form
	filename := ctx.Request().FormValue("filename")
	if filename == "" {
		ctx.Error(http.StatusBadRequest, "filename is required", nil)
		return
	}

	// Get overwrite flag
	overwrite := ctx.Request().FormValue("overwrite") == "true"

	// Get file from form
	file, header, err := ctx.Request().FormFile("file")
	if err != nil {
		if errors.Is(err, http.ErrMissingFile) {
			ctx.Error(http.StatusBadRequest, "file is required", err)
			return
		}
		ctx.Error(http.StatusBadRequest, "failed to get file from form", err)
		return
	}
	defer file.Close()

	// If filename not provided, use the uploaded file's name
	if filename == "" {
		filename = header.Filename
	}

	// Read file data
	data, err := io.ReadAll(file)
	if err != nil {
		ctx.Error(http.StatusInternalServerError, "failed to read file data", err)
		return
	}

	// Upload to core bridge
	response, err := h.core.UploadMigrationDB(ctx.Request().Context(), filename, data, overwrite)
	if err != nil {
		ctx.Error(http.StatusInternalServerError, "failed to upload migration DB", err)
		return
	}

	if !response.Success {
		ctx.Error(http.StatusBadRequest, response.Error, nil)
		return
	}

	ctx.Response(http.StatusOK, response)
}
