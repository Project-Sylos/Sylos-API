package migrations

import (
	"errors"
	"io"
	"net/http"
	"strings"

	"codeberg.org/Sylos/Sylos-API/internal/corebridge/database"
	"codeberg.org/Sylos/Sylos-API/internal/routes/middleware"
	"github.com/go-chi/chi/v5"
)

func (h handler) uploadUnified(ctx *middleware.Context) {
	migrationID := chi.URLParam(ctx.Request(), "migrationID")
	if migrationID == "" {
		ctx.Error(http.StatusBadRequest, "migration id is required", nil)
		return
	}

	uploadType := strings.TrimSpace(strings.ToLower(ctx.Request().URL.Query().Get("type")))
	if uploadType == "" {
		uploadType = database.UploadTypeZip
	}
	if uploadType != database.UploadTypeZip && uploadType != database.UploadTypeDB && uploadType != database.UploadTypeYAML {
		ctx.Error(http.StatusBadRequest, "type must be zip, db, or yaml", nil)
		return
	}

	maxSize := int64(100 << 20) // 100MB for db/yaml
	if uploadType == database.UploadTypeZip {
		maxSize = 500 << 20 // 500MB for zip
	}
	if err := ctx.Request().ParseMultipartForm(maxSize); err != nil {
		ctx.Error(http.StatusBadRequest, "failed to parse multipart form", err)
		return
	}

	overwrite := ctx.Request().FormValue("overwrite") == "true"

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

	data, err := io.ReadAll(file)
	if err != nil {
		ctx.Error(http.StatusInternalServerError, "failed to read file data", err)
		return
	}

	response, err := h.core.UploadByType(ctx.Request().Context(), migrationID, uploadType, data, overwrite)
	if err != nil {
		ctx.Error(http.StatusInternalServerError, "failed to upload migration data", err)
		return
	}

	if !response.Success {
		ctx.Error(http.StatusBadRequest, response.Error, nil)
		return
	}

	ctx.Response(http.StatusOK, response)
}
