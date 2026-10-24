package migrations

import (
	"errors"
	"io"
	"net/http"
	"path/filepath"
	"strings"

	"codeberg.org/Sylos/Sylos-API/internal/corebridge/migrationfiles"
	"codeberg.org/Sylos/Sylos-API/internal/routes/middleware"
)

// uploadDB handles POST /migrations/db/upload for Resume "upload ops folder".
// Multipart: file (zip of ops contents), filename (folder / migration id), optional overwrite=true.
func (h handler) uploadDB(ctx *middleware.Context) {
	const maxSize = 500 << 20 // 500MB (Badger ops dirs can be large)
	if err := ctx.Request().ParseMultipartForm(maxSize); err != nil {
		ctx.Error(http.StatusBadRequest, "failed to parse multipart form", err)
		return
	}

	overwrite := ctx.Request().FormValue("overwrite") == "true"
	rawName := strings.TrimSpace(ctx.Request().FormValue("filename"))
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

	if rawName == "" && header != nil {
		rawName = header.Filename
	}
	migrationID := migrationfiles.NormalizeMigrationID(rawName)
	if migrationID == "" {
		migrationID = migrationfiles.StripZipNameToMigrationID(rawName)
	}
	if migrationID == "" {
		migrationID = migrationfiles.NormalizeMigrationID(filepath.Base(rawName))
	}
	if migrationID == "" {
		ctx.Error(http.StatusBadRequest, "could not derive migration id from filename", nil)
		return
	}

	data, err := io.ReadAll(file)
	if err != nil {
		ctx.Error(http.StatusInternalServerError, "failed to read file data", err)
		return
	}

	response, err := h.mgr.UploadByType(ctx.Request().Context(), migrationID, migrationfiles.UploadTypeOps, data, overwrite)
	if err != nil {
		ctx.Error(http.StatusInternalServerError, "failed to upload migration ops", err)
		return
	}
	if !response.Success {
		ctx.Error(http.StatusBadRequest, response.Error, nil)
		return
	}
	ctx.Response(http.StatusOK, response)
}
