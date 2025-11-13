package migrations

import (
	"errors"
	"net/http"

	"github.com/go-chi/chi/v5"

	"github.com/Project-Sylos/Sylos-API/internal/corebridge"
)

func (h handler) handleGetMigrationStatus(w http.ResponseWriter, r *http.Request) {
	migrationID := chi.URLParam(r, "migrationID")
	if migrationID == "" {
		h.respondError(w, http.StatusBadRequest, "migration id is required")
		return
	}

	status, err := h.core.GetMigrationStatus(r.Context(), migrationID)
	if err != nil {
		if errors.Is(err, corebridge.ErrMigrationNotFound) {
			h.respondError(w, http.StatusNotFound, "migration not found")
			return
		}

		h.logger.Error().
			Err(err).
			Str("migration_id", migrationID).
			Msg("failed to get migration status")
		h.respondError(w, http.StatusInternalServerError, "failed to get migration status")
		return
	}

	h.respondJSON(w, http.StatusOK, status)
}
