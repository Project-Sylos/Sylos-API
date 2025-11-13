package migrations

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/Project-Sylos/Sylos-API/internal/corebridge"
)

func (h handler) handleStartMigration(w http.ResponseWriter, r *http.Request) {
	var req corebridge.StartMigrationRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.respondError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	migration, err := h.core.StartMigration(r.Context(), req)
	if err != nil {
		if errors.Is(err, corebridge.ErrServiceNotFound) {
			h.respondError(w, http.StatusBadRequest, "unknown service in request")
			return
		}

		h.logger.Error().Err(err).Msg("failed to start migration")
		h.respondError(w, http.StatusInternalServerError, "failed to start migration")
		return
	}

	h.respondJSON(w, http.StatusAccepted, migration)
}
