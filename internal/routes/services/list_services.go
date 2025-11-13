package services

import (
	"net/http"
)

func (h handler) handleListServices(w http.ResponseWriter, r *http.Request) {
	sources, err := h.core.ListSources(r.Context())
	if err != nil {
		h.logger.Error().Err(err).Msg("failed to list services")
		h.respondError(w, http.StatusInternalServerError, "failed to list services")
		return
	}

	h.respondJSON(w, http.StatusOK, sources)
}
