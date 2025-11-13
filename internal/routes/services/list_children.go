package services

import (
	"net/http"

	"github.com/go-chi/chi/v5"

	"github.com/Project-Sylos/Sylos-API/internal/corebridge"
)

func (h handler) handleListChildren(w http.ResponseWriter, r *http.Request) {
	serviceID := chi.URLParam(r, "serviceID")
	if serviceID == "" {
		h.respondError(w, http.StatusBadRequest, "service id is required")
		return
	}

	identifier := r.URL.Query().Get("identifier")
	children, err := h.core.ListChildren(r.Context(), corebridge.ListChildrenRequest{
		ServiceID:  serviceID,
		Identifier: identifier,
	})
	if err != nil {
		if err == corebridge.ErrServiceNotFound {
			h.respondError(w, http.StatusNotFound, "service not found")
			return
		}

		h.logger.Error().Err(err).Str("service", serviceID).Msg("failed to list children")
		h.respondError(w, http.StatusInternalServerError, "failed to list children")
		return
	}

	h.respondJSON(w, http.StatusOK, children)
}
