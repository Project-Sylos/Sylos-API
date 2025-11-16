package httputil

import (
	"encoding/json"
	"net/http"

	"github.com/rs/zerolog"
)

// WriteJSON serializes payload as JSON and writes it with the supplied status code.
func WriteJSON(logger zerolog.Logger, w http.ResponseWriter, status int, payload interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(payload); err != nil {
		logger.Error().Err(err).Msg("failed to write response")
	}
}

// WriteError writes a JSON error payload with the given status code.
func WriteError(logger zerolog.Logger, w http.ResponseWriter, status int, message string) {
	WriteJSON(logger, w, status, map[string]string{"error": message})
}
