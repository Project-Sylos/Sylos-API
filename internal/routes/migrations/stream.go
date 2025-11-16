package migrations

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/Project-Sylos/Sylos-API/internal/corebridge"
	"github.com/Project-Sylos/Sylos-API/internal/routes/httputil"
)

func (h handler) handleStream(w http.ResponseWriter, r *http.Request) {
	migrationID := chi.URLParam(r, "migrationID")
	if migrationID == "" {
		httputil.WriteError(h.logger, w, http.StatusBadRequest, "migration id is required")
		return
	}

	updates, cancel, err := h.core.SubscribeProgress(r.Context(), migrationID)
	if err != nil {
		if errors.Is(err, corebridge.ErrMigrationNotFound) {
			httputil.WriteError(h.logger, w, http.StatusNotFound, "migration not found")
			return
		}
		h.logger.Error().
			Err(err).
			Str("migration_id", migrationID).
			Msg("failed to subscribe to migration progress")
		httputil.WriteError(h.logger, w, http.StatusInternalServerError, "failed to subscribe to migration progress")
		return
	}
	defer cancel()

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming not supported", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	heartbeat := time.NewTicker(30 * time.Second)
	defer heartbeat.Stop()

	h.logger.Info().
		Str("migration_id", migrationID).
		Msg("opening progress stream")

	for {
		select {
		case <-r.Context().Done():
			h.logger.Info().
				Str("migration_id", migrationID).
				Msg("client closed progress stream")
			return
		case <-heartbeat.C:
			fmt.Fprintf(w, ": heartbeat\n\n")
			flusher.Flush()
		case update, ok := <-updates:
			if !ok {
				fmt.Fprintf(w, "event: close\ndata: {}\n\n")
				flusher.Flush()
				return
			}

			payload, err := json.Marshal(update)
			if err != nil {
				h.logger.Error().
					Err(err).
					Str("migration_id", migrationID).
					Msg("failed to marshal progress event")
				continue
			}

			eventName := update.Event
			if eventName == "" {
				eventName = "message"
			}

			fmt.Fprintf(w, "event: %s\n", eventName)
			fmt.Fprintf(w, "data: %s\n\n", payload)
			flusher.Flush()
		}
	}
}
