package health

import (
	"net/http"

	"github.com/go-chi/chi/v5"
)

// Register wires GET /health on the given router.
func Register(router chi.Router) {
	router.Get("/health", handleHealth)
}

func handleHealth(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"status":"ok"}`))
}
