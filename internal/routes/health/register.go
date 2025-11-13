package health

import (
	"net/http"

	"github.com/go-chi/chi/v5"
)

// RegisterPublic wires unauthenticated health checks (e.g., GET /health).
func RegisterPublic(router chi.Router) {
	router.Get("/health", handleHealth)
}

// RegisterProtected wires authenticated health checks under /api.
func RegisterProtected(router chi.Router) {
	router.Get("/health", handleHealth)
}

func handleHealth(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"status":"ok"}`))
}
