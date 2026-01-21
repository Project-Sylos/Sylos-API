package preferences

import (
	"github.com/go-chi/chi/v5"
	"github.com/rs/zerolog"

	"codeberg.org/Sylos/Sylos-API/internal/routes/middleware"
)

type handler struct {
	logger  zerolog.Logger
	dataDir string
}

// Register mounts preferences endpoints.
func Register(router chi.Router, logger zerolog.Logger, dataDir string, mw *middleware.Middleware) {
	h := handler{
		logger:  logger,
		dataDir: dataDir,
	}

	router.Get("/preferences", middleware.NoBody(mw, h.getPreferences))
	router.Put("/preferences", middleware.JSON(mw, h.updatePreferences))
}
