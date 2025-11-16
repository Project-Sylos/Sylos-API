package auth

import (
	"github.com/go-chi/chi/v5"
	"github.com/rs/zerolog"

	appauth "github.com/Project-Sylos/Sylos-API/internal/auth"
	"github.com/Project-Sylos/Sylos-API/internal/routes/middleware"
)

type handler struct {
	logger  zerolog.Logger
	manager *appauth.Manager
}

// Register mounts authentication routes that do not require prior credentials.
func Register(router chi.Router, logger zerolog.Logger, manager *appauth.Manager, mw *middleware.Middleware) {
	h := handler{
		logger:  logger,
		manager: manager,
	}

	router.Post("/api/auth/login", middleware.JSON(mw, h.login))
}
