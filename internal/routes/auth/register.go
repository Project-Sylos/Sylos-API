package auth

import (
	"github.com/go-chi/chi/v5"
	"github.com/rs/zerolog"

	appauth "codeberg.org/Sylos/Sylos-API/internal/auth"
	"codeberg.org/Sylos/Sylos-API/internal/auth/users"
	"codeberg.org/Sylos/Sylos-API/internal/routes/middleware"
)

type handler struct {
	logger    zerolog.Logger
	manager   *appauth.Manager
	userStore *users.Store
}

// Register mounts authentication routes that do not require prior credentials.
func Register(router chi.Router, logger zerolog.Logger, manager *appauth.Manager, userStore *users.Store, mw *middleware.Middleware) {
	h := handler{logger: logger, manager: manager, userStore: userStore}
	router.Post("/api/auth/login", middleware.JSON(mw, h.login))
}

func RegisterProtected(router chi.Router, userStore *users.Store, mw *middleware.Middleware) {
	session := NewSessionHandler(userStore)
	router.Get("/auth/me", middleware.NoBody(mw, session.Me))
	router.Post("/auth/logout", middleware.NoBody(mw, session.Logout))
}
