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
	router.Post("/api/public/recover-password", middleware.JSON(mw, h.recoverPassword))
}

func RegisterProtected(router chi.Router, logger zerolog.Logger, userStore *users.Store, mw *middleware.Middleware) {
	h := handler{logger: logger, userStore: userStore}
	session := NewSessionHandler(userStore)
	router.Get("/auth/me", middleware.NoBody(mw, session.Me))
	router.Post("/auth/logout", middleware.NoBody(mw, h.logout))
	router.Post("/auth/change-password", middleware.JSON(mw, h.changePassword))
	router.Get("/auth/recovery-code/status", middleware.NoBody(mw, h.recoveryCodeStatus))
	router.Post("/auth/recovery-code/regenerate", middleware.NoBody(mw, h.regenerateRecoveryCode))
	router.Post("/auth/recovery-code/acknowledge", middleware.NoBody(mw, h.acknowledgeRecoveryCode))
}
