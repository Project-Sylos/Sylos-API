package setup

import (
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/rs/zerolog"

	appauth "codeberg.org/Sylos/Sylos-API/internal/auth"
	"codeberg.org/Sylos/Sylos-API/internal/auth/users"
	"codeberg.org/Sylos/Sylos-API/internal/routes/middleware"
)

type handler struct {
	logger      zerolog.Logger
	users       *users.Store
	authManager *appauth.Manager
}

func RegisterPublic(router chi.Router, logger zerolog.Logger, userStore *users.Store, authManager *appauth.Manager, mw *middleware.Middleware) {
	h := handler{logger: logger, users: userStore, authManager: authManager}
	router.Get("/api/public/setup/status", middleware.NoBody(mw, h.status))
	router.Post("/api/public/setup", middleware.JSON(mw, h.setup))
}

type statusResponse struct {
	NeedsSetup bool `json:"needsSetup"`
}

func (h handler) status(ctx *middleware.Context) {
	count, err := h.users.Count()
	if err != nil {
		ctx.Error(http.StatusInternalServerError, "failed to read setup status", err)
		return
	}
	ctx.Response(http.StatusOK, statusResponse{NeedsSetup: count == 0})
}

type setupRequest struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

type setupResponse struct {
	Token string     `json:"token"`
	User  users.User `json:"user"`
}

func (h handler) setup(ctx *middleware.Context, req setupRequest) {
	user, err := h.users.CreateInitialAdmin(req.Username, req.Password)
	if err != nil {
		switch err {
		case users.ErrSetupComplete:
			ctx.Error(http.StatusConflict, "setup already completed", err)
		case users.ErrExists:
			ctx.Error(http.StatusConflict, "username already exists", err)
		default:
			ctx.Error(http.StatusBadRequest, "failed to create admin user", err)
		}
		return
	}

	token, err := h.authManager.GenerateToken(user.ID, []string{string(user.Role)})
	if err != nil {
		ctx.Error(http.StatusInternalServerError, "failed to generate token", err)
		return
	}

	ctx.Response(http.StatusCreated, setupResponse{Token: token, User: user})
}
