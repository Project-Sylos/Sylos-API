package auth

import (
	"net/http"

	appauth "codeberg.org/Sylos/Sylos-API/internal/auth"
	"codeberg.org/Sylos/Sylos-API/internal/auth/users"
	"codeberg.org/Sylos/Sylos-API/internal/routes/middleware"
)

type sessionHandler struct {
	userStore *users.Store
}

type meResponse struct {
	users.User
	Capabilities appauth.UserCapabilities `json:"capabilities"`
}

func NewSessionHandler(userStore *users.Store) sessionHandler {
	return sessionHandler{userStore: userStore}
}

func (h sessionHandler) Me(ctx *middleware.Context) {
	claims, ok := appauth.ClaimsFromContext(ctx.Request().Context())
	if !ok {
		ctx.Error(http.StatusUnauthorized, "unauthorized", nil)
		return
	}

	user, err := h.userStore.GetByID(claims.Subject)
	if err != nil {
		ctx.Error(http.StatusUnauthorized, "user not found", err)
		return
	}

	ctx.Response(http.StatusOK, meResponse{
		User:         user,
		Capabilities: appauth.CapabilitiesFromContext(ctx.Request().Context()),
	})
}

func (h sessionHandler) Logout(ctx *middleware.Context) {
	ctx.Response(http.StatusNoContent, nil)
}
