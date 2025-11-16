package auth

import (
	"net/http"

	"github.com/Project-Sylos/Sylos-API/internal/routes/middleware"
)

type loginRequest struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

type loginResponse struct {
	Token string `json:"token"`
}

func (h handler) login(ctx *middleware.Context, req loginRequest) {
	if req.Username == "" || req.Password == "" {
		ctx.Error(http.StatusUnauthorized, "invalid credentials", nil)
		return
	}

	token, err := h.manager.GenerateToken(req.Username, []string{"user"})
	if err != nil {
		ctx.Error(http.StatusInternalServerError, "failed to generate token", err)
		return
	}

	ctx.Response(http.StatusOK, loginResponse{Token: token})
}
