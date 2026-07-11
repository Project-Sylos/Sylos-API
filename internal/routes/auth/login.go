package auth

import (
	"errors"
	"net/http"
	"time"

	"codeberg.org/Sylos/Sylos-API/internal/auth/users"
	"codeberg.org/Sylos/Sylos-API/internal/routes/middleware"
)

type loginRequest struct {
	Username string `json:"username"`
	Password string `json:"password"`
	Remember bool   `json:"remember"`
}

type loginResponse struct {
	Token        string     `json:"token"`
	User         users.User `json:"user"`
	RecoveryCode string     `json:"recoveryCode,omitempty"`
}

func (h handler) login(ctx *middleware.Context, req loginRequest) {
	user, err := h.userStore.Authenticate(req.Username, req.Password)
	if err != nil {
		switch {
		case errors.Is(err, users.ErrInvalidCreds):
			_ = h.userStore.RecordLoginFailed(req.Username)
			ctx.Error(http.StatusUnauthorized, "invalid credentials", err)
		case errors.Is(err, users.ErrDisabled):
			_ = h.userStore.RecordLoginFailed(req.Username)
			ctx.Error(http.StatusUnauthorized, "account disabled", err)
		default:
			ctx.Error(http.StatusInternalServerError, "login failed", err)
		}
		return
	}

	if err := h.userStore.RecordLogin(user.ID); err != nil {
		h.logger.Warn().Err(err).Str("user_id", user.ID).Msg("record login audit")
	} else {
		now := time.Now().UTC()
		user.LastLoginAt = &now
	}

	ttl := 24 * time.Hour
	if req.Remember {
		ttl = 30 * 24 * time.Hour
	}

	token, err := h.manager.GenerateTokenWithTTL(user.ID, []string{string(user.Role)}, ttl)
	if err != nil {
		ctx.Error(http.StatusInternalServerError, "failed to generate token", err)
		return
	}

	resp := loginResponse{Token: token, User: user}
	if code, issued, err := h.userStore.TakePendingRecoveryReissue(user.ID); err == nil && issued {
		resp.RecoveryCode = code
	}

	ctx.Response(http.StatusOK, resp)
}
