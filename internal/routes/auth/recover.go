package auth

import (
	"errors"
	"net/http"

	"codeberg.org/Sylos/Sylos-API/internal/auth/users"
	"codeberg.org/Sylos/Sylos-API/internal/routes/middleware"
)

type recoverPasswordRequest struct {
	Username     string `json:"username"`
	RecoveryCode string `json:"recoveryCode"`
	NewPassword  string `json:"newPassword"`
}

type recoverPasswordResponse struct {
	Message string `json:"message"`
}

func (h handler) recoverPassword(ctx *middleware.Context, req recoverPasswordRequest) {
	if err := h.userStore.ResetPasswordWithRecoveryCode(req.Username, req.RecoveryCode, req.NewPassword); err != nil {
		switch {
		case errors.Is(err, users.ErrInvalidRecoveryCode):
			ctx.Error(http.StatusUnauthorized, "invalid username or recovery code", err)
		case errors.Is(err, users.ErrDisabled):
			ctx.Error(http.StatusUnauthorized, "account disabled", err)
		default:
			ctx.Error(http.StatusBadRequest, "failed to reset password", err)
		}
		return
	}
	ctx.Response(http.StatusOK, recoverPasswordResponse{
		Message: "Password updated. Sign in with your new password to receive a new recovery code.",
	})
}
