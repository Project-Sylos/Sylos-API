package auth

import (
	"errors"
	"net/http"

	appauth "codeberg.org/Sylos/Sylos-API/internal/auth"
	"codeberg.org/Sylos/Sylos-API/internal/auth/users"
	"codeberg.org/Sylos/Sylos-API/internal/routes/middleware"
)

type changePasswordRequest struct {
	CurrentPassword string `json:"currentPassword"`
	NewPassword     string `json:"newPassword"`
}

func (h handler) changePassword(ctx *middleware.Context, req changePasswordRequest) {
	claims, ok := appauth.ClaimsFromContext(ctx.Request().Context())
	if !ok {
		ctx.Error(http.StatusUnauthorized, "unauthorized", nil)
		return
	}
	if err := h.userStore.ChangePassword(claims.Subject, req.CurrentPassword, req.NewPassword); err != nil {
		switch {
		case errors.Is(err, users.ErrInvalidCreds):
			ctx.Error(http.StatusUnauthorized, "current password is incorrect", err)
		case errors.Is(err, users.ErrDisabled):
			ctx.Error(http.StatusUnauthorized, "account disabled", err)
		default:
			ctx.Error(http.StatusBadRequest, "failed to change password", err)
		}
		return
	}
	ctx.Response(http.StatusNoContent, nil)
}

func (h handler) recoveryCodeStatus(ctx *middleware.Context) {
	claims, ok := appauth.ClaimsFromContext(ctx.Request().Context())
	if !ok {
		ctx.Error(http.StatusUnauthorized, "unauthorized", nil)
		return
	}
	status, err := h.userStore.RecoveryCodeStatusFor(claims.Subject)
	if err != nil {
		ctx.Error(http.StatusInternalServerError, "failed to read recovery code status", err)
		return
	}
	ctx.Response(http.StatusOK, status)
}

type recoveryCodeResponse struct {
	RecoveryCode string `json:"recoveryCode"`
}

func (h handler) regenerateRecoveryCode(ctx *middleware.Context) {
	claims, ok := appauth.ClaimsFromContext(ctx.Request().Context())
	if !ok {
		ctx.Error(http.StatusUnauthorized, "unauthorized", nil)
		return
	}
	code, err := h.userStore.IssueRecoveryCode(claims.Subject, true)
	if err != nil {
		ctx.Error(http.StatusInternalServerError, "failed to generate recovery code", err)
		return
	}
	ctx.Response(http.StatusOK, recoveryCodeResponse{RecoveryCode: code})
}

func (h handler) acknowledgeRecoveryCode(ctx *middleware.Context) {
	claims, ok := appauth.ClaimsFromContext(ctx.Request().Context())
	if !ok {
		ctx.Error(http.StatusUnauthorized, "unauthorized", nil)
		return
	}
	if err := h.userStore.AcknowledgeRecoveryCode(claims.Subject); err != nil {
		ctx.Error(http.StatusInternalServerError, "failed to acknowledge recovery code", err)
		return
	}
	ctx.Response(http.StatusNoContent, nil)
}
