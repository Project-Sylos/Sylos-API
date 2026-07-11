package usersroutes

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/rs/zerolog"

	appauth "codeberg.org/Sylos/Sylos-API/internal/auth"
	"codeberg.org/Sylos/Sylos-API/internal/auth/users"
	"codeberg.org/Sylos/Sylos-API/internal/routes/middleware"
)

type handler struct {
	logger    zerolog.Logger
	userStore *users.Store
}

func Register(router chi.Router, logger zerolog.Logger, userStore *users.Store, mw *middleware.Middleware) {
	h := handler{logger: logger, userStore: userStore}
	router.With(appauth.RequireRole("admin")).Get("/users", middleware.NoBody(mw, h.list))
	router.With(appauth.RequireRole("admin")).Post("/users", middleware.JSON(mw, h.create))
	router.With(appauth.RequireRole("admin")).Post("/users/bulk-delete", middleware.JSON(mw, h.bulkDelete))
	router.With(appauth.RequireRole("admin")).Patch("/users/{id}", middleware.JSON(mw, h.update))
	router.With(appauth.RequireRole("admin")).Delete("/users/{id}", middleware.NoBody(mw, h.delete))
}

func (h handler) actorID(ctx *middleware.Context) string {
	claims, ok := appauth.ClaimsFromContext(ctx.Request().Context())
	if !ok {
		return ""
	}
	return claims.Subject
}

func (h handler) list(ctx *middleware.Context) {
	items, err := h.userStore.List()
	if err != nil {
		ctx.Error(http.StatusInternalServerError, "failed to list users", err)
		return
	}
	ctx.Response(http.StatusOK, items)
}

type createUserRequest struct {
	Username string     `json:"username"`
	Password string     `json:"password"`
	Role     users.Role `json:"role"`
}

type createUserResponse struct {
	users.User
	RecoveryCode string `json:"recoveryCode,omitempty"`
}

func (h handler) create(ctx *middleware.Context, req createUserRequest) {
	if req.Role == "" {
		req.Role = users.RoleUser
	}
	user, err := h.userStore.Create(req.Username, req.Password, req.Role)
	if err != nil {
		switch {
		case errors.Is(err, users.ErrExists):
			ctx.Error(http.StatusConflict, "username already exists", err)
		default:
			ctx.Error(http.StatusBadRequest, "failed to create user", err)
		}
		return
	}
	actorID := h.actorID(ctx)
	if err := h.userStore.RecordAuditEvent(users.AuditEvent{
		ActorUserID:  actorID,
		TargetUserID: user.ID,
		Action:       users.AuditUserCreate,
	}); err != nil {
		h.logger.Warn().Err(err).Str("user_id", user.ID).Msg("record user create audit")
	}
	resp := createUserResponse{User: user}
	if code, err := h.userStore.IssueRecoveryCode(user.ID, false); err == nil {
		resp.RecoveryCode = code
	}
	ctx.Response(http.StatusCreated, resp)
}

type updateUserRequest struct {
	Password *string     `json:"password"`
	Role     *users.Role `json:"role"`
	Disabled *bool       `json:"disabled"`
}

func (h handler) update(ctx *middleware.Context, req updateUserRequest) {
	id := chi.URLParam(ctx.Request(), "id")
	user, err := h.userStore.Update(id, req.Password, req.Role, req.Disabled)
	if err != nil {
		switch {
		case errors.Is(err, users.ErrNotFound):
			ctx.Error(http.StatusNotFound, "user not found", err)
		case errors.Is(err, users.ErrLastAdmin):
			ctx.Error(http.StatusConflict, "cannot modify the last admin", err)
		default:
			ctx.Error(http.StatusBadRequest, "failed to update user", err)
		}
		return
	}
	meta, _ := json.Marshal(req)
	if err := h.userStore.RecordAuditEvent(users.AuditEvent{
		ActorUserID:  h.actorID(ctx),
		TargetUserID: user.ID,
		Action:       users.AuditUserUpdate,
		Metadata:     string(meta),
	}); err != nil {
		h.logger.Warn().Err(err).Str("user_id", user.ID).Msg("record user update audit")
	}
	ctx.Response(http.StatusOK, user)
}

func (h handler) delete(ctx *middleware.Context) {
	id := chi.URLParam(ctx.Request(), "id")
	if err := h.userStore.Delete(id); err != nil {
		switch {
		case errors.Is(err, users.ErrNotFound):
			ctx.Error(http.StatusNotFound, "user not found", err)
		case errors.Is(err, users.ErrLastAdmin):
			ctx.Error(http.StatusConflict, "cannot delete the last admin", err)
		default:
			ctx.Error(http.StatusInternalServerError, "failed to delete user", err)
		}
		return
	}
	if err := h.userStore.RecordAuditEvent(users.AuditEvent{
		ActorUserID:  h.actorID(ctx),
		TargetUserID: id,
		Action:       users.AuditUserDelete,
	}); err != nil {
		h.logger.Warn().Err(err).Str("user_id", id).Msg("record user delete audit")
	}
	ctx.Response(http.StatusNoContent, nil)
}

type bulkDeleteRequest struct {
	IDs []string `json:"ids"`
}

type bulkDeleteResponse struct {
	Deleted []string `json:"deleted"`
}

func (h handler) bulkDelete(ctx *middleware.Context, req bulkDeleteRequest) {
	actorID := h.actorID(ctx)
	deleted, err := h.userStore.DeleteMany(req.IDs, actorID)
	for _, id := range deleted {
		if auditErr := h.userStore.RecordAuditEvent(users.AuditEvent{
			ActorUserID:  actorID,
			TargetUserID: id,
			Action:       users.AuditUserDelete,
		}); auditErr != nil {
			h.logger.Warn().Err(auditErr).Str("user_id", id).Msg("record bulk user delete audit")
		}
	}
	if len(deleted) == 0 {
		if err != nil {
			switch {
			case errors.Is(err, users.ErrNotFound):
				ctx.Error(http.StatusNotFound, "no users deleted", err)
			case errors.Is(err, users.ErrLastAdmin):
				ctx.Error(http.StatusConflict, "cannot delete the last admin", err)
			default:
				ctx.Error(http.StatusBadRequest, "failed to delete users", err)
			}
			return
		}
		ctx.Error(http.StatusBadRequest, "no users deleted", nil)
		return
	}
	ctx.Response(http.StatusOK, bulkDeleteResponse{Deleted: deleted})
}
