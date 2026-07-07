package usersroutes

import (
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
	router.With(appauth.RequireRole("admin")).Patch("/users/{id}", middleware.JSON(mw, h.update))
	router.With(appauth.RequireRole("admin")).Delete("/users/{id}", middleware.NoBody(mw, h.delete))
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
	ctx.Response(http.StatusCreated, user)
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
	ctx.Response(http.StatusNoContent, nil)
}
