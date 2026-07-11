package adminroutes

import (
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/rs/zerolog"

	appauth "codeberg.org/Sylos/Sylos-API/internal/auth"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge/manager"
	"codeberg.org/Sylos/Sylos-API/internal/routes/middleware"
)

type handler struct {
	logger zerolog.Logger
	mgr    *manager.Manager
}

func Register(router chi.Router, logger zerolog.Logger, mgr *manager.Manager, mw *middleware.Middleware) {
	h := handler{logger: logger, mgr: mgr}
	admin := router.With(appauth.RequireRole("admin"))
	admin.Post("/admin/clear-migrations", middleware.NoBody(mw, h.clearMigrations))
	admin.Post("/admin/wipe-install", middleware.NoBody(mw, h.wipeInstall))
	// Deprecated alias; prefer POST /admin/clear-migrations.
	admin.Post("/admin/clean-slate", middleware.NoBody(mw, h.clearMigrations))
}

func (h handler) clearMigrations(ctx *middleware.Context) {
	resp, err := h.mgr.ClearAllMigrations(ctx.Request().Context())
	if err != nil {
		ctx.Error(http.StatusInternalServerError, "failed to clear migration data", err)
		return
	}
	ctx.Response(http.StatusOK, resp)
}

func (h handler) wipeInstall(ctx *middleware.Context) {
	resp, err := h.mgr.WipeInstall(ctx.Request().Context())
	if err != nil {
		ctx.Error(http.StatusInternalServerError, "failed to wipe install data", err)
		return
	}
	ctx.Response(http.StatusOK, resp)
}
