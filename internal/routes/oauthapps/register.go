package oauthapps

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
	router.With(appauth.RequireRole("admin")).Get("/oauth-apps", middleware.NoBody(mw, h.list))
	router.With(appauth.RequireRole("admin")).Put("/oauth-apps/{providerID}", middleware.JSON(mw, h.save))
	router.With(appauth.RequireRole("admin")).Post("/oauth-apps/{providerID}/test", middleware.JSON(mw, h.test))
}

func (h handler) list(ctx *middleware.Context) {
	items, err := h.mgr.ListOAuthApps(ctx.Request().Context())
	if err != nil {
		ctx.Error(http.StatusInternalServerError, "failed to list oauth apps", err)
		return
	}
	ctx.Response(http.StatusOK, items)
}

func (h handler) save(ctx *middleware.Context, req manager.SaveOAuthAppRequest) {
	providerID := chi.URLParam(ctx.Request(), "providerID")
	if err := h.mgr.SaveOAuthApp(ctx.Request().Context(), providerID, req); err != nil {
		ctx.Error(http.StatusBadRequest, "failed to save oauth app", err)
		return
	}
	ctx.Response(http.StatusNoContent, nil)
}

func (h handler) test(ctx *middleware.Context, req manager.TestOAuthAppRequest) {
	providerID := chi.URLParam(ctx.Request(), "providerID")
	if err := h.mgr.TestOAuthApp(ctx.Request().Context(), providerID, req); err != nil {
		ctx.Error(http.StatusBadRequest, "oauth app test failed", err)
		return
	}
	ctx.Response(http.StatusOK, map[string]bool{"ok": true})
}
