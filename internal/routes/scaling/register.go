package scaling

import (
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/rs/zerolog"

	appauth "codeberg.org/Sylos/Sylos-API/internal/auth"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge/apidb"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge/manager"
	"codeberg.org/Sylos/Sylos-API/internal/routes/middleware"
)

type handler struct {
	logger zerolog.Logger
	mgr    *manager.Manager
}

// Register mounts global scaling override endpoints.
func Register(router chi.Router, logger zerolog.Logger, mgr *manager.Manager, mw *middleware.Middleware) {
	h := handler{logger: logger, mgr: mgr}

	router.Get("/scaling/defaults", middleware.NoBody(mw, h.defaults))
	router.Get("/scaling/overrides", middleware.NoBody(mw, h.listOverrides))

	admin := router.With(appauth.RequireRole("admin"))
	admin.Put("/scaling/overrides/provider/{providerId}", middleware.JSON(mw, h.putOverride))
	admin.Delete("/scaling/overrides/provider/{providerId}", middleware.NoBody(mw, h.deleteOverride))
	admin.Put("/scaling/overrides/sftp/{hostId}", middleware.JSON(mw, h.putOverride))
	admin.Delete("/scaling/overrides/sftp/{hostId}", middleware.NoBody(mw, h.deleteOverride))
}

func (h handler) defaults(ctx *middleware.Context) {
	defaults := h.mgr.ListScalingDefaults()
	ctx.Response(http.StatusOK, defaults)
}

func (h handler) listOverrides(ctx *middleware.Context) {
	items, err := h.mgr.ListScalingOverrides()
	if err != nil {
		ctx.Error(http.StatusInternalServerError, "failed to list scaling overrides", err)
		return
	}
	ctx.Response(http.StatusOK, items)
}

func (h handler) putOverride(ctx *middleware.Context, req manager.SaveScalingOverridesRequest) {
	scope, key := overrideScopeKey(ctx.Request())
	if err := h.mgr.SaveScalingOverrides(scope, key, req.Modes); err != nil {
		ctx.Error(http.StatusBadRequest, "failed to save scaling overrides", err)
		return
	}
	ctx.Response(http.StatusNoContent, nil)
}

func (h handler) deleteOverride(ctx *middleware.Context) {
	scope, key := overrideScopeKey(ctx.Request())
	if err := h.mgr.DeleteScalingOverrides(scope, key); err != nil {
		ctx.Error(http.StatusBadRequest, "failed to delete scaling overrides", err)
		return
	}
	ctx.Response(http.StatusNoContent, nil)
}

func overrideScopeKey(r *http.Request) (scope, key string) {
	if id := chi.URLParam(r, "providerId"); id != "" {
		return apidb.ScopeProvider, id
	}
	return apidb.ScopeSFTPHost, chi.URLParam(r, "hostId")
}
