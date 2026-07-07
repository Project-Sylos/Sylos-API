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
	router.With(appauth.RequireRole("admin")).Post("/admin/clean-slate", middleware.NoBody(mw, h.cleanSlate))
}

func (h handler) cleanSlate(ctx *middleware.Context) {
	resp, err := h.mgr.CleanSlate(ctx.Request().Context())
	if err != nil {
		ctx.Error(http.StatusInternalServerError, "failed to clean migration data", err)
		return
	}
	ctx.Response(http.StatusOK, resp)
}
