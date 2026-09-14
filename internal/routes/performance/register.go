package performance

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

// Register mounts install-level performance settings (DuckDB memory, etc.).
func Register(router chi.Router, logger zerolog.Logger, mgr *manager.Manager, mw *middleware.Middleware) {
	h := handler{logger: logger, mgr: mgr}
	router.Get("/performance/duckdb-memory", middleware.NoBody(mw, h.getDuckDBMemory))
	router.With(appauth.RequireRole("admin")).Put("/performance/duckdb-memory", middleware.JSON(mw, h.putDuckDBMemory))
}

func (h handler) getDuckDBMemory(ctx *middleware.Context) {
	settings, err := h.mgr.GetDuckDBMemorySettings(ctx.Request().Context())
	if err != nil {
		ctx.Error(http.StatusInternalServerError, "failed to read duckdb memory settings", err)
		return
	}
	ctx.Response(http.StatusOK, settings)
}

func (h handler) putDuckDBMemory(ctx *middleware.Context, req manager.DuckDBMemorySettings) {
	if err := h.mgr.SaveDuckDBMemorySettings(ctx.Request().Context(), req); err != nil {
		ctx.Error(http.StatusBadRequest, "failed to save duckdb memory settings", err)
		return
	}
	settings, err := h.mgr.GetDuckDBMemorySettings(ctx.Request().Context())
	if err != nil {
		ctx.Error(http.StatusInternalServerError, "failed to read duckdb memory settings", err)
		return
	}
	ctx.Response(http.StatusOK, settings)
}
