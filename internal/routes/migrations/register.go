package migrations

import (
	"github.com/go-chi/chi/v5"
	"github.com/rs/zerolog"

	"github.com/Project-Sylos/Sylos-API/internal/corebridge"
	"github.com/Project-Sylos/Sylos-API/internal/routes/middleware"
)

type handler struct {
	logger zerolog.Logger
	core   corebridge.Bridge
}

// Register mounts migration orchestration endpoints.
func Register(router chi.Router, logger zerolog.Logger, core corebridge.Bridge, mw *middleware.Middleware) {
	h := handler{
		logger: logger,
		core:   core,
	}

	router.Post("/migrations/roots", middleware.JSON(mw, h.setRoot))
	router.Post("/migrations", middleware.JSON(mw, h.start))
	router.Post("/migrate/start", middleware.JSON(mw, h.start)) // legacy alias
	router.Post("/migrations/log-terminal", middleware.JSON(mw, h.toggleLogTerminal))
	router.Post("/migrations/db/upload", middleware.MultipartForm(mw, h.uploadDB))
	router.Get("/migrations/db/list", middleware.NoBody(mw, h.listDBs))
	router.Get("/migrations", middleware.NoBody(mw, h.list))
	router.Post("/migrations/{migrationID}/load", middleware.NoBody(mw, h.load))
	router.Post("/migrations/{migrationID}/stop", middleware.NoBody(mw, h.stop))
	router.Get("/migrations/{migrationID}", middleware.NoBody(mw, h.status))
	router.Get("/migrate/status/{migrationID}", middleware.NoBody(mw, h.status)) // legacy alias
	router.Get("/migrations/{migrationID}/inspect", middleware.NoBody(mw, h.inspect))
	router.Get("/migrations/{migrationID}/stream", h.handleStream)
	router.Get("/migrate/status/{migrationID}/stream", h.handleStream) // legacy alias
}
