package services

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

// Register mounts filesystem service discovery and browsing routes.
func Register(router chi.Router, logger zerolog.Logger, core corebridge.Bridge, mw *middleware.Middleware) {
	h := handler{
		logger: logger,
		core:   core,
	}

	router.Get("/services", middleware.NoBody(mw, h.listServices))
	router.Get("/source/list", middleware.NoBody(mw, h.listServices)) // legacy alias
	router.Get("/services/{serviceID}/children", middleware.NoBody(mw, h.listChildren))
}
