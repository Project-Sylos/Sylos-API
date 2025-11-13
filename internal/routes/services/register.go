package services

import (
	"github.com/go-chi/chi/v5"
	"github.com/rs/zerolog"

	"github.com/Project-Sylos/Sylos-API/internal/corebridge"
)

type handler struct {
	logger zerolog.Logger
	core   corebridge.Bridge
}

// Register mounts filesystem service discovery and browsing routes.
func Register(router chi.Router, logger zerolog.Logger, core corebridge.Bridge) {
	h := handler{
		logger: logger,
		core:   core,
	}

	router.Get("/services", h.handleListServices)
	router.Get("/source/list", h.handleListServices) // legacy alias
	router.Get("/services/{serviceID}/children", h.handleListChildren)
}
