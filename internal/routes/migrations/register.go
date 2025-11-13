package migrations

import (
	"github.com/go-chi/chi/v5"
	"github.com/rs/zerolog"

	"github.com/Project-Sylos/Sylos-API/internal/corebridge"
)

type handler struct {
	logger zerolog.Logger
	core   corebridge.Bridge
}

// Register mounts migration orchestration endpoints.
func Register(router chi.Router, logger zerolog.Logger, core corebridge.Bridge) {
	h := handler{
		logger: logger,
		core:   core,
	}

	router.Post("/migrations", h.handleStartMigration)
	router.Post("/migrate/start", h.handleStartMigration) // legacy alias
	router.Get("/migrations/{migrationID}", h.handleGetMigrationStatus)
	router.Get("/migrate/status/{migrationID}", h.handleGetMigrationStatus) // legacy alias
}
