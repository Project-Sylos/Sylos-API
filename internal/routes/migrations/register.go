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
	router.Post("/migrations/{migrationID}/phase-change", middleware.JSON(mw, h.changePhase))
	router.Post("/migrations/log-terminal", middleware.JSON(mw, h.toggleLogTerminal))
	router.Post("/migrations/{migrationID}/db/upload", middleware.MultipartForm(mw, h.uploadDB))
	router.Post("/migrations/{migrationID}/yaml/upload", middleware.MultipartForm(mw, h.uploadYAML))
	router.Post("/migrations/{migrationID}/data/upload", middleware.MultipartForm(mw, h.uploadData))
	router.Get("/migrations/db/list", middleware.NoBody(mw, h.listDBs))
	router.Get("/migrations", middleware.NoBody(mw, h.list))
	router.Post("/migrations/{migrationID}/load", middleware.NoBody(mw, h.load))
	router.Post("/migrations/{migrationID}/stop", middleware.NoBody(mw, h.stop))
	router.Get("/migrations/{migrationID}", middleware.NoBody(mw, h.status))
	router.Get("/migrations/{migrationID}/inspect", middleware.NoBody(mw, h.inspect))
	router.Get("/migrations/{migrationID}/queue-metrics", middleware.NoBody(mw, h.queueMetrics))
	router.Post("/migrations/{migrationID}/logs", middleware.JSON(mw, h.getLogs))
	router.Get("/migrations/{migrationID}/diffs", middleware.NoBody(mw, h.listDiffs))
	router.Post("/migrations/{migrationID}/exclude", middleware.JSON(mw, h.excludeNodes))
	router.Post("/migrations/{migrationID}/node/{nodeID}/exclude", middleware.NoBody(mw, h.excludeNode)) // Backward compatibility
	router.Post("/migrations/{migrationID}/unexclude", middleware.JSON(mw, h.unexcludeNodes))
	router.Post("/migrations/{migrationID}/node/{nodeID}/unexclude", middleware.NoBody(mw, h.unexcludeNode)) // Backward compatibility
	router.Post("/migrations/{migrationID}/node/{nodeID}/mark-retry", middleware.NoBody(mw, h.markNodeForRetry))
	router.Post("/migrations/{migrationID}/node/{nodeID}/unmark-retry", middleware.NoBody(mw, h.unmarkNodeForRetry))
	router.Get("/migrations/{migrationID}/pending-work", middleware.NoBody(mw, h.checkPendingWork))
	router.Get("/migrations/{migrationID}/bgTasks", middleware.NoBody(mw, h.bgTasks))
	router.Get("/migrations/{migrationID}/stats", middleware.NoBody(mw, h.stats))
	router.Post("/migrations/{migrationID}/search", middleware.JSON(mw, h.search))
	router.Get("/migrations/{migrationID}/stream", h.handleStream)
}
