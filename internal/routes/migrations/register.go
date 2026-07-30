package migrations

import (
	"github.com/go-chi/chi/v5"
	"github.com/rs/zerolog"

	"codeberg.org/Sylos/Sylos-API/internal/corebridge/manager"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge/migrationops"
	"codeberg.org/Sylos/Sylos-API/internal/routes/middleware"
)

type handler struct {
	logger zerolog.Logger
	mgr    *manager.Manager
}

// Register mounts migration orchestration endpoints.
func Register(router chi.Router, logger zerolog.Logger, mgr *manager.Manager, mw *middleware.Middleware) {
	h := handler{
		logger: logger,
		mgr:    mgr,
	}

	router.Post("/migrations/roots", middleware.JSON(mw, h.setRoot))
	router.Post("/migrations", middleware.JSON(mw, h.start))
	router.Post("/migrations/{migrationID}/phase-change", middleware.JSON(mw, h.changePhase))
	router.Post("/migrations/{migrationID}/retry-sweep", middleware.JSON(mw, h.retrySweep))
	router.Post("/migrations/{migrationID}/resume", middleware.JSON(mw, h.resume))
	router.Post("/migrations/log-terminal", middleware.JSON(mw, h.toggleLogTerminal))
	router.Post("/migrations/{migrationID}/upload", middleware.MultipartForm(mw, h.uploadUnified))
	router.Get("/migrations/db/list", middleware.NoBody(mw, h.listDBs))
	router.Get("/migrations", middleware.NoBody(mw, h.list))
	router.Post("/migrations/{migrationID}/load", middleware.NoBody(mw, h.load))
	router.Post("/migrations/{migrationID}/rename", middleware.JSON(mw, h.rename))
	router.Post("/migrations/{migrationID}/stop", middleware.NoBody(mw, h.stop))
	router.Get("/migrations/{migrationID}", middleware.NoBody(mw, h.status))
	router.Get("/migrations/{migrationID}/inspect", middleware.NoBody(mw, h.inspect))
	router.Get("/migrations/{migrationID}/queue-metrics", middleware.NoBody(mw, h.queueMetrics))
	router.Post("/migrations/{migrationID}/logs", middleware.JSON(mw, h.getLogs))
	router.Get("/migrations/{migrationID}/diffs/stats", middleware.NoBody(mw, h.diffsStats))
	router.Get("/migrations/{migrationID}/diffs", middleware.NoBody(mw, h.listDiffs))
	router.Post("/migrations/{migrationID}/exclude", middleware.JSON(mw, h.excludeNodes))
	router.Post("/migrations/{migrationID}/unexclude", middleware.JSON(mw, h.unexcludeNodes))
	router.Post("/migrations/{migrationID}/node/{nodeID}/mark-retry-discovery", middleware.NoBody(mw, func(ctx *middleware.Context) {
		h.handleMarkNodeForRetry(ctx, migrationops.RetryKindDiscovery)
	}))
	router.Post("/migrations/{migrationID}/node/{nodeID}/mark-retry-copy", middleware.NoBody(mw, func(ctx *middleware.Context) {
		h.handleMarkNodeForRetry(ctx, migrationops.RetryKindCopy)
	}))
	router.Post("/migrations/{migrationID}/node/{nodeID}/unmark-retry-discovery", middleware.NoBody(mw, func(ctx *middleware.Context) {
		h.handleUnmarkNodeForRetry(ctx, migrationops.RetryKindDiscovery)
	}))
	router.Post("/migrations/{migrationID}/node/{nodeID}/unmark-retry-copy", middleware.NoBody(mw, func(ctx *middleware.Context) {
		h.handleUnmarkNodeForRetry(ctx, migrationops.RetryKindCopy)
	}))
	router.Post("/migrations/{migrationID}/node/{nodeID}/mark-retry-delete", middleware.NoBody(mw, func(ctx *middleware.Context) {
		h.handleMarkNodeForRetry(ctx, migrationops.RetryKindDelete)
	}))
	router.Post("/migrations/{migrationID}/node/{nodeID}/unmark-retry-delete", middleware.NoBody(mw, func(ctx *middleware.Context) {
		h.handleUnmarkNodeForRetry(ctx, migrationops.RetryKindDelete)
	}))
	router.Post("/migrations/{migrationID}/prepare-source-cleanup", middleware.JSON(mw, h.prepareSourceCleanup))
	router.Post("/migrations/{migrationID}/node/{nodeID}/skip-delete", middleware.NoBody(mw, h.handleSkipNodeDelete))
	router.Post("/migrations/{migrationID}/node/{nodeID}/unskip-delete", middleware.NoBody(mw, h.handleUnskipNodeDelete))
	router.Get("/migrations/{migrationID}/delete-summary", middleware.NoBody(mw, h.deleteSummary))
	router.Get("/migrations/{migrationID}/pending-work", middleware.NoBody(mw, h.checkPendingWork))
	router.Get("/migrations/{migrationID}/path-issues", middleware.NoBody(mw, h.listPathIssues))
	router.Get("/migrations/{migrationID}/path-check-target", middleware.NoBody(mw, h.getPathCheckTarget))
	router.Post("/migrations/{migrationID}/path-check-target", middleware.JSON(mw, h.setPathCheckTarget))
	router.Post("/migrations/{migrationID}/path-issues/validate", middleware.JSON(mw, h.validatePathIssue))
	router.Post("/migrations/{migrationID}/path-issues/accept-all", middleware.NoBody(mw, h.acceptAllPathIssues))
	router.Post("/migrations/{migrationID}/path-issues/ignore-remaining", middleware.NoBody(mw, h.ignoreRemainingPathIssues))
	router.Post("/migrations/{migrationID}/path-issues/{nodeID}/accept", middleware.JSON(mw, h.acceptPathIssue))
	router.Post("/migrations/{migrationID}/path-issues/{nodeID}/remap", middleware.JSON(mw, h.remapPathIssue))
	router.Post("/migrations/{migrationID}/path-issues/{nodeID}/reset", middleware.NoBody(mw, h.resetPathRemap))
	router.Post("/migrations/{migrationID}/path-issues/{nodeID}/ignore", middleware.NoBody(mw, h.ignorePathIssueSubtree))
	router.Post("/migrations/{migrationID}/path-issues/{nodeID}/unignore", middleware.NoBody(mw, h.unignorePathIssueSubtree))
	router.Get("/migrations/{migrationID}/bgTasks", middleware.NoBody(mw, h.bgTasks))
	router.Get("/migrations/{migrationID}/bgTasks/running", middleware.NoBody(mw, h.bgTasksRunning))
	router.Get("/migrations/{migrationID}/bgTasks/{taskID}", middleware.NoBody(mw, h.bgTaskByID))
	router.Get("/migrations/{migrationID}/stats", middleware.NoBody(mw, h.stats))
	router.Get("/migrations/{migrationID}/scaling", middleware.NoBody(mw, h.getScaling))
	router.Put("/migrations/{migrationID}/scaling", middleware.JSON(mw, h.putScaling))
	router.Post("/migrations/{migrationID}/search/count", middleware.JSON(mw, h.searchCount))
	router.Post("/migrations/{migrationID}/search", middleware.JSON(mw, h.search))
	router.Get("/migrations/{migrationID}/stream", h.handleStream)
}
