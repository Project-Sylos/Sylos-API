package preferences

import (
	"github.com/go-chi/chi/v5"
	"github.com/rs/zerolog"

	"codeberg.org/Sylos/Sylos-API/internal/auth/users"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge/manager"
	"codeberg.org/Sylos/Sylos-API/internal/routes/middleware"
)

type handler struct {
	logger    zerolog.Logger
	userStore *users.Store
	mgr       *manager.Manager
}

// Register mounts preferences endpoints.
func Register(router chi.Router, logger zerolog.Logger, userStore *users.Store, mgr *manager.Manager, mw *middleware.Middleware) {
	h := handler{
		logger:    logger,
		userStore: userStore,
		mgr:       mgr,
	}

	router.Get("/preferences", middleware.NoBody(mw, h.getPreferences))
	router.Put("/preferences", middleware.JSON(mw, h.updatePreferences))
}

func applyStallTimeoutPrefs(mgr *manager.Manager, prefs Preferences) {
	if mgr == nil {
		return
	}
	trav, del, copySec := prefs.Developer.StallTimeoutSecs()
	mgr.SetFSStallTimeoutSecs(trav, del, copySec)
}
