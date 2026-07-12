package services

import (
	"github.com/go-chi/chi/v5"
	"github.com/rs/zerolog"

	"codeberg.org/Sylos/Sylos-API/internal/auth/users"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge"
	"codeberg.org/Sylos/Sylos-API/internal/routes/middleware"
)

type handler struct {
	logger    zerolog.Logger
	core      corebridge.Bridge
	userStore *users.Store
}

// Register mounts filesystem service discovery and browsing routes.
func Register(router chi.Router, logger zerolog.Logger, core corebridge.Bridge, userStore *users.Store, mw *middleware.Middleware) {
	h := handler{
		logger:    logger,
		core:      core,
		userStore: userStore,
	}

	router.Get("/services", middleware.NoBody(mw, h.listServices))
	router.Get("/source/list", middleware.NoBody(mw, h.listServices)) // legacy alias
	router.Get("/services/{serviceID}/children", middleware.NoBody(mw, h.listChildren))
	router.Post("/services/{serviceID}/folders", middleware.JSON(mw, h.createFolder))
	router.Post("/services/{serviceID}/nodes/delete", middleware.JSON(mw, h.deleteNodes))
	router.Get("/services/{serviceID}/drives", middleware.NoBody(mw, h.listDrives))
	router.Post("/services/{serviceID}/drives/mount", middleware.JSON(mw, h.mountDrive))
}
