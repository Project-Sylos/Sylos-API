package routes

import (
	"github.com/go-chi/chi/v5"
	"github.com/rs/zerolog"

	"github.com/Project-Sylos/Sylos-API/internal/api"
	"github.com/Project-Sylos/Sylos-API/internal/auth"
	"github.com/Project-Sylos/Sylos-API/internal/corebridge"
)

type Dependencies struct {
	Logger      zerolog.Logger
	CoreBridge  corebridge.Bridge
	AuthManager *auth.Manager
}

func New(deps Dependencies) chi.Router {
	apiServer := api.New(api.Dependencies{
		Logger:      deps.Logger,
		CoreBridge:  deps.CoreBridge,
		AuthManager: deps.AuthManager,
	})

	return apiServer.Router()
}
