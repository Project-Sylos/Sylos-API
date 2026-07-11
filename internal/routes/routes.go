package routes

import (
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	chiMiddleware "github.com/go-chi/chi/v5/middleware"
	"github.com/go-chi/cors"
	"github.com/rs/zerolog"

	"codeberg.org/Sylos/Sylos-API/internal/auth"
	"codeberg.org/Sylos/Sylos-API/internal/auth/users"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge/manager"
	adminroutes "codeberg.org/Sylos/Sylos-API/internal/routes/admin"
	authroutes "codeberg.org/Sylos/Sylos-API/internal/routes/auth"
	healthroutes "codeberg.org/Sylos/Sylos-API/internal/routes/health"
	middlewarepkg "codeberg.org/Sylos/Sylos-API/internal/routes/middleware"
	migrationroutes "codeberg.org/Sylos/Sylos-API/internal/routes/migrations"
	oauthappsroutes "codeberg.org/Sylos/Sylos-API/internal/routes/oauthapps"
	preferencesroutes "codeberg.org/Sylos/Sylos-API/internal/routes/preferences"
	providerroutes "codeberg.org/Sylos/Sylos-API/internal/routes/providers"
	serviceroutes "codeberg.org/Sylos/Sylos-API/internal/routes/services"
	setuproutes "codeberg.org/Sylos/Sylos-API/internal/routes/setup"
	usersroutes "codeberg.org/Sylos/Sylos-API/internal/routes/users"
)

type Dependencies struct {
	Logger      zerolog.Logger
	CoreBridge  corebridge.Bridge
	Manager     *manager.Manager
	AuthManager *auth.Manager
	Middleware  *middlewarepkg.Middleware
	DataDir     string
	UserStore   *users.Store
}

func New(deps Dependencies) chi.Router {
	router := chi.NewRouter()
	router.Use(chiMiddleware.RequestID)
	router.Use(chiMiddleware.RealIP)
	router.Use(chiMiddleware.Recoverer)
	router.Use(loggingMiddleware(deps.Logger))
	router.Use(cors.Handler(cors.Options{
		AllowedOrigins:   []string{"*"},
		AllowedMethods:   []string{"GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"},
		AllowedHeaders:   []string{"Accept", "Authorization", "Content-Type", "X-CSRF-Token"},
		ExposedHeaders:   []string{"Link"},
		AllowCredentials: false,
		MaxAge:           300,
	}))

	mw := deps.Middleware
	if mw == nil {
		mw, _ = middlewarepkg.New(deps.Logger, "")
	}

	healthroutes.Register(router)
	setuproutes.RegisterPublic(router, deps.Logger, deps.UserStore, deps.AuthManager, mw)
	authroutes.Register(router, deps.Logger, deps.AuthManager, deps.UserStore, mw)

	apiRouter := chi.NewRouter()
	apiRouter.Use(deps.AuthManager.Middleware)

	healthroutes.Register(apiRouter)
	authroutes.RegisterProtected(apiRouter, deps.Logger, deps.UserStore, mw)
	usersroutes.Register(apiRouter, deps.Logger, deps.UserStore, mw)
	serviceroutes.Register(apiRouter, deps.Logger, deps.CoreBridge, mw)
	providerroutes.Register(apiRouter, deps.Logger, deps.Manager, mw)
	oauthappsroutes.Register(apiRouter, deps.Logger, deps.Manager, mw)
	adminroutes.Register(apiRouter, deps.Logger, deps.Manager, mw)
	migrationroutes.Register(apiRouter, deps.Logger, deps.Manager, mw)
	preferencesroutes.Register(apiRouter, deps.Logger, deps.UserStore, mw)

	router.Mount("/api", apiRouter)

	return router
}

func loggingMiddleware(logger zerolog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			ww := chiMiddleware.NewWrapResponseWriter(w, r.ProtoMajor)

			next.ServeHTTP(ww, r)

			logger.Info().
				Str("method", r.Method).
				Str("path", r.URL.Path).
				Int("status", ww.Status()).
				Int("bytes", ww.BytesWritten()).
				Dur("duration", time.Since(start)).
				Msg("request complete")
		})
	}
}
