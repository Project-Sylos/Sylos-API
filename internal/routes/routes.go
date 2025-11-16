package routes

import (
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	chiMiddleware "github.com/go-chi/chi/v5/middleware"
	"github.com/go-chi/cors"
	"github.com/rs/zerolog"

	"github.com/Project-Sylos/Sylos-API/internal/auth"
	"github.com/Project-Sylos/Sylos-API/internal/corebridge"
	authroutes "github.com/Project-Sylos/Sylos-API/internal/routes/auth"
	healthroutes "github.com/Project-Sylos/Sylos-API/internal/routes/health"
	middlewarepkg "github.com/Project-Sylos/Sylos-API/internal/routes/middleware"
	migrationroutes "github.com/Project-Sylos/Sylos-API/internal/routes/migrations"
	serviceroutes "github.com/Project-Sylos/Sylos-API/internal/routes/services"
)

type Dependencies struct {
	Logger      zerolog.Logger
	CoreBridge  corebridge.Bridge
	AuthManager *auth.Manager
	Middleware  *middlewarepkg.Middleware
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

	healthroutes.RegisterPublic(router)
	authroutes.Register(router, deps.Logger, deps.AuthManager, mw)

	apiRouter := chi.NewRouter()
	apiRouter.Use(deps.AuthManager.Middleware)

	healthroutes.RegisterProtected(apiRouter)
	serviceroutes.Register(apiRouter, deps.Logger, deps.CoreBridge, mw)
	migrationroutes.Register(apiRouter, deps.Logger, deps.CoreBridge, mw)

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
