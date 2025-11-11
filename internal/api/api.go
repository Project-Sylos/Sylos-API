package api

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	chiMiddleware "github.com/go-chi/chi/v5/middleware"
	"github.com/rs/zerolog"

	"github.com/Project-Sylos/Sylos-API/internal/auth"
	"github.com/Project-Sylos/Sylos-API/internal/corebridge"
)

type Dependencies struct {
	Logger      zerolog.Logger
	CoreBridge  corebridge.Bridge
	AuthManager *auth.Manager
}

type API struct {
	deps Dependencies
}

func New(deps Dependencies) *API {
	return &API{deps: deps}
}

func (a *API) Router() chi.Router {
	r := chi.NewRouter()
	r.Use(chiMiddleware.RequestID)
	r.Use(chiMiddleware.RealIP)
	r.Use(chiMiddleware.Recoverer)
	r.Use(a.loggingMiddleware)

	r.Get("/health", a.handleHealth)
	r.Post("/api/auth/login", a.handleLogin)

	r.Route("/api", func(api chi.Router) {
		api.Use(a.deps.AuthManager.Middleware)

		api.Get("/health", a.handleHealth)
		api.Get("/source/list", a.handleListSources)
		api.Post("/migrate/start", a.handleStartMigration)
		api.Get("/migrate/status/{migrationID}", a.handleGetMigrationStatus)
	})

	return r
}

func (a *API) handleHealth(w http.ResponseWriter, r *http.Request) {
	a.respondJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (a *API) handleLogin(w http.ResponseWriter, r *http.Request) {
	var req LoginRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		a.respondError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	// TODO: integrate real authentication/authorization
	if req.Username == "" || req.Password == "" {
		a.respondError(w, http.StatusUnauthorized, "invalid credentials")
		return
	}

	token, err := a.deps.AuthManager.GenerateToken(req.Username, []string{"user"})
	if err != nil {
		a.deps.Logger.Error().Err(err).Msg("failed to generate token")
		a.respondError(w, http.StatusInternalServerError, "failed to generate token")
		return
	}

	a.respondJSON(w, http.StatusOK, LoginResponse{Token: token})
}

func (a *API) handleListSources(w http.ResponseWriter, r *http.Request) {
	sources, err := a.deps.CoreBridge.ListSources(r.Context())
	if err != nil {
		a.deps.Logger.Error().Err(err).Msg("failed to list sources")
		a.respondError(w, http.StatusInternalServerError, "failed to list sources")
		return
	}

	a.respondJSON(w, http.StatusOK, sources)
}

func (a *API) handleStartMigration(w http.ResponseWriter, r *http.Request) {
	var req StartMigrationRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		a.respondError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	migration, err := a.deps.CoreBridge.StartMigration(r.Context(), corebridge.StartMigrationRequest{
		SourceID: req.SourceID,
		Options:  req.Options,
	})
	if err != nil {
		a.deps.Logger.Error().Err(err).Msg("failed to start migration")
		a.respondError(w, http.StatusInternalServerError, "failed to start migration")
		return
	}

	a.respondJSON(w, http.StatusAccepted, migration)
}

func (a *API) handleGetMigrationStatus(w http.ResponseWriter, r *http.Request) {
	migrationID := chi.URLParam(r, "migrationID")
	if migrationID == "" {
		a.respondError(w, http.StatusBadRequest, "migration id is required")
		return
	}

	status, err := a.deps.CoreBridge.GetMigrationStatus(r.Context(), migrationID)
	if err != nil {
		if err == corebridge.ErrMigrationNotFound {
			a.respondError(w, http.StatusNotFound, "migration not found")
			return
		}

		a.deps.Logger.Error().Err(err).Msg("failed to get migration status")
		a.respondError(w, http.StatusInternalServerError, "failed to get migration status")
		return
	}

	a.respondJSON(w, http.StatusOK, status)
}

func (a *API) loggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		ww := chiMiddleware.NewWrapResponseWriter(w, r.ProtoMajor)

		next.ServeHTTP(ww, r)

		a.deps.Logger.Info().
			Str("method", r.Method).
			Str("path", r.URL.Path).
			Int("status", ww.Status()).
			Int("bytes", ww.BytesWritten()).
			Dur("duration", time.Since(start)).
			Msg("request complete")
	})
}

func (a *API) respondJSON(w http.ResponseWriter, status int, payload interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(payload); err != nil {
		a.deps.Logger.Error().Err(err).Msg("failed to write response")
	}
}

func (a *API) respondError(w http.ResponseWriter, status int, message string) {
	a.respondJSON(w, status, map[string]string{"error": message})
}

type LoginRequest struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

type LoginResponse struct {
	Token string `json:"token"`
}

type StartMigrationRequest struct {
	SourceID string                 `json:"sourceId"`
	Options  map[string]interface{} `json:"options,omitempty"`
}
