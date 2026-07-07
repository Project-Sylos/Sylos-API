package app

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"codeberg.org/Sylos/Sylos-API/internal/auth"
	"codeberg.org/Sylos/Sylos-API/internal/auth/jwtsecret"
	"codeberg.org/Sylos/Sylos-API/internal/auth/users"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge/apidb"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge/masterkey"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge/manager"
	"codeberg.org/Sylos/Sylos-API/internal/routes"
	"codeberg.org/Sylos/Sylos-API/internal/routes/middleware"
	"codeberg.org/Sylos/Sylos-API/internal/server"
	"codeberg.org/Sylos/Sylos-API/pkg/config"
	"codeberg.org/Sylos/Sylos-API/pkg/logger"
	"codeberg.org/Sylos/Sylos-API/pkg/oauthcreds"
)

type Options struct {
	Config        config.Config
	StaticHandler http.Handler
	UseEnvKeys    bool
	// Ready is closed once the HTTP listener is accepting connections (optional).
	Ready chan<- struct{}
}

func Run(ctx context.Context, opts Options) error {
	cfg := opts.Config

	log := logger.New(cfg.Environment)
	log.Info().Msg("starting Sylos API server")

	masterKey, err := masterkey.Resolve(masterkey.Config{
		DataDir:       cfg.Runtime.DataDir,
		OAuthCredsDir: cfg.Runtime.OAuthCredsDir,
		UseEnvKeys:    opts.UseEnvKeys,
	})
	if err != nil {
		return fmt.Errorf("failed to resolve install master key: %w", err)
	}

	apiDBPath := filepath.Join(cfg.Runtime.DataDir, "sylos.duckdb")
	if _, err := os.Stat(apiDBPath); os.IsNotExist(err) {
		log.Warn().Str("path", apiDBPath).Msg("sylos.duckdb not found; creating a new encrypted API database")
	}

	apiDB, err := apidb.Open(cfg.Runtime.DataDir, masterKey)
	if err != nil {
		return fmt.Errorf("failed to open API database: %w", err)
	}
	defer apiDB.Close()

	userStore, err := users.OpenConn(apiDB.SQL(), "", cfg.Auth.BcryptCost)
	if err != nil {
		return fmt.Errorf("failed to open user store: %w", err)
	}
	defer userStore.Close()

	userCount, err := userStore.Count()
	if err != nil {
		return fmt.Errorf("failed to count users: %w", err)
	}

	secret, generated, err := jwtsecret.LoadOrGenerate(apiDB.SQL(), cfg.JWT.Secret)
	if err != nil {
		return fmt.Errorf("failed to load jwt secret: %w", err)
	}
	cfg.JWT.Secret = secret
	if generated {
		log.Info().Msg("generated persistent jwt secret in API database")
	}

	allowAnonymous := userCount == 0
	if allowAnonymous {
		log.Info().Msg("setup mode: no users configured; API open until first admin is created")
	}

	coreBridge, err := manager.NewManager(log, cfg, apiDB)
	if err != nil {
		return fmt.Errorf("failed to initialize core bridge: %w", err)
	}

	oauthCfg, err := loadOAuthConfigFromDB(apiDB)
	if err != nil {
		log.Warn().Err(err).Msg("failed to load oauth apps from API database")
		oauthCfg = oauthcreds.Config{}
	} else if oauthCfg.GoogleDrive == nil && oauthCfg.Dropbox == nil {
		log.Info().Msg("no provider oauth apps configured; cloud sign-in requires developer app credentials")
	} else {
		log.Info().Msg("loaded provider oauth apps from API database")
	}
	coreBridge.SetOAuthCreds(oauthCfg)

	authManager, err := auth.NewManager(auth.Config{
		Secret:         cfg.JWT.Secret,
		TTL:            cfg.JWT.AccessTokenTTL,
		AllowAnonymous: allowAnonymous,
	})
	if err != nil {
		return fmt.Errorf("failed to initialize auth manager: %w", err)
	}

	apiMiddleware, err := middleware.New(log, filepath.Join(cfg.Runtime.DataDir, "api-runtime.log"))
	if err != nil {
		log.Warn().Err(err).Msg("failed to initialize API middleware logger; proceeding without runtime file log")
	}
	if apiMiddleware != nil {
		defer apiMiddleware.Close()
	}

	router := routes.New(routes.Dependencies{
		Logger:      log,
		CoreBridge:  coreBridge,
		Manager:     coreBridge,
		AuthManager: authManager,
		Middleware:  apiMiddleware,
		DataDir:     cfg.Runtime.DataDir,
		UserStore:   userStore,
	})

	if opts.StaticHandler != nil {
		router.Handle("/*", opts.StaticHandler)
	}

	httpServer := server.New(server.Config{
		Address: fmt.Sprintf(":%d", cfg.HTTP.Port),
		Router:  router,
		Logger:  log,
	})

	errCh := make(chan error, 1)
	go func() {
		if err := httpServer.StartWhenReady(opts.Ready); err != nil {
			errCh <- err
		}
	}()

	select {
	case err := <-errCh:
		return fmt.Errorf("server exited with error: %w", err)
	case <-ctx.Done():
		log.Info().Msg("shutdown signal received")
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := httpServer.Stop(shutdownCtx); err != nil {
		log.Error().Err(err).Msg("graceful shutdown failed")
		return err
	}

	log.Info().Msg("server stopped cleanly")
	return nil
}

func loadOAuthConfigFromDB(db *apidb.DB) (oauthcreds.Config, error) {
	apps, err := db.ListProviderOAuthApps()
	if err != nil {
		return oauthcreds.Config{}, err
	}
	var cfg oauthcreds.Config
	for _, app := range apps {
		creds := &oauthcreds.ProviderCredentials{
			ClientID:     app.ClientID,
			ClientSecret: app.ClientSecret,
		}
		switch app.ProviderID {
		case "google_drive":
			cfg.GoogleDrive = creds
		case "dropbox":
			cfg.Dropbox = creds
		}
	}
	return cfg, nil
}
