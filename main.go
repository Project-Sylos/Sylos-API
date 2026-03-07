package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"codeberg.org/Sylos/Sylos-API/internal/auth"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge/manager"
	"codeberg.org/Sylos/Sylos-API/internal/routes"
	"codeberg.org/Sylos/Sylos-API/internal/routes/middleware"
	"codeberg.org/Sylos/Sylos-API/internal/server"
	"codeberg.org/Sylos/Sylos-API/pkg/config"
	"codeberg.org/Sylos/Sylos-API/pkg/logger"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to load config: %v\n", err)
		os.Exit(1)
	}

	log := logger.New(cfg.Environment)
	log.Info().Msg("starting Sylos API server")

	if cfg.JWT.Generated {
		log.Warn().Msg("jwt.secret not configured; generated ephemeral secret for this runtime")
	}

	coreBridge, err := manager.NewManager(log, cfg)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to initialize core bridge")
	}
	authManager, err := auth.NewManager(auth.Config{
		Secret:         cfg.JWT.Secret,
		TTL:            cfg.JWT.AccessTokenTTL,
		AllowAnonymous: cfg.JWT.Generated,
	})
	if err != nil {
		log.Fatal().Err(err).Msg("failed to initialize auth manager")
	}

	apiMiddleware, err := middleware.New(log, filepath.Join(cfg.Runtime.DataDir, "api-runtime.log"))
	if err != nil {
		log.Warn().Err(err).Msg("failed to initialize API middleware logger; proceeding without runtime file log")
	}
	defer apiMiddleware.Close()

	router := routes.New(routes.Dependencies{
		Logger:      log,
		CoreBridge:  coreBridge,
		Manager:     coreBridge,
		AuthManager: authManager,
		Middleware:  apiMiddleware,
		DataDir:     cfg.Runtime.DataDir,
	})

	httpServer := server.New(server.Config{
		Address: fmt.Sprintf(":%d", cfg.HTTP.Port),
		Router:  router,
		Logger:  log,
	})

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	go func() {
		if err := httpServer.Start(); err != nil {
			log.Fatal().Err(err).Msg("server exited with error")
		}
	}()

	<-ctx.Done()
	log.Info().Msg("shutdown signal received")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := httpServer.Stop(shutdownCtx); err != nil {
		log.Error().Err(err).Msg("graceful shutdown failed")
	} else {
		log.Info().Msg("server stopped cleanly")
	}
}
