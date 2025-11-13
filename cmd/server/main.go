package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Project-Sylos/Sylos-API/internal/auth"
	"github.com/Project-Sylos/Sylos-API/internal/corebridge"
	"github.com/Project-Sylos/Sylos-API/internal/routes"
	"github.com/Project-Sylos/Sylos-API/internal/server"
	"github.com/Project-Sylos/Sylos-API/pkg/config"
	"github.com/Project-Sylos/Sylos-API/pkg/logger"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to load config: %v\n", err)
		os.Exit(1)
	}

	log := logger.New(cfg.Environment)
	log.Info().Msg("starting Sylos API server")

	coreBridge, err := corebridge.NewManager(log, cfg)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to initialize core bridge")
	}
	authManager, err := auth.NewManager(auth.Config{
		Secret: cfg.JWT.Secret,
		TTL:    cfg.JWT.AccessTokenTTL,
	})
	if err != nil {
		log.Fatal().Err(err).Msg("failed to initialize auth manager")
	}

	router := routes.New(routes.Dependencies{
		Logger:      log,
		CoreBridge:  coreBridge,
		AuthManager: authManager,
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
