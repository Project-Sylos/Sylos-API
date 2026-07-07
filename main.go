package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"codeberg.org/Sylos/Sylos-API/pkg/app"
	"codeberg.org/Sylos/Sylos-API/pkg/config"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to load config: %v\n", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := app.Run(ctx, app.Options{Config: cfg}); err != nil {
		fmt.Fprintf(os.Stderr, "server error: %v\n", err)
		os.Exit(1)
	}
}
