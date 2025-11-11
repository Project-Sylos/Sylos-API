package server

import (
	"context"
	"net/http"

	"github.com/rs/zerolog"
)

type Config struct {
	Address string
	Router  http.Handler
	Logger  zerolog.Logger
}

type Server struct {
	httpServer *http.Server
	logger     zerolog.Logger
}

func New(cfg Config) *Server {
	return &Server{
		httpServer: &http.Server{
			Addr:    cfg.Address,
			Handler: cfg.Router,
		},
		logger: cfg.Logger,
	}
}

func (s *Server) Start() error {
	s.logger.Info().Str("addr", s.httpServer.Addr).Msg("HTTP server listening")
	if err := s.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return err
	}
	return nil
}

func (s *Server) Stop(ctx context.Context) error {
	return s.httpServer.Shutdown(ctx)
}
