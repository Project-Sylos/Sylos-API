package server

import (
	"context"
	"net"
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
	return s.serve(nil)
}

// StartWhenReady begins serving and closes ready once the TCP listener accepts connections.
func (s *Server) StartWhenReady(ready chan<- struct{}) error {
	return s.serve(ready)
}

func (s *Server) serve(ready chan<- struct{}) error {
	ln, err := net.Listen("tcp", s.httpServer.Addr)
	if err != nil {
		return err
	}
	if ready != nil {
		close(ready)
	}
	s.logger.Info().Str("addr", ln.Addr().String()).Msg("HTTP server listening")
	if err := s.httpServer.Serve(ln); err != nil && err != http.ErrServerClosed {
		return err
	}
	return nil
}

func (s *Server) Stop(ctx context.Context) error {
	return s.httpServer.Shutdown(ctx)
}
