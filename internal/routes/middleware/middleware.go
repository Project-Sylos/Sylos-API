package middleware

import (
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/rs/zerolog"
)

// Middleware provides helpers for request binding, structured responses, and runtime logging.
type Middleware struct {
	logger       zerolog.Logger
	runtimeLog   *log.Logger
	runtimeClose io.Closer
}

// Context carries request/response metadata for handlers wrapped by the middleware.
type Context struct {
	mw     *Middleware
	w      http.ResponseWriter
	r      *http.Request
	wrote  bool
	status int
	err    error
}

// New constructs a middleware instance that logs to the provided runtime log path.
// If the path is empty or cannot be opened, logging falls back to stdout.
func New(logger zerolog.Logger, runtimeLogPath string) (*Middleware, error) {
	var (
		writer io.Writer = os.Stdout
		closer io.Closer
	)

	if runtimeLogPath != "" {
		if err := os.MkdirAll(filepath.Dir(runtimeLogPath), 0o755); err != nil {
			return nil, err
		}

		file, err := os.OpenFile(runtimeLogPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
		if err != nil {
			logger.Warn().Err(err).Msg("failed to open runtime log file; falling back to stdout")
		} else {
			writer = file
			closer = file
		}
	}

	return &Middleware{
		logger:       logger,
		runtimeLog:   log.New(writer, "", log.LstdFlags|log.LUTC),
		runtimeClose: closer,
	}, nil
}

// Close closes any file handles owned by the middleware.
func (mw *Middleware) Close() error {
	if mw.runtimeClose != nil {
		return mw.runtimeClose.Close()
	}
	return nil
}

// JSON wraps a handler expecting a JSON payload of type T.
func JSON[T any](mw *Middleware, handler func(*Context, T)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		ctx := newContext(mw, w, r)

		var payload T
		if err := decodeJSON(r, &payload); err != nil {
			ctx.Error(http.StatusBadRequest, "invalid request body", err)
			mw.logRequest(ctx, time.Since(start))
			return
		}

		handler(ctx, payload)
		if !ctx.wrote {
			ctx.Response(http.StatusOK, map[string]string{"status": "ok"})
		}

		mw.logRequest(ctx, time.Since(start))
	}
}

// NoBody wraps a handler that does not expect a request body.
func NoBody(mw *Middleware, handler func(*Context)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		ctx := newContext(mw, w, r)
		handler(ctx)
		if !ctx.wrote {
			ctx.Response(http.StatusOK, map[string]string{"status": "ok"})
		}
		mw.logRequest(ctx, time.Since(start))
	}
}

// Request returns the underlying http.Request.
func (c *Context) Request() *http.Request {
	return c.r
}

// Response writes a JSON response with the provided status code and payload.
func (c *Context) Response(status int, payload interface{}) {
	if c.wrote {
		return
	}
	if status == 0 {
		status = http.StatusOK
	}

	if err := writeJSON(c.w, status, payload); err != nil {
		c.err = err
		http.Error(c.w, "failed to encode response", http.StatusInternalServerError)
		c.status = http.StatusInternalServerError
	} else {
		c.status = status
	}
	c.wrote = true
}

// Error writes a JSON error response and records the underlying error for logging.
func (c *Context) Error(status int, message string, err error) {
	if c.wrote {
		return
	}
	if status == 0 {
		status = http.StatusInternalServerError
	}
	if err != nil {
		c.err = err
	} else {
		c.err = errors.New(message)
	}

	if status >= http.StatusInternalServerError {
		c.mw.logger.Error().Err(err).Str("path", c.r.URL.Path).Msg(message)
	} else {
		c.mw.logger.Warn().Err(err).Str("path", c.r.URL.Path).Msg(message)
	}

	c.Response(status, map[string]string{"error": message})
}

// MultipartForm wraps a handler expecting a multipart form upload.
func MultipartForm(mw *Middleware, handler func(*Context)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		ctx := newContext(mw, w, r)
		handler(ctx)
		if !ctx.wrote {
			ctx.Response(http.StatusOK, map[string]string{"status": "ok"})
		}
		mw.logRequest(ctx, time.Since(start))
	}
}

// Logger exposes the zerolog logger.
func (c *Context) Logger() zerolog.Logger {
	return c.mw.logger
}

func newContext(mw *Middleware, w http.ResponseWriter, r *http.Request) *Context {
	return &Context{
		mw:     mw,
		w:      w,
		r:      r,
		status: http.StatusOK,
	}
}

func decodeJSON(r *http.Request, dst interface{}) error {
	if r.Body == nil {
		return errors.New("request body required")
	}
	defer r.Body.Close()

	if contentType := r.Header.Get("Content-Type"); contentType != "" && !strings.Contains(contentType, "application/json") {
		return errors.New("content type must be application/json")
	}

	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()

	if err := dec.Decode(dst); err != nil {
		return err
	}

	if dec.More() {
		return errors.New("request body must contain a single JSON object")
	}

	return nil
}

func writeJSON(w http.ResponseWriter, status int, payload interface{}) error {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	return json.NewEncoder(w).Encode(payload)
}

func (mw *Middleware) logRequest(ctx *Context, duration time.Duration) {
	if mw.runtimeLog == nil {
		return
	}
	errMsg := ""
	if ctx.err != nil {
		errMsg = ctx.err.Error()
	}
	mw.runtimeLog.Printf("%s %s %d %s error=%s",
		ctx.r.Method,
		ctx.r.URL.Path,
		ctx.status,
		duration.Truncate(time.Millisecond),
		errMsg,
	)
}
