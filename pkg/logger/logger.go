package logger

import (
	"os"
	"time"

	"codeberg.org/Sylos/Sylos-API/internal/corebridge/apidb"
	"github.com/rs/zerolog"
)

func New(environment string) zerolog.Logger {
	output := zerolog.New(os.Stdout).With().Timestamp().Logger()

	if environment == "development" || environment == "" {
		output = output.Level(zerolog.DebugLevel).Output(zerolog.ConsoleWriter{
			Out:        os.Stdout,
			TimeFormat: time.RFC3339,
		})
	}

	return output
}

// PersistHook writes every emitted log line into sylos.api/.
type PersistHook struct {
	DB *apidb.DB
}

func (h PersistHook) Run(e *zerolog.Event, level zerolog.Level, msg string) {
	if h.DB == nil || level == zerolog.NoLevel {
		return
	}
	_ = h.DB.AppendAPILog(level.String(), msg, "api")
}

// WithAPIPersist returns a logger that also persists lines into sylos.api/.
func WithAPIPersist(log zerolog.Logger, db *apidb.DB) zerolog.Logger {
	if db == nil {
		return log
	}
	return log.Hook(PersistHook{DB: db})
}
