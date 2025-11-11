package logger

import (
	"os"
	"time"

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
