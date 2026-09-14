package migrations

import (
	"errors"
	"net/http"
	"strings"

	"codeberg.org/Sylos/Migration-Engine/pkg/migration"
	"codeberg.org/Sylos/Sylos-API/internal/routes/middleware"
)

func writeReviewOpBusy(ctx *middleware.Context, err error) bool {
	if err == nil || !errors.Is(err, migration.ErrReviewOpBusy) {
		return false
	}
	msg := err.Error()
	if i := strings.Index(msg, ": "); i >= 0 && i+2 < len(msg) {
		msg = msg[i+2:]
	}
	ctx.Error(http.StatusConflict, msg, err)
	return true
}
