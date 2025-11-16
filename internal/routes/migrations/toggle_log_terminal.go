package migrations

import (
	"errors"
	"net/http"

	"github.com/Project-Sylos/Sylos-API/internal/corebridge"
	"github.com/Project-Sylos/Sylos-API/internal/routes/middleware"
)

type toggleLogTerminalRequest struct {
	Enable     bool   `json:"enable"`
	LogAddress string `json:"logAddress,omitempty"`
}

func (h handler) toggleLogTerminal(ctx *middleware.Context, req toggleLogTerminalRequest) {
	err := h.core.ToggleLogTerminal(ctx.Request().Context(), req.Enable, req.LogAddress)
	if err != nil {
		status := http.StatusInternalServerError
		if errors.Is(err, corebridge.ErrServiceNotFound) || isUserInputError(err) {
			status = http.StatusBadRequest
		}
		ctx.Error(status, err.Error(), err)
		return
	}

	ctx.Response(http.StatusOK, map[string]interface{}{
		"enabled": req.Enable,
		"message": "log terminal toggled successfully",
	})
}
