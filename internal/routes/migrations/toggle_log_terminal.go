package migrations

import (
	"errors"
	"net/http"

	"codeberg.org/Sylos/Sylos-API/internal/corebridge"
	"codeberg.org/Sylos/Sylos-API/internal/routes/middleware"
)

type toggleLogTerminalRequest struct {
	Enable     bool   `json:"enable"`
	LogAddress string `json:"logAddress,omitempty"`
}

func (h handler) toggleLogTerminal(ctx *middleware.Context, req toggleLogTerminalRequest) {
	err := h.mgr.ToggleLogTerminal(ctx.Request().Context(), req.Enable, req.LogAddress)
	if err != nil {
		status := http.StatusInternalServerError
		if errors.Is(err, corebridge.ErrServiceNotFound) || isUserInputError(err) {
			status = http.StatusBadRequest
		}
		ctx.Error(status, err.Error(), err)
		return
	}

	ctx.Response(http.StatusOK, map[string]any{
		"enabled": req.Enable,
		"message": "log terminal toggled successfully",
	})
}
