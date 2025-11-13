package auth

import (
	"encoding/json"
	"net/http"
)

type loginRequest struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

type loginResponse struct {
	Token string `json:"token"`
}

func (h handler) handleLogin(w http.ResponseWriter, r *http.Request) {
	var req loginRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.respondError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	if req.Username == "" || req.Password == "" {
		h.respondError(w, http.StatusUnauthorized, "invalid credentials")
		return
	}

	token, err := h.manager.GenerateToken(req.Username, []string{"user"})
	if err != nil {
		h.logger.Error().Err(err).Msg("failed to generate token")
		h.respondError(w, http.StatusInternalServerError, "failed to generate token")
		return
	}

	h.respondJSON(w, http.StatusOK, loginResponse{Token: token})
}
