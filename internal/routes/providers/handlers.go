package providers

import (
	"net/http"
	"strconv"

	"github.com/go-chi/chi/v5"
	"github.com/rs/zerolog"

	"codeberg.org/Sylos/Sylos-API/internal/corebridge"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge/manager"
	"codeberg.org/Sylos/Sylos-API/internal/routes/middleware"
)

type handler struct {
	logger zerolog.Logger
	mgr    *manager.Manager
}

func Register(router chi.Router, logger zerolog.Logger, mgr *manager.Manager, mw *middleware.Middleware) {
	h := handler{logger: logger, mgr: mgr}
	router.Get("/providers", middleware.NoBody(mw, h.listProviders))
	router.Post("/providers/{providerID}/connections", middleware.JSON(mw, h.createConnection))
	router.Post("/providers/{providerID}/connections/{connectionID}/tokens", middleware.JSON(mw, h.postTokens))
	router.Get("/providers/{providerID}/connections/{connectionID}/status", middleware.NoBody(mw, h.connectionStatus))
	router.Delete("/providers/{providerID}/connections/{connectionID}", middleware.NoBody(mw, h.revokeConnection))
	router.Get("/providers/{providerID}/connections/{connectionID}/roots", middleware.NoBody(mw, h.listRoots))
	router.Get("/providers/{providerID}/connections/{connectionID}/children", middleware.NoBody(mw, h.listChildren))
}

func (h handler) listProviders(ctx *middleware.Context) {
	items, err := h.mgr.ListProviders(ctx.Request().Context())
	if err != nil {
		ctx.Error(http.StatusInternalServerError, "failed to list providers", err)
		return
	}
	ctx.Response(http.StatusOK, items)
}

func (h handler) createConnection(ctx *middleware.Context, req corebridge.CreateConnectionRequest) {
	providerID := chi.URLParam(ctx.Request(), "providerID")
	resp, err := h.mgr.CreateProviderConnection(ctx.Request().Context(), providerID, req.MigrationID)
	if err != nil {
		ctx.Error(http.StatusBadRequest, "failed to create connection", err)
		return
	}
	ctx.Response(http.StatusCreated, resp)
}

func (h handler) postTokens(ctx *middleware.Context, req corebridge.OAuthTokenRequest) {
	providerID := chi.URLParam(ctx.Request(), "providerID")
	connectionID := chi.URLParam(ctx.Request(), "connectionID")
	status, err := h.mgr.PostProviderTokens(ctx.Request().Context(), providerID, connectionID, req)
	if err != nil {
		ctx.Error(http.StatusBadRequest, "failed to store tokens", err)
		return
	}
	ctx.Response(http.StatusOK, status)
}

func (h handler) connectionStatus(ctx *middleware.Context) {
	providerID := chi.URLParam(ctx.Request(), "providerID")
	connectionID := chi.URLParam(ctx.Request(), "connectionID")
	status, err := h.mgr.ProviderConnectionStatus(ctx.Request().Context(), providerID, connectionID)
	if err != nil {
		ctx.Error(http.StatusInternalServerError, "failed to get connection status", err)
		return
	}
	ctx.Response(http.StatusOK, status)
}

func (h handler) revokeConnection(ctx *middleware.Context) {
	providerID := chi.URLParam(ctx.Request(), "providerID")
	connectionID := chi.URLParam(ctx.Request(), "connectionID")
	if err := h.mgr.RevokeProviderConnection(ctx.Request().Context(), providerID, connectionID); err != nil {
		ctx.Error(http.StatusInternalServerError, "failed to revoke connection", err)
		return
	}
	ctx.Response(http.StatusNoContent, nil)
}

func (h handler) listRoots(ctx *middleware.Context) {
	providerID := chi.URLParam(ctx.Request(), "providerID")
	connectionID := chi.URLParam(ctx.Request(), "connectionID")
	roots, err := h.mgr.ListProviderRoots(ctx.Request().Context(), providerID, connectionID)
	if err != nil {
		ctx.Error(http.StatusInternalServerError, "failed to list roots", err)
		return
	}
	ctx.Response(http.StatusOK, roots)
}

func (h handler) listChildren(ctx *middleware.Context) {
	providerID := chi.URLParam(ctx.Request(), "providerID")
	connectionID := chi.URLParam(ctx.Request(), "connectionID")
	q := ctx.Request().URL.Query()
	identifier := q.Get("identifier")
	rootType := q.Get("rootType")
	offset, _ := strconv.Atoi(q.Get("offset"))
	limit, _ := strconv.Atoi(q.Get("limit"))
	foldersOnly := q.Get("foldersOnly") == "true" || q.Get("folders_only") == "true"
	resp, err := h.mgr.ListProviderChildren(ctx.Request().Context(), providerID, connectionID, identifier, rootType, offset, limit, foldersOnly)
	if err != nil {
		ctx.Error(http.StatusInternalServerError, "failed to list children", err)
		return
	}
	ctx.Response(http.StatusOK, resp)
}
