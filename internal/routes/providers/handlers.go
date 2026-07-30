package providers

import (
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/rs/zerolog"

	"codeberg.org/Sylos/Sylos-API/internal/corebridge"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge/manager"
	"codeberg.org/Sylos/Sylos-API/internal/routes/httputil"
	"codeberg.org/Sylos/Sylos-API/internal/routes/middleware"
)

type handler struct {
	logger zerolog.Logger
	mgr    *manager.Manager
}

func Register(router chi.Router, logger zerolog.Logger, mgr *manager.Manager, mw *middleware.Middleware) {
	h := handler{logger: logger, mgr: mgr}
	router.Get("/providers", middleware.NoBody(mw, h.listProviders))
	router.Get("/providers/sftp/saved-hosts", middleware.NoBody(mw, h.listSFTPSavedHosts))
	router.Post("/providers/sftp/saved-hosts", middleware.JSON(mw, h.upsertSFTPSavedHost))
	router.Get("/providers/sftp/saved-hosts/{hostID}", middleware.NoBody(mw, h.getSFTPSavedHost))
	router.Delete("/providers/sftp/saved-hosts/{hostID}", middleware.NoBody(mw, h.deleteSFTPSavedHost))
	router.Post("/providers/{providerID}/connections", middleware.JSON(mw, h.createConnection))
	router.Post("/providers/{providerID}/connections/{connectionID}/oauth/exchange", middleware.JSON(mw, h.exchangeOAuth))
	router.Post("/providers/{providerID}/connections/{connectionID}/tokens", middleware.JSON(mw, h.postTokens))
	router.Post("/providers/{providerID}/connections/{connectionID}/credentials", middleware.JSON(mw, h.postCredentials))
	router.Post("/providers/{providerID}/host-key/probe", middleware.JSON(mw, h.probeHostKey))
	router.Get("/providers/{providerID}/connections/{connectionID}/status", middleware.NoBody(mw, h.connectionStatus))
	router.Delete("/providers/{providerID}/connections/{connectionID}", middleware.NoBody(mw, h.revokeConnection))
	router.Get("/providers/{providerID}/connections/{connectionID}/roots", middleware.NoBody(mw, h.listRoots))
	router.Get("/providers/{providerID}/connections/{connectionID}/children", middleware.NoBody(mw, h.listChildren))
	router.Post("/providers/{providerID}/connections/{connectionID}/folders", middleware.JSON(mw, h.createFolder))
	router.Post("/providers/{providerID}/connections/{connectionID}/nodes/delete", middleware.JSON(mw, h.deleteNodes))
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
	if req.ClientSecret != "" {
		ctx.Error(http.StatusBadRequest, "direct token upload with client_secret is no longer supported; use oauth exchange", nil)
		return
	}
	providerID := chi.URLParam(ctx.Request(), "providerID")
	connectionID := chi.URLParam(ctx.Request(), "connectionID")
	status, err := h.mgr.PostProviderTokens(ctx.Request().Context(), providerID, connectionID, req)
	if err != nil {
		ctx.Error(http.StatusBadRequest, "failed to store tokens", err)
		return
	}
	ctx.Response(http.StatusOK, status)
}

func (h handler) probeHostKey(ctx *middleware.Context, req corebridge.SFTPHostKeyProbeRequest) {
	providerID := chi.URLParam(ctx.Request(), "providerID")
	resp, err := h.mgr.ProbeSFTPHostKey(ctx.Request().Context(), providerID, req)
	if err != nil {
		ctx.Error(http.StatusBadRequest, "failed to probe host key", err)
		return
	}
	ctx.Response(http.StatusOK, resp)
}

func (h handler) postCredentials(ctx *middleware.Context, req corebridge.SFTPCredentialsRequest) {
	providerID := chi.URLParam(ctx.Request(), "providerID")
	connectionID := chi.URLParam(ctx.Request(), "connectionID")
	status, err := h.mgr.PostProviderCredentials(ctx.Request().Context(), providerID, connectionID, req)
	if err != nil {
		ctx.Error(http.StatusBadRequest, "failed to store credentials", err)
		return
	}
	ctx.Response(http.StatusOK, status)
}

func (h handler) exchangeOAuth(ctx *middleware.Context, req corebridge.OAuthExchangeRequest) {
	providerID := chi.URLParam(ctx.Request(), "providerID")
	connectionID := chi.URLParam(ctx.Request(), "connectionID")
	status, err := h.mgr.ExchangeProviderOAuthCode(ctx.Request().Context(), providerID, connectionID, req)
	if err != nil {
		ctx.Error(http.StatusBadRequest, "failed to exchange oauth code", err)
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
	q := httputil.ParseListChildrenQuery(ctx.Request().URL.Query())
	resp, err := h.mgr.ListProviderChildren(ctx.Request().Context(), providerID, connectionID, q.Identifier, q.RootType, q.DriveID, q.Offset, q.Limit, q.FoldersOnly)
	if err != nil {
		ctx.Error(http.StatusInternalServerError, "failed to list children", err)
		return
	}
	ctx.Response(http.StatusOK, resp)
}

func (h handler) listSFTPSavedHosts(ctx *middleware.Context) {
	items, err := h.mgr.ListSFTPSavedHosts(ctx.Request().Context())
	if err != nil {
		ctx.Error(http.StatusInternalServerError, "failed to list saved SFTP hosts", err)
		return
	}
	ctx.Response(http.StatusOK, map[string]any{"items": items})
}

func (h handler) getSFTPSavedHost(ctx *middleware.Context) {
	hostID := chi.URLParam(ctx.Request(), "hostID")
	item, err := h.mgr.GetSFTPSavedHost(ctx.Request().Context(), hostID)
	if err != nil {
		ctx.Error(http.StatusNotFound, "saved SFTP host not found", err)
		return
	}
	ctx.Response(http.StatusOK, item)
}

func (h handler) upsertSFTPSavedHost(ctx *middleware.Context, req corebridge.SFTPSavedHostUpsertRequest) {
	item, err := h.mgr.UpsertSFTPSavedHost(ctx.Request().Context(), req)
	if err != nil {
		ctx.Error(http.StatusBadRequest, "failed to save SFTP host", err)
		return
	}
	ctx.Response(http.StatusOK, item)
}

func (h handler) deleteSFTPSavedHost(ctx *middleware.Context) {
	hostID := chi.URLParam(ctx.Request(), "hostID")
	if err := h.mgr.DeleteSFTPSavedHost(ctx.Request().Context(), hostID); err != nil {
		ctx.Error(http.StatusNotFound, "failed to delete saved SFTP host", err)
		return
	}
	ctx.Response(http.StatusNoContent, nil)
}
