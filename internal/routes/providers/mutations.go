package providers

import (
	"net/http"

	"github.com/go-chi/chi/v5"

	"codeberg.org/Sylos/Sylos-API/internal/corebridge"
	"codeberg.org/Sylos/Sylos-API/internal/routes/middleware"
)

func (h handler) createFolder(ctx *middleware.Context, req corebridge.CreateBrowseFolderRequest) {
	providerID := chi.URLParam(ctx.Request(), "providerID")
	connectionID := chi.URLParam(ctx.Request(), "connectionID")
	if providerID == "" || connectionID == "" {
		ctx.Error(http.StatusBadRequest, "provider and connection id are required", nil)
		return
	}
	if req.ParentID == "" {
		ctx.Error(http.StatusBadRequest, "parentId is required", nil)
		return
	}
	if req.Name == "" {
		ctx.Error(http.StatusBadRequest, "name is required", nil)
		return
	}

	folder, err := h.mgr.CreateProviderFolder(ctx.Request().Context(), providerID, connectionID, req)
	if err != nil {
		if err == corebridge.ErrServiceNotFound {
			ctx.Error(http.StatusNotFound, "provider not found", err)
			return
		}
		ctx.Error(http.StatusBadRequest, "failed to create folder", err)
		return
	}

	ctx.Response(http.StatusCreated, folder)
}

func (h handler) deleteNodes(ctx *middleware.Context, req corebridge.DeleteBrowseNodesRequest) {
	providerID := chi.URLParam(ctx.Request(), "providerID")
	connectionID := chi.URLParam(ctx.Request(), "connectionID")
	if providerID == "" || connectionID == "" {
		ctx.Error(http.StatusBadRequest, "provider and connection id are required", nil)
		return
	}
	if len(req.Nodes) == 0 {
		ctx.Error(http.StatusBadRequest, "nodes are required", nil)
		return
	}
	if req.ContextID == "" {
		ctx.Error(http.StatusBadRequest, "contextId is required", nil)
		return
	}

	result, err := h.mgr.DeleteProviderNodes(ctx.Request().Context(), providerID, connectionID, req)
	if err != nil {
		if err == corebridge.ErrServiceNotFound {
			ctx.Error(http.StatusNotFound, "provider not found", err)
			return
		}
		ctx.Error(http.StatusBadRequest, "failed to delete nodes", err)
		return
	}

	status := http.StatusOK
	if len(result.Deleted) == 0 && len(result.Errors) > 0 {
		status = http.StatusBadRequest
	}
	ctx.Response(status, result)
}
