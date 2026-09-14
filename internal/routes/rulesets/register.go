package rulesets

import (
	"errors"
	"net/http"
	"strings"

	"github.com/go-chi/chi/v5"
	"github.com/rs/zerolog"

	"codeberg.org/Sylos/Migration-Engine/pkg/filter"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge/apidb"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge/manager"
	"codeberg.org/Sylos/Sylos-API/internal/routes/middleware"
)

type handler struct {
	logger zerolog.Logger
	mgr    *manager.Manager
}

func Register(router chi.Router, logger zerolog.Logger, mgr *manager.Manager, mw *middleware.Middleware) {
	h := handler{logger: logger, mgr: mgr}

	router.Get("/rulesets/prepackaged", middleware.NoBody(mw, h.listPrepackaged))
	router.Post("/rulesets/import", middleware.JSON(mw, h.importRuleset))
	router.Get("/rulesets", middleware.NoBody(mw, h.list))
	router.Post("/rulesets", middleware.JSON(mw, h.create))
	router.Get("/rulesets/{id}/export", middleware.NoBody(mw, h.export))
	router.Get("/rulesets/{id}", middleware.NoBody(mw, h.get))
	router.Put("/rulesets/{id}", middleware.JSON(mw, h.update))
	router.Delete("/rulesets/{id}", middleware.NoBody(mw, h.delete))
}

func (h handler) list(ctx *middleware.Context) {
	createdBy := strings.TrimSpace(ctx.Request().URL.Query().Get("createdBy"))
	items, err := h.mgr.ListRulesets(ctx.Request().Context(), createdBy)
	if err != nil {
		ctx.Error(http.StatusInternalServerError, "failed to list rulesets", err)
		return
	}
	ctx.Response(http.StatusOK, items)
}

func (h handler) listPrepackaged(ctx *middleware.Context) {
	items, err := h.mgr.ListRulesets(ctx.Request().Context(), "prepackaged")
	if err != nil {
		ctx.Error(http.StatusInternalServerError, "failed to list prepackaged rulesets", err)
		return
	}
	ctx.Response(http.StatusOK, items)
}

func (h handler) get(ctx *middleware.Context) {
	id := chi.URLParam(ctx.Request(), "id")
	item, err := h.mgr.GetRuleset(ctx.Request().Context(), id)
	if err != nil {
		status, msg := managerRulesetError(err)
		ctx.Error(status, msg, err)
		return
	}
	ctx.Response(http.StatusOK, item)
}

func (h handler) create(ctx *middleware.Context, req corebridge.CreateRulesetRequest) {
	item, err := h.mgr.CreateRuleset(ctx.Request().Context(), req)
	if err != nil {
		status, msg := managerRulesetError(err)
		ctx.Error(status, msg, err)
		return
	}
	ctx.Response(http.StatusCreated, item)
}

func (h handler) update(ctx *middleware.Context, req corebridge.UpdateRulesetRequest) {
	id := chi.URLParam(ctx.Request(), "id")
	item, err := h.mgr.UpdateRuleset(ctx.Request().Context(), id, req)
	if err != nil {
		status, msg := managerRulesetError(err)
		ctx.Error(status, msg, err)
		return
	}
	ctx.Response(http.StatusOK, item)
}

func (h handler) delete(ctx *middleware.Context) {
	id := chi.URLParam(ctx.Request(), "id")
	if err := h.mgr.DeleteRuleset(ctx.Request().Context(), id); err != nil {
		status, msg := managerRulesetError(err)
		ctx.Error(status, msg, err)
		return
	}
	ctx.Response(http.StatusNoContent, nil)
}

func (h handler) importRuleset(ctx *middleware.Context, rs filter.Ruleset) {
	item, err := h.mgr.ImportRuleset(ctx.Request().Context(), rs)
	if err != nil {
		status, msg := managerRulesetError(err)
		ctx.Error(status, msg, err)
		return
	}
	ctx.Response(http.StatusCreated, item)
}

func (h handler) export(ctx *middleware.Context) {
	id := chi.URLParam(ctx.Request(), "id")
	rs, err := h.mgr.ExportRuleset(ctx.Request().Context(), id)
	if err != nil {
		status, msg := managerRulesetError(err)
		ctx.Error(status, msg, err)
		return
	}
	ctx.Response(http.StatusOK, rs)
}

func managerRulesetError(err error) (int, string) {
	if errors.Is(err, apidb.ErrNotFound) {
		return http.StatusNotFound, "ruleset not found"
	}
	if strings.Contains(err.Error(), "prepackaged") {
		return http.StatusForbidden, err.Error()
	}
	if strings.Contains(err.Error(), "invalid ruleset") {
		return http.StatusBadRequest, err.Error()
	}
	return http.StatusInternalServerError, "ruleset operation failed"
}
