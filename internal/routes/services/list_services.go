package services

import (
	"net/http"

	appauth "codeberg.org/Sylos/Sylos-API/internal/auth"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge"
	"codeberg.org/Sylos/Sylos-API/internal/routes/middleware"
	"codeberg.org/Sylos/Sylos-API/internal/routes/preferences"
)

func (h handler) listServices(ctx *middleware.Context) {
	sources, err := h.core.ListSources(ctx.Request().Context())
	if err != nil {
		ctx.Error(http.StatusInternalServerError, "failed to list services", err)
		return
	}

	showSpectra := false
	if claims, ok := appauth.ClaimsFromContext(ctx.Request().Context()); ok && h.userStore != nil {
		raw, err := h.userStore.GetPreferencesJSON(claims.Subject)
		if err == nil {
			showSpectra = preferences.ShowSpectraServiceFromJSON(raw)
		}
	}

	if !showSpectra {
		sources = filterSpectraSources(sources)
	}

	ctx.Response(http.StatusOK, sources)
}

func filterSpectraSources(sources []corebridge.Source) []corebridge.Source {
	if len(sources) == 0 {
		return sources
	}
	out := make([]corebridge.Source, 0, len(sources))
	for _, s := range sources {
		if s.Type == corebridge.ServiceTypeSpectra {
			continue
		}
		out = append(out, s)
	}
	return out
}
