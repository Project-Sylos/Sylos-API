package auth

import (
	"context"
	"slices"
)

// UserCapabilities describes admin-only actions the current user may perform.
type UserCapabilities struct {
	CleanSlate bool `json:"cleanSlate"`
}

// CapabilitiesFromContext returns API capabilities derived from JWT roles.
func CapabilitiesFromContext(ctx context.Context) UserCapabilities {
	claims, ok := ClaimsFromContext(ctx)
	if !ok {
		return UserCapabilities{}
	}
	if slices.Contains(claims.Roles, "admin") {
		return UserCapabilities{CleanSlate: true}
	}
	return UserCapabilities{}
}
