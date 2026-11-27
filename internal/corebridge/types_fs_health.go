package corebridge

import "time"

// FS health step names and classifications used by ProbeMigrationFSHealth.
const (
	FSHealthSideSource      = "src"
	FSHealthSideDestination = "dst"
	FSHealthSideBoth        = "both"

	FSHealthStepRehydrate = "rehydrate"
	FSHealthStepListRoot  = "listRoot"

	FSHealthClassOK               = "ok"
	FSHealthClassAuthFailed       = "auth_failed"
	FSHealthClassPermissionDenied = "permission_denied"
	FSHealthClassAccessFailed     = "access_failed"
	FSHealthClassTransient        = "transient"
	FSHealthClassNotConfigured    = "not_configured"
)

// FSHealthStepResult is one probe step for a side.
type FSHealthStepResult struct {
	OK         bool   `json:"ok"`
	HTTPStatus int    `json:"httpStatus,omitempty"`
	Error      string `json:"error,omitempty"`
	ErrorCode  string `json:"errorCode,omitempty"`
}

// FSHealthSideReport is the probe outcome for source or destination.
type FSHealthSideReport struct {
	OK             bool                          `json:"ok"`
	ServiceID      string                        `json:"serviceId,omitempty"`
	ServiceType    string                        `json:"serviceType,omitempty"`
	ConnectionID   string                        `json:"connectionId,omitempty"`
	RootID         string                        `json:"rootId,omitempty"`
	Steps          map[string]FSHealthStepResult `json:"steps,omitempty"`
	Classification string                        `json:"classification,omitempty"`
}

// FSHealthReport is returned by POST /migrations/{id}/fs-health and embedded on
// resume/start failures when the freshness gate re-runs the probe.
type FSHealthReport struct {
	OK          bool                `json:"ok"`
	CheckedAt   time.Time           `json:"checkedAt"`
	Side        string              `json:"side,omitempty"`
	Source      *FSHealthSideReport `json:"source,omitempty"`
	Destination *FSHealthSideReport `json:"destination,omitempty"`
	// Sides mirrors source/destination for clients that prefer a map keyed by role.
	Sides map[string]*FSHealthSideReport `json:"sides,omitempty"`
}
