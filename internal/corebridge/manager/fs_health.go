package manager

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/google/uuid"

	"codeberg.org/Sylos/Migration-Engine/pkg/migration"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge/apidb"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge/roots"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge/services"
	"codeberg.org/Sylos/Sylos-FS/pkg/credentials"
	fstypes "codeberg.org/Sylos/Sylos-FS/pkg/types"
)

const fsHealthFreshness = 10 * time.Minute

// FSHealthFailedError carries a completed health report when the probe fails.
type FSHealthFailedError struct {
	Report corebridge.FSHealthReport
}

func (e *FSHealthFailedError) Error() string {
	if e == nil {
		return "filesystem health check failed"
	}
	return "filesystem health check failed"
}

// ProbeMigrationFSHealth lists each requested bound root and runs a create/delete
// permission probe. On full success it stamps lastFsHealthAt on the migration registry.
func (m *Manager) ProbeMigrationFSHealth(ctx context.Context, migrationID, side string) (corebridge.FSHealthReport, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	side = normalizeFSHealthSide(side)
	if side == "" {
		side = corebridge.FSHealthSideBoth
	}
	checkedAt := time.Now().UTC()
	report := corebridge.FSHealthReport{
		CheckedAt: checkedAt,
		Side:      side,
		Sides:     map[string]*corebridge.FSHealthSideReport{},
	}

	if err := m.ensureFSAdaptersRehydrated(migrationID); err != nil {
		sideRep := &corebridge.FSHealthSideReport{
			OK:             false,
			Classification: classifyHealthErr(err),
			Steps: map[string]corebridge.FSHealthStepResult{
				corebridge.FSHealthStepRehydrate: stepFromErr(err),
			},
		}
		if side == corebridge.FSHealthSideSource || side == corebridge.FSHealthSideBoth {
			report.Source = sideRep
			report.Sides["source"] = sideRep
		}
		if side == corebridge.FSHealthSideDestination || side == corebridge.FSHealthSideBoth {
			dst := *sideRep
			report.Destination = &dst
			report.Sides["destination"] = &dst
		}
		m.applyAuthAttentionFromHealth(migrationID, &report)
		return report, nil
	}

	plan := m.rootsMgr.GetPlan(migrationID)
	if plan == nil {
		return report, fmt.Errorf("roots not configured for migration %s", migrationID)
	}

	wantSrc := side == corebridge.FSHealthSideSource || side == corebridge.FSHealthSideBoth
	wantDst := side == corebridge.FSHealthSideDestination || side == corebridge.FSHealthSideBoth

	if wantSrc {
		report.Source = m.probeFSHealthSide(ctx, plan, migration.FSCredentialRoleSource)
		report.Sides["source"] = report.Source
	}
	if wantDst {
		report.Destination = m.probeFSHealthSide(ctx, plan, migration.FSCredentialRoleDestination)
		report.Sides["destination"] = report.Destination
	}

	report.OK = true
	if report.Source != nil && !report.Source.OK {
		report.OK = false
	}
	if report.Destination != nil && !report.Destination.OK {
		report.OK = false
	}

	if report.OK {
		m.stampFSHealthSuccess(migrationID, side, checkedAt)
	} else {
		m.applyAuthAttentionFromHealth(migrationID, &report)
	}
	return report, nil
}

// EnsureFSHealthFresh runs ProbeMigrationFSHealth for required sides when the
// last successful stamp is missing, partial, or older than fsHealthFreshness.
// Returns nil, nil when already fresh. On a failed probe returns the report and *FSHealthFailedError.
func (m *Manager) EnsureFSHealthFresh(ctx context.Context, migrationID string) (*corebridge.FSHealthReport, error) {
	required := m.requiredFSHealthSides(migrationID)
	if m.fsHealthIsFresh(migrationID, required) {
		return nil, nil
	}
	report, err := m.ProbeMigrationFSHealth(ctx, migrationID, required)
	if err != nil {
		return &report, err
	}
	if !report.OK {
		return &report, &FSHealthFailedError{Report: report}
	}
	return &report, nil
}

func (m *Manager) fsHealthIsFresh(migrationID, requiredSide string) bool {
	rec, err := m.getMigrationRecord(migrationID)
	if err != nil || !rec.LastFsHealthOK || rec.LastFsHealthAt.IsZero() {
		return false
	}
	if time.Since(rec.LastFsHealthAt) > fsHealthFreshness {
		return false
	}
	return healthSidesCover(rec.LastFsHealthSides, requiredSide)
}

func (m *Manager) requiredFSHealthSides(migrationID string) string {
	plan := m.rootsMgr.GetPlan(migrationID)
	if plan == nil {
		return corebridge.FSHealthSideBoth
	}
	hasSrc := plan.HasSource
	hasDst := plan.HasDestination
	switch {
	case hasSrc && hasDst:
		return corebridge.FSHealthSideBoth
	case hasSrc:
		return corebridge.FSHealthSideSource
	case hasDst:
		return corebridge.FSHealthSideDestination
	default:
		return corebridge.FSHealthSideBoth
	}
}

func healthSidesCover(have, need string) bool {
	have = normalizeFSHealthSide(have)
	need = normalizeFSHealthSide(need)
	if have == "" || need == "" {
		return false
	}
	if have == corebridge.FSHealthSideBoth {
		return true
	}
	return have == need
}

func normalizeFSHealthSide(side string) string {
	switch strings.ToLower(strings.TrimSpace(side)) {
	case "", "both", "all":
		return corebridge.FSHealthSideBoth
	case "src", "source":
		return corebridge.FSHealthSideSource
	case "dst", "dest", "destination":
		return corebridge.FSHealthSideDestination
	default:
		return ""
	}
}

func (m *Manager) stampFSHealthSuccess(migrationID, side string, at time.Time) {
	rec, err := m.getMigrationRecord(migrationID)
	if err != nil {
		return
	}
	rec.ID = migrationID
	rec.LastFsHealthAt = at.UTC()
	rec.LastFsHealthSides = normalizeFSHealthSide(side)
	rec.LastFsHealthOK = true
	if err := m.upsertMigrationRecord(rec); err != nil {
		m.logger.Warn().Err(err).Str("migration_id", migrationID).Msg("stamp fs health success")
	}
}

func (m *Manager) applyAuthAttentionFromHealth(migrationID string, report *corebridge.FSHealthReport) {
	if report == nil || m.apiDB == nil {
		return
	}
	auth := false
	for _, side := range []*corebridge.FSHealthSideReport{report.Source, report.Destination} {
		if side == nil {
			continue
		}
		if side.Classification == corebridge.FSHealthClassAuthFailed {
			auth = true
			break
		}
	}
	if !auth {
		return
	}
	rec, err := m.getMigrationRecord(migrationID)
	if err != nil {
		return
	}
	rec.ID = migrationID
	rec.NeedsAttention = true
	rec.AttentionReasons = appendUniqueReason(rec.AttentionReasons, apidb.AttentionCredentialsInvalid)
	_ = m.upsertMigrationRecord(rec)
}

func appendUniqueReason(reasons []string, reason string) []string {
	for _, r := range reasons {
		if r == reason {
			return reasons
		}
	}
	return append(reasons, reason)
}

func (m *Manager) probeFSHealthSide(ctx context.Context, plan *roots.RootPlan, role string) *corebridge.FSHealthSideReport {
	rep := &corebridge.FSHealthSideReport{
		Steps: map[string]corebridge.FSHealthStepResult{},
	}
	var adapter fstypes.FSAdapter
	var root fstypes.Folder
	var def services.ServiceDefinition
	var connID string
	switch role {
	case migration.FSCredentialRoleSource:
		if plan == nil || !plan.HasSource || plan.SourceAdapter == nil {
			rep.Classification = corebridge.FSHealthClassNotConfigured
			rep.Steps[corebridge.FSHealthStepRehydrate] = corebridge.FSHealthStepResult{
				OK:    false,
				Error: "source root not configured",
			}
			return rep
		}
		adapter = plan.SourceAdapter
		root = plan.SourceRoot
		def = plan.SourceDefinition
		connID = plan.SourceConnectionID
	case migration.FSCredentialRoleDestination:
		if plan == nil || !plan.HasDestination || plan.DestinationAdapter == nil {
			rep.Classification = corebridge.FSHealthClassNotConfigured
			rep.Steps[corebridge.FSHealthStepRehydrate] = corebridge.FSHealthStepResult{
				OK:    false,
				Error: "destination root not configured",
			}
			return rep
		}
		adapter = plan.DestinationAdapter
		root = plan.DestinationRoot
		def = plan.DestinationDefinition
		connID = plan.DestinationConnectionID
	default:
		rep.Classification = corebridge.FSHealthClassNotConfigured
		return rep
	}

	rep.ServiceID = def.ID
	rep.ServiceType = string(def.Type)
	rep.ConnectionID = connID
	rep.RootID = root.ServiceID
	rep.Steps[corebridge.FSHealthStepRehydrate] = corebridge.FSHealthStepResult{OK: true}

	if root.ServiceID == "" {
		rep.Classification = corebridge.FSHealthClassAccessFailed
		rep.Steps[corebridge.FSHealthStepListRoot] = corebridge.FSHealthStepResult{
			OK:    false,
			Error: "bound root id is empty",
		}
		return rep
	}

	_, listErr := adapter.ListChildren(ctx, root.ServiceID, nil, root.LocationPath)
	rep.Steps[corebridge.FSHealthStepListRoot] = stepFromErr(listErr)
	if listErr != nil {
		rep.Classification = classifyHealthErr(listErr)
		return rep
	}

	probeName := ".sylos-fs-health-" + uuid.New().String()
	folder, folderErr := adapter.CreateFolder(ctx, root.ServiceID, probeName, nil)
	rep.Steps[corebridge.FSHealthStepCreateFolder] = stepFromErr(folderErr)
	if folderErr != nil {
		rep.Classification = classifyHealthErr(folderErr)
		return rep
	}

	fileName := "probe.txt"
	file, fileErr := adapter.CreateFile(ctx, folder.ServiceID, fileName, 0, nil)
	if fileErr == nil {
		wc, openErr := adapter.OpenWrite(ctx, file.ServiceID)
		if openErr != nil {
			fileErr = openErr
		} else if wc != nil {
			closeErr := wc.Close()
			if closeErr != nil && !errors.Is(closeErr, io.EOF) {
				fileErr = closeErr
			}
		}
	}
	rep.Steps[corebridge.FSHealthStepCreateFile] = stepFromErr(fileErr)
	if fileErr != nil {
		rep.Classification = classifyHealthErr(fileErr)
		_ = adapter.DeleteNode(ctx, folder.ServiceID, fstypes.NodeTypeFolder)
		return rep
	}

	delFileErr := adapter.DeleteNode(ctx, file.ServiceID, fstypes.NodeTypeFile)
	delFolderErr := adapter.DeleteNode(ctx, folder.ServiceID, fstypes.NodeTypeFolder)
	if delFileErr != nil || delFolderErr != nil {
		err := delFileErr
		if err == nil {
			err = delFolderErr
		}
		rep.Steps[corebridge.FSHealthStepDelete] = stepFromErr(err)
		rep.Classification = classifyHealthErr(err)
		return rep
	}
	rep.Steps[corebridge.FSHealthStepDelete] = corebridge.FSHealthStepResult{OK: true}
	rep.Classification = corebridge.FSHealthClassOK
	rep.OK = true
	return rep
}

func stepFromErr(err error) corebridge.FSHealthStepResult {
	if err == nil {
		return corebridge.FSHealthStepResult{OK: true}
	}
	return corebridge.FSHealthStepResult{
		OK:         false,
		Error:      err.Error(),
		HTTPStatus: httpStatusFromErr(err),
		ErrorCode:  classifyHealthErr(err),
	}
}

func classifyHealthErr(err error) string {
	if err == nil {
		return corebridge.FSHealthClassOK
	}
	if errors.Is(err, ErrMigrationNeedsAttention) {
		return corebridge.FSHealthClassAuthFailed
	}
	if errors.Is(err, credentials.ErrNeedsRefresh) {
		return corebridge.FSHealthClassAuthFailed
	}
	var rl *credentials.RateLimitedError
	if errors.As(err, &rl) {
		return corebridge.FSHealthClassTransient
	}
	msg := strings.ToLower(err.Error())
	switch {
	case strings.Contains(msg, "unauthorized") ||
		strings.Contains(msg, "invalid_grant") ||
		strings.Contains(msg, "invalid token") ||
		(strings.Contains(msg, "auth") && strings.Contains(msg, "fail")):
		return corebridge.FSHealthClassAuthFailed
	case strings.Contains(msg, "permission") ||
		strings.Contains(msg, "access is denied") ||
		strings.Contains(msg, "forbidden") ||
		errors.Is(err, os.ErrPermission):
		return corebridge.FSHealthClassPermissionDenied
	case strings.Contains(msg, "timeout") ||
		strings.Contains(msg, "temporar") ||
		strings.Contains(msg, "unavailable") ||
		strings.Contains(msg, "connection reset"):
		return corebridge.FSHealthClassTransient
	default:
		return corebridge.FSHealthClassAccessFailed
	}
}

func httpStatusFromErr(err error) int {
	for err != nil {
		switch e := err.(type) {
		case interface{ StatusCode() int }:
			if code := e.StatusCode(); code != 0 {
				return code
			}
		case interface{ Status() int }:
			if code := e.Status(); code != 0 {
				return code
			}
		}
		u, ok := err.(interface{ Unwrap() error })
		if !ok {
			return 0
		}
		err = u.Unwrap()
	}
	return 0
}

// sweepResponseFromFSHealth builds a SweepResponse for resume/retry when health fails.
func sweepResponseFromFSHealth(report corebridge.FSHealthReport) corebridge.SweepResponse {
	return corebridge.SweepResponse{
		Success:  false,
		Error:    "filesystem health check failed",
		Message:  "Reconnect or fix filesystem access, then retry.",
		FsHealth: &report,
	}
}
