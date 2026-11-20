package migrationops

import (
	"errors"
	"fmt"

	"codeberg.org/Sylos/Migration-Engine/pkg/migration"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge"
	"codeberg.org/Sylos/go-path-linter/pkg/issue"
)

// ListPathIssues loads the naming-compatibility queue, including ignored warnings.
// Count is the number of active (non-ignored) issues that still block copy.
// Rematches pending proposals that collide with existing DST siblings so Merge
// appears even when DST compare missed the collision mark.
func ListPathIssues(mig *migration.Migration, limit int) (corebridge.PathIssuesListResponse, error) {
	if mig == nil {
		return corebridge.PathIssuesListResponse{}, fmt.Errorf("migration is nil")
	}
	enabled := mig.PathChecksEnabled()
	if !enabled {
		return corebridge.PathIssuesListResponse{PathChecksEnabled: false}, nil
	}
	if _, err := mig.RematchPendingGPLCollisions(); err != nil {
		return corebridge.PathIssuesListResponse{}, err
	}
	rows, err := mig.ListPathIssues(limit)
	if err != nil {
		return corebridge.PathIssuesListResponse{}, err
	}
	issues := pathIssueRowsToAPI(rows)
	return corebridge.PathIssuesListResponse{
		Issues:            issues,
		Count:             len(migration.ActivePathIssues(rows)),
		PathChecksEnabled: true,
	}, nil
}

// ValidatePathProposal dry-runs a proposed basename.
func ValidatePathProposal(mig *migration.Migration, nodeID, proposedPath string) (corebridge.ValidatePathProposalResponse, error) {
	if mig == nil {
		return corebridge.ValidatePathProposalResponse{}, fmt.Errorf("migration is nil")
	}
	enabled := mig.PathChecksEnabled()
	res, err := mig.ValidatePathProposal(nodeID, proposedPath)
	if err != nil {
		return corebridge.ValidatePathProposalResponse{}, err
	}
	return corebridge.ValidatePathProposalResponse{
		Valid:             res.Valid,
		Messages:          gplIssuesToAPIMessages(res.Issues, res.Messages),
		PathChecksEnabled: enabled,
	}, nil
}

// AcceptPathChange accepts a suggested name (force=false) or applies a manual remap (force=true).
// When force is false and proposedPath is empty, the pending suggestion for the node is used.
// Force skips validation, marks the subtree ignored, and skips the GPL sweep.
func AcceptPathChange(mig *migration.Migration, nodeID, proposedPath string, force bool) (corebridge.PathIssuesMutationResponse, error) {
	if mig == nil {
		return corebridge.PathIssuesMutationResponse{}, fmt.Errorf("migration is nil")
	}
	if !mig.PathChecksEnabled() {
		return corebridge.PathIssuesMutationResponse{
			Success:           false,
			Message:           migration.PathChecksNotApplicableMessage,
			PathChecksEnabled: false,
		}, nil
	}
	if force {
		if proposedPath == "" {
			return corebridge.PathIssuesMutationResponse{Success: false, Message: "A name is required.", PathChecksEnabled: true}, nil
		}
		defer mig.EndPathRenameProgress()
		if err := mig.AcceptPathChange(nodeID, proposedPath, true); err != nil {
			return pathValidationOrErr(err)
		}
		return refreshPathIssuesResponse(mig, 0, 1)
	}
	if proposedPath == "" {
		issues, err := mig.ListPathIssues(0)
		if err != nil {
			return corebridge.PathIssuesMutationResponse{}, err
		}
		for _, row := range issues {
			if row.NodeID == nodeID {
				proposedPath = row.ProposedPath
				break
			}
		}
	}
	if proposedPath == "" {
		return corebridge.PathIssuesMutationResponse{Success: false, Message: "No suggested name is available for this item.", PathChecksEnabled: true}, nil
	}
	defer mig.EndPathRenameProgress()
	if err := mig.AcceptPathChange(nodeID, proposedPath, false); err != nil {
		return pathValidationOrErr(err)
	}
	if err := mig.RunGPLSweepForNode(nodeID); err != nil {
		return corebridge.PathIssuesMutationResponse{}, err
	}
	_ = mig.RunDSTRenameSweep()
	mig.PathRenameBeginPhase(migration.PathRenamePhaseRefresh, "Refreshing review…", 0)
	return refreshPathIssuesResponse(mig, 1, 0)
}

// AcceptAllPathProposals accepts every active pending suggestion, sweeps once, returns the queue.
func AcceptAllPathProposals(mig *migration.Migration) (corebridge.PathIssuesMutationResponse, error) {
	if mig == nil {
		return corebridge.PathIssuesMutationResponse{}, fmt.Errorf("migration is nil")
	}
	if !mig.PathChecksEnabled() {
		return corebridge.PathIssuesMutationResponse{Success: true, PathChecksEnabled: false}, nil
	}
	defer mig.EndPathRenameProgress()
	n, err := mig.AcceptAllPathProposals()
	if err != nil {
		return pathValidationOrErr(err)
	}
	if err := mig.RunGPLSweep("/"); err != nil {
		return corebridge.PathIssuesMutationResponse{}, err
	}
	_ = mig.RunDSTRenameSweep()
	mig.PathRenameBeginPhase(migration.PathRenamePhaseRefresh, "Refreshing review…", 0)
	return refreshPathIssuesResponse(mig, n, 0)
}

// IgnorePathIssueSubtree dismisses warnings for one node and its contents.
// Manual-review (blocking) issues are refused.
func IgnorePathIssueSubtree(mig *migration.Migration, nodeID string) (corebridge.PathIssuesMutationResponse, error) {
	if mig == nil {
		return corebridge.PathIssuesMutationResponse{}, fmt.Errorf("migration is nil")
	}
	if err := mig.SetGPLSubtreeIgnored(nodeID, true); err != nil {
		return corebridge.PathIssuesMutationResponse{
			Success: false, Message: err.Error(), PathChecksEnabled: mig.PathChecksEnabled(),
		}, nil
	}
	return refreshPathIssuesResponse(mig, 0, 1)
}

// UnignorePathIssueSubtree restores previously dismissed warnings for one node and its contents.
func UnignorePathIssueSubtree(mig *migration.Migration, nodeID string) (corebridge.PathIssuesMutationResponse, error) {
	if mig == nil {
		return corebridge.PathIssuesMutationResponse{}, fmt.Errorf("migration is nil")
	}
	if err := mig.SetGPLSubtreeIgnored(nodeID, false); err != nil {
		return corebridge.PathIssuesMutationResponse{}, err
	}
	return refreshPathIssuesResponse(mig, 0, 0)
}

// ResetPathRemap clears an accepted destination rename and re-evaluates naming warnings.
func ResetPathRemap(mig *migration.Migration, nodeID string) (corebridge.PathIssuesMutationResponse, error) {
	if mig == nil {
		return corebridge.PathIssuesMutationResponse{}, fmt.Errorf("migration is nil")
	}
	if !mig.PathChecksEnabled() {
		return corebridge.PathIssuesMutationResponse{Success: true, PathChecksEnabled: false}, nil
	}
	if err := mig.ResetPathRemap(nodeID); err != nil {
		return corebridge.PathIssuesMutationResponse{}, err
	}
	return refreshPathIssuesResponse(mig, 0, 0)
}

// IgnoreRemainingPathIssues dismisses all active destination-name warnings.
func IgnoreRemainingPathIssues(mig *migration.Migration) (corebridge.PathIssuesMutationResponse, error) {
	if mig == nil {
		return corebridge.PathIssuesMutationResponse{}, fmt.Errorf("migration is nil")
	}
	n, err := mig.IgnoreAllPathIssues()
	if err != nil {
		return corebridge.PathIssuesMutationResponse{}, err
	}
	return refreshPathIssuesResponse(mig, 0, n)
}

// EnsurePathIssuesClearForCopy runs a sweep and blocks copy when active issues remain.
func EnsurePathIssuesClearForCopy(mig *migration.Migration) error {
	if mig == nil {
		return fmt.Errorf("migration is nil")
	}
	if !mig.PathChecksEnabled() {
		return nil
	}
	if err := mig.RunGPLSweep("/"); err != nil {
		return err
	}
	rows, err := mig.ListPathIssues(0)
	if err != nil {
		return err
	}
	active := migration.ActivePathIssues(rows)
	if len(active) == 0 {
		return nil
	}
	issues := pathIssueRowsToAPI(active)
	return &corebridge.PathIssuesRemainingError{Count: len(issues), Issues: issues}
}

func refreshPathIssuesResponse(mig *migration.Migration, accepted, ignored int) (corebridge.PathIssuesMutationResponse, error) {
	list, err := ListPathIssues(mig, 0)
	if err != nil {
		return corebridge.PathIssuesMutationResponse{}, err
	}
	return corebridge.PathIssuesMutationResponse{
		Success:           true,
		Accepted:          accepted,
		Ignored:           ignored,
		Issues:            list.Issues,
		Count:             list.Count,
		PathChecksEnabled: list.PathChecksEnabled,
	}, nil
}

func pathValidationOrErr(err error) (corebridge.PathIssuesMutationResponse, error) {
	var pve *migration.PathValidationError
	if errors.As(err, &pve) {
		msg := "This destination name isn’t allowed."
		if len(pve.Messages) > 0 {
			msg = pve.Messages[0]
		} else if pve.Message != "" {
			msg = pve.Message
		}
		return corebridge.PathIssuesMutationResponse{
			Success: false,
			Message: msg,
		}, nil
	}
	if errors.Is(err, migration.ErrPathRenameSoftStopped) {
		return corebridge.PathIssuesMutationResponse{
			Success: false,
			Message: "Rename cancelled and reverted.",
		}, nil
	}
	if errors.Is(err, migration.ErrPathRenameHardStopped) {
		return corebridge.PathIssuesMutationResponse{
			Success: false,
			Message: "Rename hard-stopped; some related paths may be inconsistent.",
		}, nil
	}
	return corebridge.PathIssuesMutationResponse{}, err
}

func pathIssueRowsToAPI(rows []migration.PathIssueRow) []corebridge.PathIssue {
	out := make([]corebridge.PathIssue, 0, len(rows))
	for _, row := range rows {
		summary := "Suggested rename"
		if row.Status == "collision" {
			summary = "Name already used in this folder"
		}
		if len(row.Messages) > 0 && row.Messages[0].Message != "" {
			summary = row.Messages[0].Message
		}
		msgs := make([]corebridge.PathIssueMessage, 0, len(row.Messages))
		for _, m := range row.Messages {
			msgs = append(msgs, corebridge.PathIssueMessage{
				Category: m.Category,
				Message:  m.Message,
				Detail:   m.Detail,
				DocsURL:  m.DocsURL,
			})
		}
		out = append(out, corebridge.PathIssue{
			NodeID:           row.NodeID,
			Path:             row.Path,
			Name:             row.Name,
			ProposedPath:     row.ProposedPath,
			Status:           row.Status,
			Category:         row.Category,
			Summary:          summary,
			EventTime:        row.EventTime,
			Messages:         msgs,
			Ignored:          row.Ignored,
			DstAction:        row.DstAction,
			CollisionSibling:    row.CollisionSibling,
			SuggestedUniqueName: row.SuggestedUniqueName,
		})
	}
	return out
}

// PreviewPathResolutionCascade dry-runs nested collisions and file overwrite winners.
func PreviewPathResolutionCascade(mig *migration.Migration, nodeID, action, proposedName string) (corebridge.PathResolutionCascadePreview, error) {
	if mig == nil {
		return corebridge.PathResolutionCascadePreview{}, fmt.Errorf("migration is nil")
	}
	if !mig.PathChecksEnabled() {
		return corebridge.PathResolutionCascadePreview{
			Success: false, Message: migration.PathChecksNotApplicableMessage, PathChecksEnabled: false,
		}, nil
	}
	prev, err := mig.PreviewPathResolutionCascade(nodeID, action, proposedName)
	if err != nil {
		return corebridge.PathResolutionCascadePreview{
			Success: false, Message: err.Error(), PathChecksEnabled: true, Action: action, SrcFolderID: nodeID,
		}, nil
	}
	return toAPICascadePreview(prev), nil
}

func toAPICascadePreview(p migration.PathResolutionCascadePreview) corebridge.PathResolutionCascadePreview {
	out := corebridge.PathResolutionCascadePreview{
		Success:              true,
		Action:               p.Action,
		SrcFolderID:          p.SrcFolderID,
		DstFolderID:          p.DstFolderID,
		FolderCollisionCount: p.FolderCollisionCount,
		FileOverwriteCount:   p.FileOverwriteCount,
		DstSubtreeListed:     p.DstSubtreeListed,
		PathChecksEnabled:    p.PathChecksEnabled,
	}
	if len(p.FolderCollisions) > 0 {
		out.FolderCollisions = make([]corebridge.PathCascadeFolderCollision, len(p.FolderCollisions))
		for i, c := range p.FolderCollisions {
			out.FolderCollisions[i] = corebridge.PathCascadeFolderCollision{
				SrcID: c.SrcID, SrcName: c.SrcName, SrcPath: c.SrcPath,
				DstID: c.DstID, DstName: c.DstName, DstPath: c.DstPath,
			}
		}
	}
	if len(p.FileOverwrites) > 0 {
		out.FileOverwrites = make([]corebridge.PathCascadeFileOverwrite, len(p.FileOverwrites))
		for i, f := range p.FileOverwrites {
			out.FileOverwrites[i] = corebridge.PathCascadeFileOverwrite{
				SrcID: f.SrcID, SrcName: f.SrcName, SrcPath: f.SrcPath,
				SrcSize: f.SrcSize, SrcMTime: f.SrcMTime,
				DstID: f.DstID, DstName: f.DstName, DstPath: f.DstPath,
				DstSize: f.DstSize, DstMTime: f.DstMTime,
				DefaultWinner: f.DefaultWinner,
			}
		}
	}
	return out
}

// AcceptUniquePathName allocates a unique destination basename and accepts it.
// proposedName, when set, is validated and used instead of auto-allocating.
func AcceptUniquePathName(mig *migration.Migration, nodeID, proposedName string) (corebridge.PathIssuesMutationResponse, error) {
	if mig == nil {
		return corebridge.PathIssuesMutationResponse{}, fmt.Errorf("migration is nil")
	}
	if !mig.PathChecksEnabled() {
		return corebridge.PathIssuesMutationResponse{
			Success: false, Message: migration.PathChecksNotApplicableMessage, PathChecksEnabled: false,
		}, nil
	}
	defer mig.EndPathRenameProgress()
	if _, err := mig.AcceptUniquePathName(nodeID, proposedName); err != nil {
		return pathValidationOrErr(err)
	}
	if err := mig.RunGPLSweepForNode(nodeID); err != nil {
		return corebridge.PathIssuesMutationResponse{}, err
	}
	_ = mig.RunDSTRenameSweep()
	mig.PathRenameBeginPhase(migration.PathRenamePhaseRefresh, "Refreshing review…", 0)
	return refreshPathIssuesResponse(mig, 1, 0)
}

func stringsToMessages(msgs []string) []corebridge.PathIssueMessage {
	if len(msgs) == 0 {
		return nil
	}
	out := make([]corebridge.PathIssueMessage, 0, len(msgs))
	for _, m := range msgs {
		out = append(out, corebridge.PathIssueMessage{Message: m})
	}
	return out
}

func gplIssuesToAPIMessages(issues []issue.Issue, fallback []string) []corebridge.PathIssueMessage {
	if len(issues) == 0 {
		return stringsToMessages(fallback)
	}
	out := make([]corebridge.PathIssueMessage, 0, len(issues))
	seen := make(map[string]struct{}, len(issues))
	for _, iss := range issues {
		msg := migration.FriendlyPathIssueMessage(iss)
		if msg == "" {
			continue
		}
		key := msg + "\x00" + iss.DocsURL + "\x00" + iss.Detail
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, corebridge.PathIssueMessage{
			Category: string(iss.Category),
			Message:  msg,
			Detail:   iss.Detail,
			DocsURL:  iss.DocsURL,
		})
	}
	if len(out) == 0 {
		return stringsToMessages(fallback)
	}
	return out
}
