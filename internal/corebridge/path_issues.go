package corebridge

import (
	"errors"
	"fmt"

	"codeberg.org/Sylos/Migration-Engine/pkg/migration"
	"codeberg.org/Sylos/go-path-linter/pkg/issue"
)

// Path issue API error codes (user-facing titles elsewhere; codes stay stable for clients).
const (
	ErrCodePathIssuesRemaining = "PATH_ISSUES_REMAINING"
	ErrCodePathValidation      = "PATH_VALIDATION_FAILED"
)

// PathIssue is one destination-name finding for review.
type PathIssue struct {
	NodeID       string             `json:"nodeId"`
	Path         string             `json:"path"`
	ProposedPath string             `json:"proposedPath"`
	Status       string             `json:"status"`
	Category     string             `json:"category,omitempty"`
	Summary      string             `json:"summary,omitempty"`
	EventTime    int64              `json:"eventTime,omitempty"`
	Messages     []PathIssueMessage `json:"messages,omitempty"`
	// Ignored is true when the warning is dismissed (original name kept).
	Ignored bool `json:"ignored,omitempty"`
}

// PathIssueMessage is a short user-facing validation finding.
type PathIssueMessage struct {
	Category string `json:"category,omitempty"`
	Message  string `json:"message"`
	// Detail is a GPL structured attribute (e.g. InvalidChar: forbidden runes found).
	Detail  string `json:"detail,omitempty"`
	DocsURL string `json:"docsURL,omitempty"`
}

// ValidatePathProposalRequest is the body for dry-run destination name checks.
type ValidatePathProposalRequest struct {
	NodeID       string `json:"nodeId"`
	ProposedPath string `json:"proposedPath"`
}

// ValidatePathProposalResponse is the dry-run result with friendly messages.
type ValidatePathProposalResponse struct {
	Valid             bool               `json:"valid"`
	Messages          []PathIssueMessage `json:"messages,omitempty"`
	PathChecksEnabled bool               `json:"pathChecksEnabled"`
}

// RemapPathRequest is the body for manual destination rename.
type RemapPathRequest struct {
	ProposedPath        string `json:"proposedPath"`
	ForceSkipValidation bool   `json:"forceSkipValidation,omitempty"`
}

// AcceptPathRequest is the body for accepting a suggested destination name.
type AcceptPathRequest struct {
	ProposedPath string `json:"proposedPath,omitempty"`
}

// PathIssuesListResponse is GET /path-issues.
type PathIssuesListResponse struct {
	Issues            []PathIssue `json:"issues"`
	Count             int         `json:"count"`
	PathChecksEnabled bool        `json:"pathChecksEnabled"`
}

// PathIssuesMutationResponse is returned after accept / remap / accept-all / ignore.
type PathIssuesMutationResponse struct {
	Success           bool        `json:"success"`
	Accepted          int         `json:"accepted,omitempty"`
	Ignored           int         `json:"ignored,omitempty"`
	Issues            []PathIssue `json:"issues,omitempty"`
	Count             int         `json:"count"`
	Message           string      `json:"message,omitempty"`
	PathChecksEnabled bool        `json:"pathChecksEnabled"`
}

// PathIssuesRemainingError is returned when Start Copy is blocked by active path issues.
type PathIssuesRemainingError struct {
	Count  int
	Issues []PathIssue
}

func (e *PathIssuesRemainingError) Error() string {
	if e == nil {
		return "some destination names still need attention"
	}
	return fmt.Sprintf("some destination names still need attention (%d remaining)", e.Count)
}

// ListPathIssues loads the destination-name queue, including ignored warnings.
// Count is the number of active (non-ignored) issues that still block copy.
func ListPathIssues(mig *migration.Migration, limit int) (PathIssuesListResponse, error) {
	if mig == nil {
		return PathIssuesListResponse{}, fmt.Errorf("migration is nil")
	}
	enabled := mig.PathChecksEnabled()
	if !enabled {
		return PathIssuesListResponse{PathChecksEnabled: false}, nil
	}
	rows, err := mig.ListPathIssues(limit)
	if err != nil {
		return PathIssuesListResponse{}, err
	}
	issues := pathIssueRowsToAPI(rows)
	return PathIssuesListResponse{
		Issues:            issues,
		Count:             len(migration.ActivePathIssues(rows)),
		PathChecksEnabled: true,
	}, nil
}

// ValidatePathProposal dry-runs a proposed basename.
func ValidatePathProposal(mig *migration.Migration, nodeID, proposedPath string) (ValidatePathProposalResponse, error) {
	if mig == nil {
		return ValidatePathProposalResponse{}, fmt.Errorf("migration is nil")
	}
	enabled := mig.PathChecksEnabled()
	res, err := mig.ValidatePathProposal(nodeID, proposedPath)
	if err != nil {
		return ValidatePathProposalResponse{}, err
	}
	return ValidatePathProposalResponse{
		Valid:             res.Valid,
		Messages:          gplIssuesToAPIMessages(res.Issues, res.Messages),
		PathChecksEnabled: enabled,
	}, nil
}

// AcceptPathProposal accepts a suggested name, sweeps, and returns the refreshed queue.
func AcceptPathProposal(mig *migration.Migration, nodeID, proposedPath string) (PathIssuesMutationResponse, error) {
	if mig == nil {
		return PathIssuesMutationResponse{}, fmt.Errorf("migration is nil")
	}
	if !mig.PathChecksEnabled() {
		return PathIssuesMutationResponse{
			Success:           false,
			Message:           migration.PathChecksNotApplicableMessage,
			PathChecksEnabled: false,
		}, nil
	}
	if proposedPath == "" {
		issues, err := mig.ListPathIssues(0)
		if err != nil {
			return PathIssuesMutationResponse{}, err
		}
		for _, row := range issues {
			if row.NodeID == nodeID {
				proposedPath = row.ProposedPath
				break
			}
		}
	}
	if proposedPath == "" {
		return PathIssuesMutationResponse{Success: false, Message: "No suggested destination name is available for this item.", PathChecksEnabled: true}, nil
	}
	if err := mig.AcceptPathProposal(nodeID, proposedPath); err != nil {
		return pathValidationOrErr(err)
	}
	if err := mig.RunGPLSweep(); err != nil {
		return PathIssuesMutationResponse{}, err
	}
	return refreshPathIssuesResponse(mig, 1, 0)
}

// RemapPathManual applies a manual destination name. Force skips validation and marks the subtree ignored (no sweep).
func RemapPathManual(mig *migration.Migration, nodeID, proposedPath string, force bool) (PathIssuesMutationResponse, error) {
	if mig == nil {
		return PathIssuesMutationResponse{}, fmt.Errorf("migration is nil")
	}
	if !mig.PathChecksEnabled() {
		return PathIssuesMutationResponse{
			Success:           false,
			Message:           migration.PathChecksNotApplicableMessage,
			PathChecksEnabled: false,
		}, nil
	}
	if proposedPath == "" {
		return PathIssuesMutationResponse{Success: false, Message: "A destination name is required.", PathChecksEnabled: true}, nil
	}
	if err := mig.RemapPathManual(nodeID, proposedPath, force); err != nil {
		return pathValidationOrErr(err)
	}
	if force {
		return refreshPathIssuesResponse(mig, 0, 1)
	}
	if err := mig.RunGPLSweep(); err != nil {
		return PathIssuesMutationResponse{}, err
	}
	return refreshPathIssuesResponse(mig, 1, 0)
}

// AcceptAllPathProposals accepts every active pending suggestion, sweeps once, returns the queue.
func AcceptAllPathProposals(mig *migration.Migration) (PathIssuesMutationResponse, error) {
	if mig == nil {
		return PathIssuesMutationResponse{}, fmt.Errorf("migration is nil")
	}
	if !mig.PathChecksEnabled() {
		return PathIssuesMutationResponse{Success: true, PathChecksEnabled: false}, nil
	}
	n, err := mig.AcceptAllPathProposals()
	if err != nil {
		return pathValidationOrErr(err)
	}
	if err := mig.RunGPLSweep(); err != nil {
		return PathIssuesMutationResponse{}, err
	}
	return refreshPathIssuesResponse(mig, n, 0)
}

// IgnorePathIssueSubtree dismisses warnings for one node and its contents.
func IgnorePathIssueSubtree(mig *migration.Migration, nodeID string) (PathIssuesMutationResponse, error) {
	if mig == nil {
		return PathIssuesMutationResponse{}, fmt.Errorf("migration is nil")
	}
	if err := mig.IgnoreGPLSubtree(nodeID); err != nil {
		return PathIssuesMutationResponse{}, err
	}
	return refreshPathIssuesResponse(mig, 0, 1)
}

// UnignorePathIssueSubtree restores previously dismissed warnings for one node and its contents.
func UnignorePathIssueSubtree(mig *migration.Migration, nodeID string) (PathIssuesMutationResponse, error) {
	if mig == nil {
		return PathIssuesMutationResponse{}, fmt.Errorf("migration is nil")
	}
	if err := mig.UnignoreGPLSubtree(nodeID); err != nil {
		return PathIssuesMutationResponse{}, err
	}
	return refreshPathIssuesResponse(mig, 0, 0)
}

// IgnoreRemainingPathIssues dismisses all active destination-name warnings.
func IgnoreRemainingPathIssues(mig *migration.Migration) (PathIssuesMutationResponse, error) {
	if mig == nil {
		return PathIssuesMutationResponse{}, fmt.Errorf("migration is nil")
	}
	n, err := mig.IgnoreAllPathIssues()
	if err != nil {
		return PathIssuesMutationResponse{}, err
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
	if err := mig.RunGPLSweep(); err != nil {
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
	return &PathIssuesRemainingError{Count: len(issues), Issues: issues}
}

func refreshPathIssuesResponse(mig *migration.Migration, accepted, ignored int) (PathIssuesMutationResponse, error) {
	list, err := ListPathIssues(mig, 0)
	if err != nil {
		return PathIssuesMutationResponse{}, err
	}
	return PathIssuesMutationResponse{
		Success:           true,
		Accepted:          accepted,
		Ignored:           ignored,
		Issues:            list.Issues,
		Count:             list.Count,
		PathChecksEnabled: list.PathChecksEnabled,
	}, nil
}

func pathValidationOrErr(err error) (PathIssuesMutationResponse, error) {
	var pve *migration.PathValidationError
	if errors.As(err, &pve) {
		msg := "This destination name isn’t allowed."
		if len(pve.Messages) > 0 {
			msg = pve.Messages[0]
		} else if pve.Message != "" {
			msg = pve.Message
		}
		return PathIssuesMutationResponse{
			Success: false,
			Message: msg,
		}, nil
	}
	return PathIssuesMutationResponse{}, err
}

func pathIssueRowsToAPI(rows []migration.PathIssueRow) []PathIssue {
	out := make([]PathIssue, 0, len(rows))
	for _, row := range rows {
		summary := "Suggested rename"
		if row.Status == "collision" {
			summary = "Name already used in this folder"
		}
		if len(row.Messages) > 0 && row.Messages[0].Message != "" {
			summary = row.Messages[0].Message
		}
		msgs := make([]PathIssueMessage, 0, len(row.Messages))
		for _, m := range row.Messages {
			msgs = append(msgs, PathIssueMessage{
				Category: m.Category,
				Message:  m.Message,
				Detail:   m.Detail,
				DocsURL:  m.DocsURL,
			})
		}
		out = append(out, PathIssue{
			NodeID:       row.NodeID,
			Path:         row.Path,
			ProposedPath: row.ProposedPath,
			Status:       row.Status,
			Category:     row.Category,
			Summary:      summary,
			EventTime:    row.EventTime,
			Messages:     msgs,
			Ignored:      row.Ignored,
		})
	}
	return out
}

func stringsToMessages(msgs []string) []PathIssueMessage {
	if len(msgs) == 0 {
		return nil
	}
	out := make([]PathIssueMessage, 0, len(msgs))
	for _, m := range msgs {
		out = append(out, PathIssueMessage{Message: m})
	}
	return out
}

func gplIssuesToAPIMessages(issues []issue.Issue, fallback []string) []PathIssueMessage {
	if len(issues) == 0 {
		return stringsToMessages(fallback)
	}
	out := make([]PathIssueMessage, 0, len(issues))
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
		out = append(out, PathIssueMessage{
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
