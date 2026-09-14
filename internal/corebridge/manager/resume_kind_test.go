package manager

import (
	"testing"

	"codeberg.org/Sylos/Migration-Engine/pkg/migration"
)

func TestResumeKindTraversalSuspendedIsStartTraversal(t *testing.T) {
	cases := []struct {
		phase string
		want  string
	}{
		{migration.PhaseTraversalSuspended, "start-traversal"},
		{migration.PhaseTraversalReview, "retry-sweep"},
		{migration.PhaseCopySuspended, "start-copy"},
		{migration.PhaseCopyReview, "copy-retry"},
		{migration.PhaseDeleteSuspended, "start-delete"},
		{migration.PhaseDeleteReview, "delete-retry"},
		{migration.PhaseTraversalFinalizeFailed, "retry-finalize"},
		{migration.PhaseCreated, "retry-sweep"},
	}
	for _, tc := range cases {
		if got := resumeKind(tc.phase); got != tc.want {
			t.Fatalf("resumeKind(%s)=%s want %s", tc.phase, got, tc.want)
		}
	}
}
