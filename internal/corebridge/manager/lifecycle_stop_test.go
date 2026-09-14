// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package manager

import (
	"testing"

	"codeberg.org/Sylos/Migration-Engine/pkg/migration"
)

func TestMigrationStopAllowed(t *testing.T) {
	cases := []struct {
		phase string
		live  bool
		want  bool
	}{
		{migration.PhaseTraversing, true, true},
		{migration.PhaseCopying, true, true},
		{migration.PhaseDeleting, true, true},
		{migration.PhaseTraversing, false, false},
		{migration.PhaseDeleting, false, false},
		{migration.PhaseTraversalSuspended, true, false},
		{migration.PhaseDeleteSuspended, true, false},
		{migration.PhaseAborted, true, false},
		{migration.PhaseCopyReview, true, false},
	}
	for _, tc := range cases {
		got := migrationStopAllowed(tc.phase, tc.live)
		if got != tc.want {
			t.Fatalf("migrationStopAllowed(%q, %v) = %v, want %v", tc.phase, tc.live, got, tc.want)
		}
	}
}
