// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package manager

import (
	"testing"

	"codeberg.org/Sylos/Migration-Engine/pkg/migration"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge/apidb"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge/roots"
	"github.com/rs/zerolog"
)

func TestRegistryPhaseNeedsHeal(t *testing.T) {
	t.Parallel()
	if !registryPhaseNeedsHeal(migration.PhaseCopying) {
		t.Fatal("copy-in-progress")
	}
	if !registryPhaseNeedsHeal(migration.PhaseTraversalFinalizing) {
		t.Fatal("finalizing")
	}
	if !registryPhaseNeedsHeal(migration.PhaseAborted) {
		t.Fatal("aborted")
	}
	if registryPhaseNeedsHeal(migration.PhaseCopySuspended) {
		t.Fatal("suspended must not heal")
	}
	if registryPhaseNeedsHeal(migration.PhaseCopyReview) {
		t.Fatal("review must not heal")
	}
}

func TestColdMigrationStatusIncludesUnclean(t *testing.T) {
	t.Parallel()
	mgr := &Manager{
		rootsMgr: roots.NewManager(zerolog.Nop(), t.TempDir(), nil, nil),
	}
	st := mgr.coldMigrationStatus(apidb.MigrationRecord{
		ID:               "cold-unclean",
		Name:             "cold-unclean",
		Phase:            migration.PhaseCopySuspended,
		UncleanInterrupt: true,
	})
	if !st.UncleanInterrupt {
		t.Fatal("expected uncleanInterrupt on cold status")
	}
	if st.Status != migration.PhaseCopySuspended {
		t.Fatalf("status=%s", st.Status)
	}
}
