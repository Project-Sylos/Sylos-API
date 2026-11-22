// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package migrationops

import (
	"fmt"

	"codeberg.org/Sylos/Migration-Engine/pkg/migration"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge"
)

// PathRenameProgress returns live AcceptPathChange cascade progress.
func PathRenameProgress(mig *migration.Migration) (corebridge.PathRenameProgressResponse, error) {
	if mig == nil {
		return corebridge.PathRenameProgressResponse{}, fmt.Errorf("migration is nil")
	}
	p := mig.PathRenameProgressSnapshot()
	return corebridge.PathRenameProgressResponse{
		Active:      p.Active,
		Phase:       p.Phase,
		Done:        p.Done,
		Total:       p.Total,
		NodeID:      p.NodeID,
		Message:     p.Message,
		CanSoftStop: p.CanSoftStop,
		CanHardStop: p.CanHardStop,
	}, nil
}

// RequestPathRenameSoftStop asks an updating cascade to stop and revert.
func RequestPathRenameSoftStop(mig *migration.Migration) error {
	if mig == nil {
		return fmt.Errorf("migration is nil")
	}
	return mig.RequestPathRenameSoftStop()
}

// RequestPathRenameHardStop asks a reverting cascade to abort immediately.
func RequestPathRenameHardStop(mig *migration.Migration) error {
	if mig == nil {
		return fmt.Errorf("migration is nil")
	}
	return mig.RequestPathRenameHardStop()
}
