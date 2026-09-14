// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package migrationops

import (
	"fmt"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
	"codeberg.org/Sylos/Migration-Engine/pkg/migration"
)

// DBOpsFromMigration returns seal telemetry and recent DB op timing samples.
func DBOpsFromMigration(mig *migration.Migration, opts db.DBOpsReportOptions) (db.DBOpsReport, error) {
	if mig == nil {
		return db.DBOpsReport{}, fmt.Errorf("migration is nil")
	}
	return mig.GetDBOpsReport(opts)
}
