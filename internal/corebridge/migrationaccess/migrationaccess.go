package migrationaccess

import (
	"context"
	"fmt"
	"path/filepath"

	"codeberg.org/Sylos/Migration-Engine/pkg/migration"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge/apidb"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge/database"
)

// Opener resolves per-migration encryption keys and opens migration DuckDBs through the engine.
type Opener struct {
	APIDB     *apidb.DB
	Engine    *migration.MigrationManager
	DataDir   string
}

// AuthorizeMigrationAccess is a stub RBAC hook; allow-all until roles are defined.
func AuthorizeMigrationAccess(_ string, _ string) error {
	return nil
}

// OpenMigrationDB looks up the migration key, opens the encrypted migration DB, and returns the engine migration.
func (o *Opener) OpenMigrationDB(ctx context.Context, migrationID, userID string) (*migration.Migration, error) {
	_ = ctx
	if err := AuthorizeMigrationAccess(userID, migrationID); err != nil {
		return nil, err
	}
	if o.APIDB == nil {
		return nil, fmt.Errorf("API database not configured")
	}
	key, err := o.APIDB.EnsureMigrationKey(migrationID)
	if err != nil {
		return nil, fmt.Errorf("migration key for %q: %w", migrationID, err)
	}
	migrationDir, err := filepath.Abs(database.GetMigrationDir(o.DataDir, migrationID))
	if err != nil {
		return nil, err
	}
	mig, err := o.Engine.GetMigration(migrationID, migrationDir, key)
	if err != nil {
		return nil, err
	}
	if mig == nil {
		return nil, corebridge.ErrMigrationNotFound
	}
	return mig, nil
}

// MigrationKey returns the per-migration encryption key, creating one if needed.
func (o *Opener) MigrationKey(migrationID string) ([]byte, error) {
	return o.APIDB.EnsureMigrationKey(migrationID)
}
