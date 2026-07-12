package migrationaccess

import (
	"context"
	"fmt"
	"path/filepath"

	"codeberg.org/Sylos/Migration-Engine/pkg/migration"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge/apidb"
)

// Opener resolves per-migration keys from the API database and opens plaintext migration DuckDBs.
// Per-migration keys are stored encrypted in sylos.duckdb (wrapped by the install master key from the OS keyring).
// Token encryption inside migration DBs uses the decrypted per-migration key via the engine.
type Opener struct {
	APIDB   *apidb.DB
	Engine  *migration.MigrationManager
	DataDir string
}

// OpenMigrationDB loads the per-migration key and opens the migration database.
func (o *Opener) OpenMigrationDB(ctx context.Context, migrationID, userID string) (*migration.Migration, error) {
	_ = ctx
	_ = userID
	if o.APIDB == nil {
		return nil, fmt.Errorf("API database not configured")
	}

	tokenKey, err := o.APIDB.EnsureMigrationKey(migrationID)
	if err != nil {
		return nil, fmt.Errorf("migration key for %q: %w", migrationID, err)
	}

	migrationDir, err := filepath.Abs(filepath.Join(o.DataDir, migrationID))
	if err != nil {
		return nil, err
	}

	mig, err := o.Engine.GetMigration(migrationID, migrationDir, tokenKey)
	if err != nil {
		return nil, err
	}
	if mig == nil {
		return nil, corebridge.ErrMigrationNotFound
	}
	return mig, nil
}
