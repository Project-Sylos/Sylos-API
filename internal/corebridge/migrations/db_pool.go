package migrations

import (
	"fmt"
	"sync"

	"github.com/Project-Sylos/Migration-Engine/pkg/db"
	"github.com/Project-Sylos/Migration-Engine/pkg/migration"
	"github.com/rs/zerolog"
)

// DBPool manages database connections for migrations
// API is the sole owner of DB lifecycle - opens, maintains, and closes DBs
type DBPool struct {
	mu     sync.RWMutex
	dbs    map[string]*db.DB
	paths  map[string]string // Track DB path for each migration ID
	logger zerolog.Logger
}

// NewDBPool creates a new database connection pool
func NewDBPool(logger zerolog.Logger) *DBPool {
	return &DBPool{
		dbs:    make(map[string]*db.DB),
		paths:  make(map[string]string),
		logger: logger,
	}
}

// Get retrieves a database instance for the given migration ID
// Returns nil if the DB is not in the pool
func (p *DBPool) Get(migrationID string) *db.DB {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.dbs[migrationID]
}

// Open opens a database connection and adds it to the pool
// If the DB is already open, returns the existing instance
// API owns the DB lifecycle - this should be called when migration is created/resumed
func (p *DBPool) Open(migrationID, dbPath string) (*db.DB, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Check if already open
	if existingDB := p.dbs[migrationID]; existingDB != nil {
		p.logger.Debug().
			Str("migration_id", migrationID).
			Str("db_path", dbPath).
			Msg("DB already open for migration, returning existing instance")
		return existingDB, nil
	}

	// Open database connection using migration.SetupDatabase (as per migration engine docs)
	// This will open existing DB or create if needed, but we set RemoveExisting=false
	boltDB, _, err := migration.SetupDatabase(migration.DatabaseConfig{
		Path:           dbPath,
		RemoveExisting: false, // Never remove existing DB - API owns lifecycle
	})
	if err != nil {
		return nil, fmt.Errorf("failed to open database for migration %s: %w", migrationID, err)
	}

	// Store in pool
	p.dbs[migrationID] = boltDB
	p.paths[migrationID] = dbPath

	p.logger.Info().
		Str("migration_id", migrationID).
		Str("db_path", dbPath).
		Msg("opened database connection for migration")

	return boltDB, nil
}

// Close closes a database connection and removes it from the pool
// API decides when to close - only called when migration is fully done/archived
func (p *DBPool) Close(migrationID string) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	boltDB, exists := p.dbs[migrationID]
	if !exists {
		p.logger.Debug().
			Str("migration_id", migrationID).
			Msg("DB not in pool, already closed or never opened")
		return nil
	}

	// Remove from pool before closing (in case close fails)
	delete(p.dbs, migrationID)
	delete(p.paths, migrationID)

	err := boltDB.Close()
	if err != nil {
		p.logger.Error().
			Err(err).
			Str("migration_id", migrationID).
			Msg("failed to close database connection")
		return fmt.Errorf("failed to close database for migration %s: %w", migrationID, err)
	}

	p.logger.Info().
		Str("migration_id", migrationID).
		Msg("closed database connection for migration")

	return nil
}

// Has returns true if the migration ID has an open database connection
func (p *DBPool) Has(migrationID string) bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	_, exists := p.dbs[migrationID]
	return exists
}

// GetPath returns the database path for a migration ID
func (p *DBPool) GetPath(migrationID string) string {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.paths[migrationID]
}

// CloseAll closes all database connections in the pool
// Useful for cleanup/shutdown
func (p *DBPool) CloseAll() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	var firstErr error
	for migrationID, boltDB := range p.dbs {
		if err := boltDB.Close(); err != nil {
			p.logger.Error().
				Err(err).
				Str("migration_id", migrationID).
				Msg("failed to close database connection during pool shutdown")
			if firstErr == nil {
				firstErr = err
			}
		}
	}

	// Clear pool
	p.dbs = make(map[string]*db.DB)
	p.paths = make(map[string]string)

	p.logger.Info().Msg("closed all database connections in pool")

	return firstErr
}

// Count returns the number of open database connections
func (p *DBPool) Count() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return len(p.dbs)
}
