package migrations

import (
	"database/sql"
	"fmt"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
	corebridgeDB "codeberg.org/Sylos/Sylos-API/internal/corebridge/database"
)

// GetDB retrieves the database instance for a migration
// Tries record.DB first, then falls back to pool
// Returns nil if DB is not available
func (m *Manager) GetDB(migrationID string) *db.DB {
	m.mu.RLock()
	record := m.migrations[migrationID]
	m.mu.RUnlock()

	if record != nil && record.DB != nil {
		return record.DB
	}

	return m.dbPool.Get(migrationID)
}

// EnsureDB ensures a database connection is open for the given migration ID
func (m *Manager) EnsureDB(migrationID string) (*db.DB, error) {
	dbInstance := m.GetDB(migrationID)
	if dbInstance != nil {
		return dbInstance, nil
	}

	dbPath, err := m.resolveDBPath("", migrationID)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve database path for migration %s: %w", migrationID, err)
	}

	dbInstance, err = m.dbPool.EnsureOpen(migrationID, dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to ensure database is open for migration %s: %w", migrationID, err)
	}

	m.mu.Lock()
	if record := m.migrations[migrationID]; record != nil {
		record.DB = dbInstance
	}
	m.mu.Unlock()

	return dbInstance, nil
}

// KillDBConnection kills the database connection for a migration but keeps the path
func (m *Manager) KillDBConnection(migrationID string) error {
	m.mu.Lock()
	if record := m.migrations[migrationID]; record != nil {
		record.DB = nil
	}
	m.mu.Unlock()

	return m.dbPool.KillConnection(migrationID)
}

// GetDuckDBPool returns the DuckDB connection pool
func (m *Manager) GetDuckDBPool() *corebridgeDB.DuckDBPool {
	return m.duckdbPool
}

// GetDuckDB retrieves a DuckDB connection for a migration
func (m *Manager) GetDuckDB(migrationID string) *sql.DB {
	return m.duckdbPool.Get(migrationID)
}

// EnsureDuckDB ensures a DuckDB connection is open for the given migration ID
func (m *Manager) EnsureDuckDB(migrationID, duckdbPath string) (*sql.DB, error) {
	if existing := m.duckdbPool.Get(migrationID); existing != nil {
		return existing, nil
	}

	return m.duckdbPool.EnsureOpen(migrationID, duckdbPath)
}

// KillDuckDBConnection kills the DuckDB connection for a migration but keeps the path
func (m *Manager) KillDuckDBConnection(migrationID string) error {
	return m.duckdbPool.KillConnection(migrationID)
}

// CloseDB closes the database connection for a migration
func (m *Manager) CloseDB(migrationID string) error {
	m.mu.Lock()
	if record := m.migrations[migrationID]; record != nil {
		record.DB = nil
	}
	m.mu.Unlock()

	return m.dbPool.Close(migrationID)
}
