package database

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	_ "github.com/marcboeker/go-duckdb"
	"github.com/rs/zerolog"
)

// DuckDBPool manages DuckDB connections for migrations
type DuckDBPool struct {
	connections map[string]*sql.DB
	logger      zerolog.Logger
}

// NewDuckDBPool creates a new DuckDB connection pool
func NewDuckDBPool(logger zerolog.Logger) *DuckDBPool {
	return &DuckDBPool{
		connections: make(map[string]*sql.DB),
		logger:      logger,
	}
}

// GetDuckDBPath returns the DuckDB file path for a migration
// Convention: {name}-duck.db (e.g., migration.db -> migration-duck.db)
func GetDuckDBPath(boltDBPath string) string {
	// Extract directory and filename
	dir := filepath.Dir(boltDBPath)
	filename := filepath.Base(boltDBPath)

	// Remove .db extension and add -duck.db
	name := strings.TrimSuffix(filename, ".db")
	return filepath.Join(dir, name+"-duck.db")
}

// OpenDuckDB opens a DuckDB connection for a migration
func (p *DuckDBPool) OpenDuckDB(migrationID, duckdbPath string) (*sql.DB, error) {
	// Check if already open
	if existing := p.connections[migrationID]; existing != nil {
		p.logger.Debug().
			Str("migration_id", migrationID).
			Str("duckdb_path", duckdbPath).
			Msg("DuckDB already open for migration, returning existing connection")
		return existing, nil
	}

	// Check if DuckDB file exists
	if _, err := os.Stat(duckdbPath); os.IsNotExist(err) {
		return nil, fmt.Errorf("DuckDB file does not exist: %s (ETL may not have completed)", duckdbPath)
	}

	// Open DuckDB connection using sql.Open with duckdb driver
	conn, err := sql.Open("duckdb", duckdbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open DuckDB connection: %w", err)
	}

	// Test the connection
	if err := conn.Ping(); err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to ping DuckDB connection: %w", err)
	}

	// Store in pool
	p.connections[migrationID] = conn

	p.logger.Info().
		Str("migration_id", migrationID).
		Str("duckdb_path", duckdbPath).
		Msg("opened DuckDB connection for migration")

	return conn, nil
}

// Get retrieves a DuckDB connection for the given migration ID
// Returns nil if not open
func (p *DuckDBPool) Get(migrationID string) *sql.DB {
	return p.connections[migrationID]
}

// Close closes a DuckDB connection and removes it from the pool
func (p *DuckDBPool) Close(migrationID string) error {
	conn, exists := p.connections[migrationID]
	if !exists {
		p.logger.Debug().
			Str("migration_id", migrationID).
			Msg("DuckDB not in pool, already closed or never opened")
		return nil
	}

	// Remove from pool before closing
	delete(p.connections, migrationID)

	if err := conn.Close(); err != nil {
		p.logger.Error().
			Err(err).
			Str("migration_id", migrationID).
			Msg("failed to close DuckDB connection")
		return fmt.Errorf("failed to close DuckDB for migration %s: %w", migrationID, err)
	}

	p.logger.Info().
		Str("migration_id", migrationID).
		Msg("closed DuckDB connection for migration")

	return nil
}

// Has returns true if the migration ID has an open DuckDB connection
func (p *DuckDBPool) Has(migrationID string) bool {
	_, exists := p.connections[migrationID]
	return exists
}

// CloseAll closes all DuckDB connections in the pool
func (p *DuckDBPool) CloseAll() error {
	var firstErr error
	for migrationID, conn := range p.connections {
		if err := conn.Close(); err != nil {
			p.logger.Error().
				Err(err).
				Str("migration_id", migrationID).
				Msg("failed to close DuckDB connection during pool shutdown")
			if firstErr == nil {
				firstErr = err
			}
		}
	}

	// Clear pool
	p.connections = make(map[string]*sql.DB)

	p.logger.Info().Msg("closed all DuckDB connections in pool")

	return firstErr
}

// CheckDuckDBExists checks if the DuckDB file exists for a migration
func CheckDuckDBExists(boltDBPath string) (bool, error) {
	duckdbPath := GetDuckDBPath(boltDBPath)
	_, err := os.Stat(duckdbPath)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

// DeleteDuckDB deletes the DuckDB file if it exists
func DeleteDuckDB(boltDBPath string) error {
	duckdbPath := GetDuckDBPath(boltDBPath)
	if _, err := os.Stat(duckdbPath); os.IsNotExist(err) {
		return nil // File doesn't exist, nothing to delete
	}

	if err := os.Remove(duckdbPath); err != nil {
		return fmt.Errorf("failed to delete DuckDB file: %w", err)
	}

	return nil
}

// GetDuckDBPathFromConfigPath derives DuckDB path from YAML config path
func GetDuckDBPathFromConfigPath(configPath string) string {
	// Config is {migrationID}.yaml, DB is {migrationID}.db, DuckDB is {migrationID}.db.duckdb
	dbPath := DatabasePathFromConfigPath(configPath)
	return GetDuckDBPath(dbPath)
}
