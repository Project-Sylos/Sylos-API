package database

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	_ "github.com/marcboeker/go-duckdb"
	"github.com/rs/zerolog"
)

// DuckDBPool manages DuckDB connections for migrations
type DuckDBPool struct {
	mu          sync.RWMutex
	connections map[string]*sql.DB
	paths       map[string]string // Track DuckDB path for each migration ID (allows reopening after kill)
	isOpening   map[string]bool   // Track migrations that are currently being opened
	logger      zerolog.Logger
}

// NewDuckDBPool creates a new DuckDB connection pool
func NewDuckDBPool(logger zerolog.Logger) *DuckDBPool {
	return &DuckDBPool{
		connections: make(map[string]*sql.DB),
		paths:       make(map[string]string),
		isOpening:   make(map[string]bool),
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
// This is a legacy method - use EnsureOpen instead for proper synchronization
func (p *DuckDBPool) OpenDuckDB(migrationID, duckdbPath string) (*sql.DB, error) {
	return p.EnsureOpen(migrationID, duckdbPath)
}

// Get retrieves a DuckDB connection for the given migration ID
// Returns nil if not open
func (p *DuckDBPool) Get(migrationID string) *sql.DB {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.connections[migrationID]
}

// KillConnection closes a DuckDB connection but keeps the path in memory
// This allows the connection to be reopened later using EnsureOpen
// Use this when you need to temporarily close a connection (e.g., during ETL, phase transitions)
func (p *DuckDBPool) KillConnection(migrationID string) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	conn, exists := p.connections[migrationID]
	if !exists {
		p.logger.Debug().
			Str("migration_id", migrationID).
			Msg("DuckDB not in pool, already closed or never opened")
		return nil
	}

	// Remove connection from pool but keep path
	delete(p.connections, migrationID)
	// NOTE: Do NOT delete from paths - we keep the path so it can be reopened

	if err := conn.Close(); err != nil {
		p.logger.Error().
			Err(err).
			Str("migration_id", migrationID).
			Msg("failed to close DuckDB connection")
		return fmt.Errorf("failed to kill DuckDB connection for migration %s: %w", migrationID, err)
	}

	p.logger.Info().
		Str("migration_id", migrationID).
		Msg("killed DuckDB connection (path retained for reopening)")

	return nil
}

// Close closes a DuckDB connection and removes it from the pool
// This is a full cleanup - use KillConnection if you want to reopen later
func (p *DuckDBPool) Close(migrationID string) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	conn, exists := p.connections[migrationID]
	if !exists {
		p.logger.Debug().
			Str("migration_id", migrationID).
			Msg("DuckDB not in pool, already closed or never opened")
		return nil
	}

	// Remove from pool before closing
	delete(p.connections, migrationID)
	delete(p.paths, migrationID)

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

// EnsureOpen ensures a DuckDB connection is open for the given migration ID
// If the connection is already open, returns it
// If another request is currently opening it, waits up to 3 seconds for it to complete
// If the connection was killed, reopens it using the stored path
// If no path exists, uses the provided duckdbPath to open a new connection
// This is the recommended way to get a DuckDB connection when you're not sure if it's open
func (p *DuckDBPool) EnsureOpen(migrationID, duckdbPath string) (*sql.DB, error) {
	const waitTimeout = 3 * time.Second
	const pollInterval = 50 * time.Millisecond

	// Fast path: check if already open (read lock)
	p.mu.RLock()
	if existing := p.connections[migrationID]; existing != nil {
		p.mu.RUnlock()
		p.logger.Debug().
			Str("migration_id", migrationID).
			Msg("DuckDB already open, returning existing connection")
		return existing, nil
	}
	isOpening := p.isOpening[migrationID]
	p.mu.RUnlock()

	// If another request is opening it, wait for it to complete
	if isOpening {
		p.logger.Debug().
			Str("migration_id", migrationID).
			Msg("DuckDB is being opened by another request, waiting...")

		deadline := time.Now().Add(waitTimeout)
		for time.Now().Before(deadline) {
			time.Sleep(pollInterval)

			p.mu.RLock()
			if conn := p.connections[migrationID]; conn != nil {
				p.mu.RUnlock()
				p.logger.Debug().
					Str("migration_id", migrationID).
					Msg("DuckDB opened by another request, returning connection")
				return conn, nil
			}
			// Check if still opening or if opening failed (flag cleared but no connection)
			if !p.isOpening[migrationID] && p.connections[migrationID] == nil {
				// Opening finished but no connection - probably failed, break to retry
				p.mu.RUnlock()
				break
			}
			p.mu.RUnlock()
		}

		// After timeout, check one more time
		p.mu.RLock()
		if conn := p.connections[migrationID]; conn != nil {
			p.mu.RUnlock()
			return conn, nil
		}
		p.mu.RUnlock()

		// Timeout - return error
		return nil, fmt.Errorf("DuckDB connection request timed out after %v: connection was not opened in time", waitTimeout)
	}

	// Need to open it ourselves - acquire write lock and mark as opening
	p.mu.Lock()
	// Double-check after acquiring lock (another request might have opened it)
	if existing := p.connections[migrationID]; existing != nil {
		p.mu.Unlock()
		p.logger.Debug().
			Str("migration_id", migrationID).
			Msg("DuckDB opened by another request while waiting for lock, returning connection")
		return existing, nil
	}

	// Mark as opening
	p.isOpening[migrationID] = true
	p.mu.Unlock()

	// Do the actual opening (without holding the lock to avoid blocking other operations)
	var conn *sql.DB
	var openErr error
	func() {
		defer func() {
			// Clear isOpening flag when done (whether success or failure)
			p.mu.Lock()
			delete(p.isOpening, migrationID)
			p.mu.Unlock()
		}()

		// Check if we have a path stored (connection was killed but path retained)
		p.mu.RLock()
		storedPath := p.paths[migrationID]
		p.mu.RUnlock()

		if storedPath != "" {
			// Reopen using stored path
			duckdbPath = storedPath
			p.logger.Debug().
				Str("migration_id", migrationID).
				Str("duckdb_path", duckdbPath).
				Msg("reopening DuckDB connection from stored path")
		} else if duckdbPath == "" {
			// No stored path and no provided path - cannot open
			openErr = fmt.Errorf("cannot ensure DuckDB is open: no path provided and no stored path for migration %s", migrationID)
			return
		} else {
			// Store the provided path for future use
			p.mu.Lock()
			p.paths[migrationID] = duckdbPath
			p.mu.Unlock()
		}

		// Check if migration DB file exists
		if _, err := os.Stat(duckdbPath); os.IsNotExist(err) {
			openErr = fmt.Errorf("migration DB file does not exist: %s (migration may not be ready for path review)", duckdbPath)
			return
		}

		// Open DuckDB connection using sql.Open with duckdb driver
		conn, openErr = sql.Open("duckdb", duckdbPath)
		if openErr != nil {
			openErr = fmt.Errorf("failed to open DuckDB connection: %w", openErr)
			return
		}

		// Test the connection
		if err := conn.Ping(); err != nil {
			conn.Close()
			conn = nil
			openErr = fmt.Errorf("failed to ping DuckDB connection: %w", err)
			return
		}

		// Run ANALYZE on node tables (engine schema: src_nodes, dst_nodes)
		tables := []string{
			"src_nodes", "dst_nodes",
		}
		for _, table := range tables {
			if _, err := conn.Exec("ANALYZE " + table); err != nil {
				p.logger.Debug().
					Err(err).
					Str("migration_id", migrationID).
					Str("table", table).
					Msg("failed to analyze table (may not exist yet)")
			}
		}

		// Store in pool
		p.mu.Lock()
		p.connections[migrationID] = conn
		p.mu.Unlock()

		p.logger.Info().
			Str("migration_id", migrationID).
			Str("duckdb_path", duckdbPath).
			Msg("ensured DuckDB connection is open")
	}()

	if openErr != nil {
		return nil, openErr
	}

	if conn == nil {
		return nil, fmt.Errorf("failed to open DuckDB connection for migration %s", migrationID)
	}

	return conn, nil
}

// GetPath returns the DuckDB path for a migration ID
func (p *DuckDBPool) GetPath(migrationID string) string {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.paths[migrationID]
}

// Has returns true if the migration ID has an open DuckDB connection
func (p *DuckDBPool) Has(migrationID string) bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	_, exists := p.connections[migrationID]
	return exists
}

// CloseAll closes all DuckDB connections in the pool
func (p *DuckDBPool) CloseAll() error {
	p.mu.Lock()
	defer p.mu.Unlock()

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
	p.paths = make(map[string]string)
	p.isOpening = make(map[string]bool)

	p.logger.Info().Msg("closed all DuckDB connections in pool")

	return firstErr
}

// CheckMigrationDBExists checks if the migration DB file exists (duck-only: main .db is DuckDB)
func CheckMigrationDBExists(dbPath string) (bool, error) {
	_, err := os.Stat(dbPath)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

// CheckDuckDBExists is an alias for CheckMigrationDBExists (duck-only: main db is DuckDB)
func CheckDuckDBExists(dbPath string) (bool, error) {
	return CheckMigrationDBExists(dbPath)
}

// DeleteDuckDB deletes the migration DB file if it exists (duck-only: main .db is DuckDB)
func DeleteDuckDB(dbPath string) error {
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		return nil
	}
	if err := os.Remove(dbPath); err != nil {
		return fmt.Errorf("failed to delete migration DB file: %w", err)
	}
	return nil
}

// GetDuckDBPathFromConfigPath derives migration DB path from YAML config (duck-only: main .db is DuckDB)
func GetDuckDBPathFromConfigPath(configPath string) string {
	return DatabasePathFromConfigPath(configPath)
}
