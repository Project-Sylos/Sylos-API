package apidb

import (
	"crypto/rand"
	"database/sql"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	_ "github.com/duckdb/duckdb-go/v2"
	enginedb "codeberg.org/Sylos/Migration-Engine/pkg/db"
)

const defaultDBName = "sylos.duckdb"

// DB is the encrypted Sylos API database (users, migrations registry, keys, provider OAuth apps).
type DB struct {
	path       string
	masterKey  []byte
	engine     *enginedb.DB
	sql        *sql.DB
}

// Open opens or creates the encrypted API database.
func Open(dataDir string, masterKey []byte) (*DB, error) {
	if len(masterKey) != 32 {
		return nil, fmt.Errorf("master key must be 32 bytes")
	}
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return nil, err
	}
	path := filepath.Join(dataDir, defaultDBName)

	engine, err := enginedb.Open(enginedb.Options{
		Path:          path,
		EncryptionKey: masterKey,
	})
	if err != nil {
		return nil, fmt.Errorf("open API database: %w", err)
	}

	conn, err := engine.GetDB()
	if err != nil {
		_ = engine.Close()
		return nil, err
	}

	db := &DB{path: path, masterKey: masterKey, engine: engine, sql: conn}
	if err := db.migrate(); err != nil {
		_ = db.Close()
		return nil, err
	}
	return db, nil
}

func (d *DB) SQL() *sql.DB {
	return d.sql
}

func (d *DB) Path() string {
	return d.path
}

func (d *DB) Close() error {
	if d.engine == nil {
		return nil
	}
	err := d.engine.Close()
	d.engine = nil
	d.sql = nil
	return err
}

func (d *DB) migrate() error {
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS users (
			id VARCHAR PRIMARY KEY,
			username VARCHAR NOT NULL,
			password_hash VARCHAR NOT NULL,
			role VARCHAR NOT NULL,
			created_at VARCHAR NOT NULL,
			disabled BOOLEAN NOT NULL DEFAULT false,
			preferences VARCHAR
		)`,
		`CREATE UNIQUE INDEX IF NOT EXISTS users_username_lower ON users (lower(username))`,
		`CREATE TABLE IF NOT EXISTS api_migrations (
			id VARCHAR PRIMARY KEY,
			name VARCHAR NOT NULL,
			database_path VARCHAR,
			created_at TIMESTAMP NOT NULL,
			is_new_migration BOOLEAN NOT NULL DEFAULT false,
			has_path_review_changes BOOLEAN NOT NULL DEFAULT false
		)`,
		`CREATE TABLE IF NOT EXISTS migration_keys (
			migration_id VARCHAR PRIMARY KEY,
			encryption_key BLOB NOT NULL,
			created_at TIMESTAMP NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS provider_oauth_apps (
			provider_id VARCHAR PRIMARY KEY,
			client_id VARCHAR NOT NULL,
			client_secret VARCHAR NOT NULL,
			display_name VARCHAR,
			is_default BOOLEAN NOT NULL DEFAULT false,
			updated_at TIMESTAMP NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS install_config (
			key VARCHAR PRIMARY KEY,
			value VARCHAR NOT NULL
		)`,
	}
	for _, stmt := range stmts {
		if _, err := d.sql.Exec(stmt); err != nil {
			return fmt.Errorf("api db migrate: %w", err)
		}
	}
	return nil
}

const InstallConfigJWTSecret = "jwt_secret"

// GetInstallConfig returns a value from install_config, or sql.ErrNoRows if missing.
func (d *DB) GetInstallConfig(key string) (string, error) {
	var value string
	err := d.sql.QueryRow(`SELECT value FROM install_config WHERE key = ?`, key).Scan(&value)
	return value, err
}

// SetInstallConfig upserts a value in install_config.
func (d *DB) SetInstallConfig(key, value string) error {
	_, err := d.sql.Exec(
		`INSERT INTO install_config (key, value) VALUES (?, ?)
		 ON CONFLICT (key) DO UPDATE SET value = excluded.value`,
		key, value,
	)
	return err
}

// MigrationRecord mirrors metadata.MigrationMetadata for API DB storage.
type MigrationRecord struct {
	ID                   string
	Name                 string
	DatabasePath         string
	CreatedAt            time.Time
	IsNewMigration       bool
	HasPathReviewChanges bool
}

func (d *DB) UpsertMigration(rec MigrationRecord) error {
	if rec.Name == "" {
		rec.Name = rec.ID
	}
	if rec.CreatedAt.IsZero() {
		rec.CreatedAt = time.Now().UTC()
	}
	_, err := d.sql.Exec(
		`INSERT INTO api_migrations (id, name, database_path, created_at, is_new_migration, has_path_review_changes)
		 VALUES (?, ?, ?, ?, ?, ?)
		 ON CONFLICT (id) DO UPDATE SET
		 name = excluded.name,
		 database_path = excluded.database_path,
		 is_new_migration = excluded.is_new_migration,
		 has_path_review_changes = excluded.has_path_review_changes`,
		rec.ID, rec.Name, rec.DatabasePath, rec.CreatedAt, rec.IsNewMigration, rec.HasPathReviewChanges,
	)
	return err
}

func (d *DB) GetMigration(id string) (MigrationRecord, error) {
	var rec MigrationRecord
	var createdAt time.Time
	err := d.sql.QueryRow(
		`SELECT id, name, database_path, created_at, is_new_migration, has_path_review_changes
		 FROM api_migrations WHERE id = ?`, id,
	).Scan(&rec.ID, &rec.Name, &rec.DatabasePath, &createdAt, &rec.IsNewMigration, &rec.HasPathReviewChanges)
	if err != nil {
		return MigrationRecord{}, err
	}
	rec.CreatedAt = createdAt
	return rec, nil
}

func (d *DB) ListMigrations() ([]MigrationRecord, error) {
	rows, err := d.sql.Query(
		`SELECT id, name, database_path, created_at, is_new_migration, has_path_review_changes
		 FROM api_migrations ORDER BY created_at DESC`,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []MigrationRecord
	for rows.Next() {
		var rec MigrationRecord
		if err := rows.Scan(&rec.ID, &rec.Name, &rec.DatabasePath, &rec.CreatedAt, &rec.IsNewMigration, &rec.HasPathReviewChanges); err != nil {
			return nil, err
		}
		out = append(out, rec)
	}
	return out, rows.Err()
}

// DeleteAllMigrationRegistry removes migration registry rows and per-migration encryption keys.
func (d *DB) DeleteAllMigrationRegistry() error {
	if _, err := d.sql.Exec(`DELETE FROM migration_keys`); err != nil {
		return fmt.Errorf("delete migration keys: %w", err)
	}
	if _, err := d.sql.Exec(`DELETE FROM api_migrations`); err != nil {
		return fmt.Errorf("delete api migrations: %w", err)
	}
	return nil
}

func (d *DB) EnsureMigrationKey(migrationID string) ([]byte, error) {
	var key []byte
	err := d.sql.QueryRow(
		`SELECT encryption_key FROM migration_keys WHERE migration_id = ?`, migrationID,
	).Scan(&key)
	if err == nil && len(key) == 32 {
		return key, nil
	}
	if err != nil && err != sql.ErrNoRows {
		return nil, err
	}
	key = make([]byte, 32)
	if _, err := io.ReadFull(rand.Reader, key); err != nil {
		return nil, err
	}
	now := time.Now().UTC()
	_, err = d.sql.Exec(
		`INSERT INTO migration_keys (migration_id, encryption_key, created_at) VALUES (?, ?, ?)`,
		migrationID, key, now,
	)
	if err != nil {
		return nil, err
	}
	return key, nil
}

func (d *DB) MigrationKey(migrationID string) ([]byte, error) {
	var key []byte
	err := d.sql.QueryRow(
		`SELECT encryption_key FROM migration_keys WHERE migration_id = ?`, migrationID,
	).Scan(&key)
	if err != nil {
		return nil, err
	}
	if len(key) != 32 {
		return nil, fmt.Errorf("invalid migration key length for %s", migrationID)
	}
	return key, nil
}

type ProviderOAuthApp struct {
	ProviderID   string
	ClientID     string
	ClientSecret string
	DisplayName  string
	IsDefault    bool
	UpdatedAt    time.Time
}

func (d *DB) UpsertProviderOAuthApp(app ProviderOAuthApp) error {
	if app.UpdatedAt.IsZero() {
		app.UpdatedAt = time.Now().UTC()
	}
	_, err := d.sql.Exec(
		`INSERT INTO provider_oauth_apps (provider_id, client_id, client_secret, display_name, is_default, updated_at)
		 VALUES (?, ?, ?, ?, ?, ?)
		 ON CONFLICT (provider_id) DO UPDATE SET
		 client_id = excluded.client_id,
		 client_secret = excluded.client_secret,
		 display_name = excluded.display_name,
		 is_default = excluded.is_default,
		 updated_at = excluded.updated_at`,
		app.ProviderID, app.ClientID, app.ClientSecret, app.DisplayName, app.IsDefault, app.UpdatedAt,
	)
	return err
}

func (d *DB) GetProviderOAuthApp(providerID string) (ProviderOAuthApp, error) {
	var app ProviderOAuthApp
	err := d.sql.QueryRow(
		`SELECT provider_id, client_id, client_secret, display_name, is_default, updated_at
		 FROM provider_oauth_apps WHERE provider_id = ?`, providerID,
	).Scan(&app.ProviderID, &app.ClientID, &app.ClientSecret, &app.DisplayName, &app.IsDefault, &app.UpdatedAt)
	return app, err
}

func (d *DB) ListProviderOAuthApps() ([]ProviderOAuthApp, error) {
	rows, err := d.sql.Query(
		`SELECT provider_id, client_id, client_secret, display_name, is_default, updated_at
		 FROM provider_oauth_apps ORDER BY provider_id`,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ProviderOAuthApp
	for rows.Next() {
		var app ProviderOAuthApp
		if err := rows.Scan(&app.ProviderID, &app.ClientID, &app.ClientSecret, &app.DisplayName, &app.IsDefault, &app.UpdatedAt); err != nil {
			return nil, err
		}
		out = append(out, app)
	}
	return out, rows.Err()
}

func (d *DB) LoadOAuthCredsConfig() (map[string]ProviderOAuthApp, error) {
	apps, err := d.ListProviderOAuthApps()
	if err != nil {
		return nil, err
	}
	out := make(map[string]ProviderOAuthApp, len(apps))
	for _, app := range apps {
		out[app.ProviderID] = app
	}
	return out, nil
}
