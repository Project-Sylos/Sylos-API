package apidb

import (
	"crypto/rand"
	"database/sql"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	_ "github.com/marcboeker/go-duckdb"
	enginedb "codeberg.org/Sylos/Migration-Engine/pkg/db"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge/migrationkey"
)

const defaultDBName = "sylos.duckdb"

// DB is the Sylos API database (users, migrations registry, encrypted per-migration keys, provider OAuth apps).
type DB struct {
	path      string
	masterKey []byte
	sql       *sql.DB
}

// Open opens or creates the API database. masterKey must be the 32-byte install key from masterkey.Resolve.
func Open(dataDir string, masterKey []byte) (*DB, error) {
	if len(masterKey) != 32 {
		return nil, fmt.Errorf("master key must be 32 bytes")
	}

	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return nil, err
	}
	path := filepath.Join(dataDir, defaultDBName)

	engine, err := enginedb.Open(enginedb.Options{
		Path: path,
	})
	if err != nil {
		return nil, fmt.Errorf("open API database: %w", err)
	}
	conn, err := engine.GetDB()
	if err != nil {
		_ = engine.Close()
		return nil, err
	}

	db := &DB{path: path, masterKey: masterKey, sql: conn}
	if err := db.migrate(); err != nil {
		_ = conn.Close()
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
	if d.sql != nil {
		err := d.sql.Close()
		d.sql = nil
		return err
	}
	return nil
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
		// Store encrypted migration keys (not plaintext!).
		`CREATE TABLE IF NOT EXISTS migration_keys (
			migration_id VARCHAR PRIMARY KEY,
			encrypted_encryption_key BLOB NOT NULL,
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
		`CREATE TABLE IF NOT EXISTS sftp_known_hosts (
			host_port VARCHAR PRIMARY KEY,
			host_key VARCHAR NOT NULL,
			fingerprint VARCHAR NOT NULL,
			updated_at TIMESTAMP NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS sftp_saved_hosts (
			id VARCHAR PRIMARY KEY,
			display_name VARCHAR NOT NULL,
			host VARCHAR NOT NULL,
			port INTEGER NOT NULL,
			username VARCHAR NOT NULL,
			auth_method VARCHAR NOT NULL,
			secrets_blob BLOB NOT NULL,
			host_key VARCHAR,
			created_at TIMESTAMP NOT NULL,
			updated_at TIMESTAMP NOT NULL,
			last_used_at TIMESTAMP
		)`,
		`CREATE UNIQUE INDEX IF NOT EXISTS sftp_saved_hosts_endpoint
			ON sftp_saved_hosts (host, port, username)`,
	}
	for _, stmt := range stmts {
		if _, err := d.sql.Exec(stmt); err != nil {
			return fmt.Errorf("api db migrate: %w", err)
		}
	}
	return d.migrateProviderOAuthHealthColumns()
}

func (d *DB) migrateProviderOAuthHealthColumns() error {
	for _, col := range []struct{ name, ddl string }{
		{"health_status", `ALTER TABLE provider_oauth_apps ADD COLUMN health_status VARCHAR`},
		{"health_checked_at", `ALTER TABLE provider_oauth_apps ADD COLUMN health_checked_at TIMESTAMP`},
		{"health_error", `ALTER TABLE provider_oauth_apps ADD COLUMN health_error VARCHAR`},
		{"health_monitor_enabled", `ALTER TABLE provider_oauth_apps ADD COLUMN health_monitor_enabled BOOLEAN`},
	} {
		var exists int64
		err := d.sql.QueryRow(
			`SELECT COUNT(*) FROM information_schema.columns WHERE table_name = 'provider_oauth_apps' AND column_name = ?`,
			col.name,
		).Scan(&exists)
		if err != nil {
			return err
		}
		if exists > 0 {
			continue
		}
		if _, err := d.sql.Exec(col.ddl); err != nil {
			if !strings.Contains(strings.ToLower(err.Error()), "already exists") {
				return err
			}
		}
	}
	_, _ = d.sql.Exec(`UPDATE provider_oauth_apps SET health_monitor_enabled = false WHERE health_monitor_enabled IS NULL`)
	_, _ = d.sql.Exec(`UPDATE provider_oauth_apps SET health_monitor_enabled = true WHERE health_status = 'healthy'`)
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

// WipeInstallUserData removes users, cloud provider OAuth apps, install config, and SFTP host pins from the API database.
// Migration registry rows should be cleared separately via DeleteAllMigrationRegistry.
func (d *DB) WipeInstallUserData() error {
	if _, err := d.sql.Exec(`DELETE FROM users`); err != nil {
		return fmt.Errorf("delete users: %w", err)
	}
	if _, err := d.sql.Exec(`DELETE FROM user_audit_events`); err != nil {
		return fmt.Errorf("delete user audit events: %w", err)
	}
	if _, err := d.sql.Exec(`DELETE FROM provider_oauth_apps`); err != nil {
		return fmt.Errorf("delete provider oauth apps: %w", err)
	}
	if _, err := d.sql.Exec(`DELETE FROM install_config`); err != nil {
		return fmt.Errorf("delete install config: %w", err)
	}
	if err := d.DeleteAllSFTPKnownHosts(); err != nil {
		return err
	}
	if err := d.DeleteAllSFTPSavedHosts(); err != nil {
		return err
	}
	return nil
}

// EnsureMigrationKey returns the decrypted per-migration key for the given migrationID,
// creating a new one if not present (and storing it encrypted with the master key).
func (d *DB) EnsureMigrationKey(migrationID string) ([]byte, error) {
	var encryptedKey []byte
	err := d.sql.QueryRow(
		`SELECT encrypted_encryption_key FROM migration_keys WHERE migration_id = ?`, migrationID,
	).Scan(&encryptedKey)
	if err == nil {
		key, err := migrationkey.DecryptMigrationKey(encryptedKey, d.masterKey)
		if err == nil && len(key) == 32 {
			return key, nil
		}
		if err != nil {
			return nil, fmt.Errorf("failed to decrypt migration key: %w", err)
		}
	}
	if err != nil && err != sql.ErrNoRows {
		return nil, err
	}
	// No key found, so generate a new per-migration key, encrypt it, and store.
	key := make([]byte, 32)
	if _, err := io.ReadFull(rand.Reader, key); err != nil {
		return nil, err
	}
	encryptedKey, err = migrationkey.EncryptMigrationKey(key, d.masterKey)
	if err != nil {
		return nil, fmt.Errorf("failed to encrypt migration key: %w", err)
	}
	now := time.Now().UTC()
	_, err = d.sql.Exec(
		`INSERT INTO migration_keys (migration_id, encrypted_encryption_key, created_at) VALUES (?, ?, ?)`,
		migrationID, encryptedKey, now,
	)
	if err != nil {
		return nil, err
	}
	return key, nil
}

// MigrationKey returns the decrypted per-migration key for the given migrationID.
func (d *DB) MigrationKey(migrationID string) ([]byte, error) {
	var encryptedKey []byte
	err := d.sql.QueryRow(
		`SELECT encrypted_encryption_key FROM migration_keys WHERE migration_id = ?`, migrationID,
	).Scan(&encryptedKey)
	if err != nil {
		return nil, err
	}
	key, err := migrationkey.DecryptMigrationKey(encryptedKey, d.masterKey)
	if err != nil {
		return nil, fmt.Errorf("failed to decrypt migration key: %w", err)
	}
	if len(key) != 32 {
		return nil, fmt.Errorf("invalid migration key length for %s", migrationID)
	}
	return key, nil
}

type ProviderOAuthApp struct {
	ProviderID            string
	ClientID              string
	ClientSecret          string
	DisplayName           string
	IsDefault             bool
	UpdatedAt             time.Time
	HealthStatus          string
	HealthCheckedAt       *time.Time
	HealthError           string
	HealthMonitorEnabled  bool
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
	var checkedAt sql.NullTime
	var healthStatus, healthError sql.NullString
	var healthMonitor bool
	err := d.sql.QueryRow(
		`SELECT provider_id, client_id, client_secret, display_name, is_default, updated_at,
		        health_status, health_checked_at, health_error, COALESCE(health_monitor_enabled, false)
		 FROM provider_oauth_apps WHERE provider_id = ?`, providerID,
	).Scan(
		&app.ProviderID, &app.ClientID, &app.ClientSecret, &app.DisplayName, &app.IsDefault, &app.UpdatedAt,
		&healthStatus, &checkedAt, &healthError, &healthMonitor,
	)
	if err != nil {
		return ProviderOAuthApp{}, err
	}
	if healthStatus.Valid {
		app.HealthStatus = healthStatus.String
	}
	if healthError.Valid {
		app.HealthError = healthError.String
	}
	if checkedAt.Valid {
		t := checkedAt.Time
		app.HealthCheckedAt = &t
	}
	app.HealthMonitorEnabled = healthMonitor
	return app, nil
}

func (d *DB) UpdateProviderOAuthHealth(providerID, status string, checkedAt time.Time, healthError string) error {
	_, err := d.sql.Exec(
		`UPDATE provider_oauth_apps SET health_status = ?, health_checked_at = ?, health_error = ? WHERE provider_id = ?`,
		status, checkedAt, healthError, providerID,
	)
	return err
}

func (d *DB) SetProviderOAuthHealthMonitor(providerID string, enabled bool) error {
	_, err := d.sql.Exec(
		`UPDATE provider_oauth_apps SET health_monitor_enabled = ? WHERE provider_id = ?`,
		enabled, providerID,
	)
	return err
}

func (d *DB) DeleteProviderOAuthApp(providerID string) error {
	_, err := d.sql.Exec(`DELETE FROM provider_oauth_apps WHERE provider_id = ?`, providerID)
	return err
}

func (d *DB) ListProviderOAuthApps() ([]ProviderOAuthApp, error) {
	rows, err := d.sql.Query(
		`SELECT provider_id, client_id, client_secret, display_name, is_default, updated_at,
		        health_status, health_checked_at, health_error, COALESCE(health_monitor_enabled, false)
		 FROM provider_oauth_apps ORDER BY provider_id`,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ProviderOAuthApp
	for rows.Next() {
		var app ProviderOAuthApp
		var checkedAt sql.NullTime
		var healthStatus, healthError sql.NullString
		var healthMonitor bool
		if err := rows.Scan(
			&app.ProviderID, &app.ClientID, &app.ClientSecret, &app.DisplayName, &app.IsDefault, &app.UpdatedAt,
			&healthStatus, &checkedAt, &healthError, &healthMonitor,
		); err != nil {
			return nil, err
		}
		if healthStatus.Valid {
			app.HealthStatus = healthStatus.String
		}
		if healthError.Valid {
			app.HealthError = healthError.String
		}
		if checkedAt.Valid {
			t := checkedAt.Time
			app.HealthCheckedAt = &t
		}
		app.HealthMonitorEnabled = healthMonitor
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
