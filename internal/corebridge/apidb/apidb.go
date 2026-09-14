package apidb

import (
	"crypto/rand"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"codeberg.org/Sylos/Sylos-FS/pkg/credentials"
	badger "github.com/dgraph-io/badger/v4"
)

// DB is the Sylos API database (users, migrations registry, encrypted per-migration keys, provider OAuth apps).
type DB struct {
	dir       string
	masterKey []byte
	db        *badger.DB
}

// Open opens or creates the API Badger store (sylos.api/). masterKey must be the 32-byte install key from masterkey.Resolve.
func Open(dataDir string, masterKey []byte) (*DB, error) {
	if len(masterKey) != 32 {
		return nil, fmt.Errorf("master key must be 32 bytes")
	}
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return nil, err
	}
	dir := filepath.Join(dataDir, defaultStoreDirName)
	bopts := badger.DefaultOptions(filepath.Clean(dir))
	bopts.Logger = nil
	bopts.SyncWrites = false
	bopts.MemTableSize = 128 << 20
	bopts.NumMemtables = 5
	bopts.NumLevelZeroTables = 10
	bopts.NumLevelZeroTablesStall = 30
	bopts.NumCompactors = 8
	bopts.ValueLogFileSize = 256 << 20
	bopts.IndexCacheSize = 64 << 20
	raw, err := badger.Open(bopts)
	if err != nil {
		return nil, fmt.Errorf("open API database: %w", err)
	}
	d := &DB{dir: dir, masterKey: masterKey, db: raw}
	if err := d.db.Update(initSchema); err != nil {
		_ = raw.Close()
		return nil, fmt.Errorf("init API database schema: %w", err)
	}
	if err := d.SeedBuiltinRulesets(); err != nil {
		_ = raw.Close()
		return nil, fmt.Errorf("seed builtin rulesets: %w", err)
	}
	return d, nil
}

func (d *DB) Path() string {
	return d.dir
}

func (d *DB) Close() error {
	if d.db != nil {
		err := d.db.Close()
		d.db = nil
		return err
	}
	return nil
}

const InstallConfigJWTSecret = "jwt_secret"

// GetInstallConfig returns a value from install config, or ErrNotFound if missing.
func (d *DB) GetInstallConfig(key string) (string, error) {
	var value string
	err := d.view(func(txn *badger.Txn) error {
		var err error
		value, err = getString(txn, keyCfg(key))
		return err
	})
	return value, err
}

// SetInstallConfig upserts a value in install config.
func (d *DB) SetInstallConfig(key, value string) error {
	return d.update(func(txn *badger.Txn) error {
		return putString(txn, keyCfg(key), value)
	})
}

// MigrationRecord mirrors metadata.MigrationMetadata for API DB storage.
type MigrationRecord struct {
	ID                   string    `json:"id"`
	Name                 string    `json:"name"`
	DatabasePath         string    `json:"databasePath"`
	CreatedAt            time.Time `json:"createdAt"`
	IsNewMigration       bool      `json:"isNewMigration"`
	HasPathReviewChanges bool      `json:"hasPathReviewChanges"`
}

func (d *DB) UpsertMigration(rec MigrationRecord) error {
	if rec.Name == "" {
		rec.Name = rec.ID
	}
	if rec.CreatedAt.IsZero() {
		rec.CreatedAt = time.Now().UTC()
	}
	return d.update(func(txn *badger.Txn) error {
		return putJSON(txn, keyMig(rec.ID), rec)
	})
}

func (d *DB) GetMigration(id string) (MigrationRecord, error) {
	var rec MigrationRecord
	err := d.view(func(txn *badger.Txn) error {
		return getJSON(txn, keyMig(id), &rec)
	})
	return rec, err
}

func (d *DB) ListMigrations() ([]MigrationRecord, error) {
	var out []MigrationRecord
	err := d.view(func(txn *badger.Txn) error {
		recs, err := listPrefixJSON[MigrationRecord](txn, prefixMig, nil)
		if err != nil {
			return err
		}
		out = recs
		return nil
	})
	if err != nil {
		return nil, err
	}
	sortMigrationsByCreatedAtDesc(out)
	return out, nil
}

func sortMigrationsByCreatedAtDesc(recs []MigrationRecord) {
	for i := 0; i < len(recs); i++ {
		for j := i + 1; j < len(recs); j++ {
			if recs[j].CreatedAt.After(recs[i].CreatedAt) {
				recs[i], recs[j] = recs[j], recs[i]
			}
		}
	}
}

// DeleteAllMigrationRegistry removes migration registry rows and per-migration encryption keys.
func (d *DB) DeleteAllMigrationRegistry() error {
	return d.update(func(txn *badger.Txn) error {
		if err := deletePrefix(txn, prefixMigKey); err != nil {
			return fmt.Errorf("delete migration keys: %w", err)
		}
		if err := deletePrefix(txn, prefixMig); err != nil {
			return fmt.Errorf("delete api migrations: %w", err)
		}
		return nil
	})
}

// WipeInstallUserData removes users, cloud provider OAuth apps, install config, SFTP host pins, and scaling overrides.
func (d *DB) WipeInstallUserData() error {
	return d.update(func(txn *badger.Txn) error {
		if err := deletePrefix(txn, prefixUser); err != nil {
			return fmt.Errorf("delete users: %w", err)
		}
		if err := deletePrefix(txn, prefixUserAudit); err != nil {
			return fmt.Errorf("delete user audit events: %w", err)
		}
		if err := deletePrefix(txn, prefixOAuth); err != nil {
			return fmt.Errorf("delete provider oauth apps: %w", err)
		}
		if err := deletePrefix(txn, prefixCfg); err != nil {
			return fmt.Errorf("delete install config: %w", err)
		}
		if err := deletePrefix(txn, prefixSftpKnown); err != nil {
			return fmt.Errorf("delete sftp known hosts: %w", err)
		}
		if err := deletePrefix(txn, prefixSftpSaved); err != nil {
			return fmt.Errorf("delete sftp saved hosts: %w", err)
		}
		if err := deletePrefix(txn, prefixScale); err != nil {
			return fmt.Errorf("delete scaling overrides: %w", err)
		}
		return nil
	})
}

type migrationKeyRecord struct {
	MigrationID string    `json:"migrationId"`
	Encrypted   []byte    `json:"encrypted"`
	CreatedAt   time.Time `json:"createdAt"`
}

// EnsureMigrationKey returns the decrypted per-migration key for the given migrationID,
// creating a new one if not present (and storing it encrypted with the master key).
func (d *DB) EnsureMigrationKey(migrationID string) ([]byte, error) {
	key, err := d.MigrationKey(migrationID)
	if err == nil {
		return key, nil
	}
	if err != ErrNotFound {
		return nil, err
	}
	newKey := make([]byte, 32)
	if _, err := io.ReadFull(rand.Reader, newKey); err != nil {
		return nil, err
	}
	encryptedKey, err := credentials.Encrypt(newKey, d.masterKey)
	if err != nil {
		return nil, fmt.Errorf("failed to encrypt migration key: %w", err)
	}
	rec := migrationKeyRecord{
		MigrationID: migrationID,
		Encrypted:   encryptedKey,
		CreatedAt:   time.Now().UTC(),
	}
	if err := d.update(func(txn *badger.Txn) error {
		return putJSON(txn, keyMigKey(migrationID), rec)
	}); err != nil {
		return nil, err
	}
	return newKey, nil
}

// MigrationKey returns the decrypted per-migration key for the given migrationID.
func (d *DB) MigrationKey(migrationID string) ([]byte, error) {
	var rec migrationKeyRecord
	err := d.view(func(txn *badger.Txn) error {
		return getJSON(txn, keyMigKey(migrationID), &rec)
	})
	if err != nil {
		return nil, err
	}
	key, err := credentials.Decrypt(rec.Encrypted, d.masterKey)
	if err != nil {
		return nil, fmt.Errorf("failed to decrypt migration key: %w", err)
	}
	if len(key) != 32 {
		return nil, fmt.Errorf("invalid migration key length for %s", migrationID)
	}
	return key, nil
}

// PutMigrationKeyEncrypted stores an imported encrypted migration key blob.
func (d *DB) PutMigrationKeyEncrypted(migrationID string, encrypted []byte, createdAt time.Time) error {
	if createdAt.IsZero() {
		createdAt = time.Now().UTC()
	}
	rec := migrationKeyRecord{
		MigrationID: migrationID,
		Encrypted:   encrypted,
		CreatedAt:   createdAt,
	}
	return d.update(func(txn *badger.Txn) error {
		return putJSON(txn, keyMigKey(migrationID), rec)
	})
}

type ProviderOAuthApp struct {
	ProviderID           string     `json:"providerId"`
	ClientID             string     `json:"clientId"`
	ClientSecret         string     `json:"clientSecret"`
	TenantID             string     `json:"tenantId"`
	DisplayName          string     `json:"displayName"`
	IsDefault            bool       `json:"isDefault"`
	UpdatedAt            time.Time  `json:"updatedAt"`
	HealthStatus         string     `json:"healthStatus"`
	HealthCheckedAt      *time.Time `json:"healthCheckedAt,omitempty"`
	HealthError          string     `json:"healthError"`
	HealthMonitorEnabled bool       `json:"healthMonitorEnabled"`
}

func (d *DB) UpsertProviderOAuthApp(app ProviderOAuthApp) error {
	if app.UpdatedAt.IsZero() {
		app.UpdatedAt = time.Now().UTC()
	}
	return d.update(func(txn *badger.Txn) error {
		return putJSON(txn, keyOAuth(app.ProviderID), app)
	})
}

func (d *DB) GetProviderOAuthApp(providerID string) (ProviderOAuthApp, error) {
	var app ProviderOAuthApp
	err := d.view(func(txn *badger.Txn) error {
		return getJSON(txn, keyOAuth(providerID), &app)
	})
	return app, err
}

func (d *DB) UpdateProviderOAuthHealth(providerID, status string, checkedAt time.Time, healthError string) error {
	return d.update(func(txn *badger.Txn) error {
		var app ProviderOAuthApp
		if err := getJSON(txn, keyOAuth(providerID), &app); err != nil {
			return err
		}
		app.HealthStatus = status
		t := checkedAt
		app.HealthCheckedAt = &t
		app.HealthError = healthError
		return putJSON(txn, keyOAuth(providerID), app)
	})
}

func (d *DB) SetProviderOAuthHealthMonitor(providerID string, enabled bool) error {
	return d.update(func(txn *badger.Txn) error {
		var app ProviderOAuthApp
		if err := getJSON(txn, keyOAuth(providerID), &app); err != nil {
			return err
		}
		app.HealthMonitorEnabled = enabled
		return putJSON(txn, keyOAuth(providerID), app)
	})
}

func (d *DB) DeleteProviderOAuthApp(providerID string) error {
	return d.update(func(txn *badger.Txn) error {
		return deleteKey(txn, keyOAuth(providerID))
	})
}

func (d *DB) ListProviderOAuthApps() ([]ProviderOAuthApp, error) {
	var out []ProviderOAuthApp
	err := d.view(func(txn *badger.Txn) error {
		recs, err := listPrefixJSON[ProviderOAuthApp](txn, prefixOAuth, nil)
		if err != nil {
			return err
		}
		out = recs
		return nil
	})
	if err != nil {
		return nil, err
	}
	sortOAuthAppsByProviderID(out)
	return out, nil
}

func sortOAuthAppsByProviderID(apps []ProviderOAuthApp) {
	for i := 0; i < len(apps); i++ {
		for j := i + 1; j < len(apps); j++ {
			if apps[j].ProviderID < apps[i].ProviderID {
				apps[i], apps[j] = apps[j], apps[i]
			}
		}
	}
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
