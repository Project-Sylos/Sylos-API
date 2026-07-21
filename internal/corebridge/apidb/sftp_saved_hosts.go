package apidb

import (
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"codeberg.org/Sylos/Sylos-FS/pkg/credentials"
)

// SFTPSavedHost is a FileZilla-style remembered SFTP site (no live health checks).
type SFTPSavedHost struct {
	ID           string
	DisplayName  string
	Host         string
	Port         int
	Username     string
	AuthMethod   string // "password" | "pem"
	Password     string // decrypted; omitted from list responses
	PrivateKey   string
	KeyPassphrase string
	HostKey      string
	CreatedAt    time.Time
	UpdatedAt    time.Time
	LastUsedAt   *time.Time
}

type sftpSavedSecrets struct {
	Password      string `json:"password,omitempty"`
	PrivateKey    string `json:"privateKey,omitempty"`
	KeyPassphrase string `json:"keyPassphrase,omitempty"`
}

func (d *DB) encryptSFTPSavedSecrets(secrets sftpSavedSecrets) ([]byte, error) {
	raw, err := json.Marshal(secrets)
	if err != nil {
		return nil, err
	}
	return credentials.Encrypt(raw, d.masterKey)
}

func (d *DB) decryptSFTPSavedSecrets(blob []byte) (sftpSavedSecrets, error) {
	raw, err := credentials.Decrypt(blob, d.masterKey)
	if err != nil {
		return sftpSavedSecrets{}, err
	}
	var secrets sftpSavedSecrets
	if err := json.Unmarshal(raw, &secrets); err != nil {
		return sftpSavedSecrets{}, err
	}
	return secrets, nil
}

func normalizeSFTPSavedHostInput(host *SFTPSavedHost) error {
	if host == nil {
		return fmt.Errorf("sftp saved host is required")
	}
	host.Host = strings.TrimSpace(strings.ToLower(host.Host))
	host.Username = strings.TrimSpace(host.Username)
	host.DisplayName = strings.TrimSpace(host.DisplayName)
	host.AuthMethod = strings.TrimSpace(strings.ToLower(host.AuthMethod))
	host.HostKey = strings.TrimSpace(host.HostKey)
	if host.Port <= 0 {
		host.Port = 22
	}
	if host.Host == "" {
		return fmt.Errorf("host is required")
	}
	if host.Username == "" {
		return fmt.Errorf("username is required")
	}
	switch host.AuthMethod {
	case "password", "pem":
	default:
		return fmt.Errorf("authMethod must be password or pem")
	}
	if host.DisplayName == "" {
		host.DisplayName = fmt.Sprintf("%s@%s:%d", host.Username, host.Host, host.Port)
	}
	return nil
}

// ListSFTPSavedHosts returns remembered hosts without decrypted secrets.
func (d *DB) ListSFTPSavedHosts() ([]SFTPSavedHost, error) {
	rows, err := d.sql.Query(`
		SELECT id, display_name, host, port, username, auth_method, host_key, created_at, updated_at, last_used_at
		FROM sftp_saved_hosts
		ORDER BY COALESCE(last_used_at, updated_at) DESC, display_name ASC
	`)
	if err != nil {
		return nil, fmt.Errorf("list sftp saved hosts: %w", err)
	}
	defer rows.Close()

	var out []SFTPSavedHost
	for rows.Next() {
		var h SFTPSavedHost
		var lastUsed sql.NullTime
		var hostKey sql.NullString
		if err := rows.Scan(
			&h.ID, &h.DisplayName, &h.Host, &h.Port, &h.Username, &h.AuthMethod, &hostKey,
			&h.CreatedAt, &h.UpdatedAt, &lastUsed,
		); err != nil {
			return nil, err
		}
		if hostKey.Valid {
			h.HostKey = hostKey.String
		}
		if lastUsed.Valid {
			t := lastUsed.Time
			h.LastUsedAt = &t
		}
		out = append(out, h)
	}
	return out, rows.Err()
}

// GetSFTPSavedHost returns one host with secrets decrypted for reconnect.
func (d *DB) GetSFTPSavedHost(id string) (SFTPSavedHost, error) {
	id = strings.TrimSpace(id)
	if id == "" {
		return SFTPSavedHost{}, fmt.Errorf("saved host id is required")
	}
	var h SFTPSavedHost
	var blob []byte
	var lastUsed sql.NullTime
	var hostKey sql.NullString
	err := d.sql.QueryRow(`
		SELECT id, display_name, host, port, username, auth_method, secrets_blob, host_key, created_at, updated_at, last_used_at
		FROM sftp_saved_hosts WHERE id = ?
	`, id).Scan(
		&h.ID, &h.DisplayName, &h.Host, &h.Port, &h.Username, &h.AuthMethod, &blob, &hostKey,
		&h.CreatedAt, &h.UpdatedAt, &lastUsed,
	)
	if err == sql.ErrNoRows {
		return SFTPSavedHost{}, fmt.Errorf("saved host not found")
	}
	if err != nil {
		return SFTPSavedHost{}, err
	}
	secrets, err := d.decryptSFTPSavedSecrets(blob)
	if err != nil {
		return SFTPSavedHost{}, fmt.Errorf("decrypt saved host secrets: %w", err)
	}
	h.Password = secrets.Password
	h.PrivateKey = secrets.PrivateKey
	h.KeyPassphrase = secrets.KeyPassphrase
	if hostKey.Valid {
		h.HostKey = hostKey.String
	}
	if lastUsed.Valid {
		t := lastUsed.Time
		h.LastUsedAt = &t
	}
	return h, nil
}

// UpsertSFTPSavedHost creates or updates a remembered host. When ID is empty, matches by host+port+username.
func (d *DB) UpsertSFTPSavedHost(host SFTPSavedHost) (SFTPSavedHost, error) {
	if err := normalizeSFTPSavedHostInput(&host); err != nil {
		return SFTPSavedHost{}, err
	}
	blob, err := d.encryptSFTPSavedSecrets(sftpSavedSecrets{
		Password:      host.Password,
		PrivateKey:    host.PrivateKey,
		KeyPassphrase: host.KeyPassphrase,
	})
	if err != nil {
		return SFTPSavedHost{}, fmt.Errorf("encrypt saved host secrets: %w", err)
	}

	now := time.Now().UTC()
	if host.ID == "" {
		var existingID string
		lookupErr := d.sql.QueryRow(`
			SELECT id FROM sftp_saved_hosts WHERE host = ? AND port = ? AND username = ?
		`, host.Host, host.Port, host.Username).Scan(&existingID)
		if lookupErr == nil {
			host.ID = existingID
		} else if lookupErr != sql.ErrNoRows {
			return SFTPSavedHost{}, lookupErr
		} else {
			host.ID = newSFTPSavedHostID()
		}
	}

	var existingCreated time.Time
	lookupErr := d.sql.QueryRow(`SELECT created_at FROM sftp_saved_hosts WHERE id = ?`, host.ID).Scan(&existingCreated)
	exists := lookupErr == nil
	if lookupErr != nil && lookupErr != sql.ErrNoRows {
		return SFTPSavedHost{}, lookupErr
	}
	if exists {
		host.CreatedAt = existingCreated
	} else {
		host.CreatedAt = now
	}
	host.UpdatedAt = now
	host.LastUsedAt = &now

	if exists {
		res, execErr := d.sql.Exec(`
			UPDATE sftp_saved_hosts SET
				display_name = ?,
				auth_method = ?,
				secrets_blob = ?,
				host_key = ?,
				updated_at = ?,
				last_used_at = ?
			WHERE id = ?
		`, host.DisplayName, host.AuthMethod, blob, nullIfEmpty(host.HostKey),
			host.UpdatedAt, host.LastUsedAt, host.ID)
		if execErr != nil {
			return SFTPSavedHost{}, fmt.Errorf("upsert sftp saved host: %w", execErr)
		}
		if n, _ := res.RowsAffected(); n == 0 {
			return SFTPSavedHost{}, fmt.Errorf("upsert sftp saved host: row disappeared")
		}
	} else {
		_, execErr := d.sql.Exec(`
			INSERT INTO sftp_saved_hosts (
				id, display_name, host, port, username, auth_method, secrets_blob, host_key, created_at, updated_at, last_used_at
			) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		`, host.ID, host.DisplayName, host.Host, host.Port, host.Username, host.AuthMethod, blob, nullIfEmpty(host.HostKey),
			host.CreatedAt, host.UpdatedAt, host.LastUsedAt)
		if execErr != nil {
			return SFTPSavedHost{}, fmt.Errorf("upsert sftp saved host: %w", execErr)
		}
	}

	// Never return secrets on write responses used for list-style UIs; caller can Get if needed.
	host.Password = ""
	host.PrivateKey = ""
	host.KeyPassphrase = ""
	return host, nil
}

// TouchSFTPSavedHostLastUsed updates last_used_at after a successful reconnect.
func (d *DB) TouchSFTPSavedHostLastUsed(id string) error {
	id = strings.TrimSpace(id)
	if id == "" {
		return nil
	}
	_, err := d.sql.Exec(`UPDATE sftp_saved_hosts SET last_used_at = ? WHERE id = ?`, time.Now().UTC(), id)
	return err
}

// DeleteSFTPSavedHost removes one remembered host.
func (d *DB) DeleteSFTPSavedHost(id string) error {
	id = strings.TrimSpace(id)
	if id == "" {
		return fmt.Errorf("saved host id is required")
	}
	res, err := d.sql.Exec(`DELETE FROM sftp_saved_hosts WHERE id = ?`, id)
	if err != nil {
		return fmt.Errorf("delete sftp saved host: %w", err)
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return fmt.Errorf("saved host not found")
	}
	return nil
}

// DeleteAllSFTPSavedHosts clears remembered SFTP sites (wipe-install).
func (d *DB) DeleteAllSFTPSavedHosts() error {
	if _, err := d.sql.Exec(`DELETE FROM sftp_saved_hosts`); err != nil {
		return fmt.Errorf("delete sftp saved hosts: %w", err)
	}
	return nil
}

func nullIfEmpty(s string) any {
	if strings.TrimSpace(s) == "" {
		return nil
	}
	return s
}

func newSFTPSavedHostID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return fmt.Sprintf("%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(b[:])
}
