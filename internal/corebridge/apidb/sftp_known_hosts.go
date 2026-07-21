package apidb

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// SFTPKnownHost is a TOFU-pinned SSH host key for an SFTP endpoint.
type SFTPKnownHost struct {
	HostPort    string // "host:port" (lowercase host)
	HostKey     string
	Fingerprint string
	UpdatedAt   time.Time
}

func sftpHostPortKey(host string, port int) string {
	host = strings.TrimSpace(strings.ToLower(host))
	if port <= 0 {
		port = 22
	}
	return fmt.Sprintf("%s:%d", host, port)
}

// UpsertSFTPKnownHost pins or updates a trusted host key.
func (d *DB) UpsertSFTPKnownHost(host string, port int, hostKey, fingerprint string) error {
	hostKey = strings.TrimSpace(hostKey)
	fingerprint = strings.TrimSpace(fingerprint)
	if hostKey == "" || fingerprint == "" {
		return fmt.Errorf("sftp known host: host key and fingerprint are required")
	}
	key := sftpHostPortKey(host, port)
	now := time.Now().UTC()
	_, err := d.sql.Exec(`
		INSERT INTO sftp_known_hosts (host_port, host_key, fingerprint, updated_at)
		VALUES (?, ?, ?, ?)
		ON CONFLICT (host_port) DO UPDATE SET
			host_key = excluded.host_key,
			fingerprint = excluded.fingerprint,
			updated_at = excluded.updated_at
	`, key, hostKey, fingerprint, now)
	if err != nil {
		return fmt.Errorf("upsert sftp known host: %w", err)
	}
	return nil
}

// LookupSFTPKnownHost returns a pinned entry when present.
func (d *DB) LookupSFTPKnownHost(host string, port int) (SFTPKnownHost, bool, error) {
	key := sftpHostPortKey(host, port)
	var row SFTPKnownHost
	var updatedAt time.Time
	err := d.sql.QueryRow(`
		SELECT host_port, host_key, fingerprint, updated_at
		FROM sftp_known_hosts WHERE host_port = ?
	`, key).Scan(&row.HostPort, &row.HostKey, &row.Fingerprint, &updatedAt)
	if err == sql.ErrNoRows {
		return SFTPKnownHost{}, false, nil
	}
	if err != nil {
		return SFTPKnownHost{}, false, err
	}
	row.UpdatedAt = updatedAt
	return row, true, nil
}

// DeleteAllSFTPKnownHosts clears all pinned SFTP host keys (wipe-install).
func (d *DB) DeleteAllSFTPKnownHosts() error {
	if _, err := d.sql.Exec(`DELETE FROM sftp_known_hosts`); err != nil {
		return fmt.Errorf("delete sftp known hosts: %w", err)
	}
	return nil
}

type legacySFTPKnownHostsFile struct {
	Hosts map[string]struct {
		HostKey     string `json:"hostKey"`
		Fingerprint string `json:"fingerprint"`
	} `json:"hosts"`
}

// ImportLegacySFTPKnownHostsFile migrates data/sftp-known-hosts.json into DuckDB once, then removes the file.
func (d *DB) ImportLegacySFTPKnownHostsFile(dataDir string) error {
	path := filepath.Join(dataDir, "sftp-known-hosts.json")
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	var file legacySFTPKnownHostsFile
	if err := json.Unmarshal(data, &file); err != nil {
		return fmt.Errorf("parse legacy sftp known hosts: %w", err)
	}
	for hostPort, entry := range file.Hosts {
		host := hostPort
		port := 22
		if i := strings.LastIndex(hostPort, ":"); i >= 0 {
			host = hostPort[:i]
			if p, err := strconv.Atoi(hostPort[i+1:]); err == nil && p > 0 {
				port = p
			}
		}
		if err := d.UpsertSFTPKnownHost(host, port, entry.HostKey, entry.Fingerprint); err != nil {
			return err
		}
	}
	_ = os.Remove(path)
	return nil
}
