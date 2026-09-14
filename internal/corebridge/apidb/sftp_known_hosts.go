package apidb

import (
	"errors"
	"fmt"
	"strings"
	"time"

	badger "github.com/dgraph-io/badger/v4"
)

// SFTPKnownHost is a TOFU-pinned SSH host key for an SFTP endpoint.
type SFTPKnownHost struct {
	HostPort    string    `json:"hostPort"`
	HostKey     string    `json:"hostKey"`
	Fingerprint string    `json:"fingerprint"`
	UpdatedAt   time.Time `json:"updatedAt"`
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
	row := SFTPKnownHost{
		HostPort:    sftpHostPortKey(host, port),
		HostKey:     hostKey,
		Fingerprint: fingerprint,
		UpdatedAt:   time.Now().UTC(),
	}
	return d.update(func(txn *badger.Txn) error {
		return putJSON(txn, keySftpKnown(row.HostPort), row)
	})
}

// LookupSFTPKnownHost returns a pinned entry when present.
func (d *DB) LookupSFTPKnownHost(host string, port int) (SFTPKnownHost, bool, error) {
	key := sftpHostPortKey(host, port)
	var row SFTPKnownHost
	err := d.view(func(txn *badger.Txn) error {
		return getJSON(txn, keySftpKnown(key), &row)
	})
	if errors.Is(err, ErrNotFound) {
		return SFTPKnownHost{}, false, nil
	}
	if err != nil {
		return SFTPKnownHost{}, false, err
	}
	return row, true, nil
}

// DeleteAllSFTPKnownHosts clears all pinned SFTP host keys (wipe-install).
func (d *DB) DeleteAllSFTPKnownHosts() error {
	return d.update(func(txn *badger.Txn) error {
		return deletePrefix(txn, prefixSftpKnown)
	})
}