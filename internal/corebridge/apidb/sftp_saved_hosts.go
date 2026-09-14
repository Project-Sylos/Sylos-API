package apidb

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"codeberg.org/Sylos/Sylos-FS/pkg/credentials"
	badger "github.com/dgraph-io/badger/v4"
)

// SFTPSavedHost is a FileZilla-style remembered SFTP site (no live health checks).
type SFTPSavedHost struct {
	ID            string     `json:"id"`
	DisplayName   string     `json:"displayName"`
	Host          string     `json:"host"`
	Port          int        `json:"port"`
	Username      string     `json:"username"`
	AuthMethod    string     `json:"authMethod"`
	Password      string     `json:"-"`
	PrivateKey    string     `json:"-"`
	KeyPassphrase string     `json:"-"`
	SecretsBlob   []byte     `json:"secretsBlob"`
	HostKey       string     `json:"hostKey,omitempty"`
	CreatedAt     time.Time  `json:"createdAt"`
	UpdatedAt     time.Time  `json:"updatedAt"`
	LastUsedAt    *time.Time `json:"lastUsedAt,omitempty"`
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
	var out []SFTPSavedHost
	err := d.view(func(txn *badger.Txn) error {
		recs, err := listPrefixJSON[SFTPSavedHost](txn, prefixSftpSaved, func(k []byte) bool {
			return strings.HasPrefix(string(k), prefixSftpSavedEp)
		})
		if err != nil {
			return err
		}
		out = recs
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("list sftp saved hosts: %w", err)
	}
	sortSFTPSavedHosts(out)
	return out, nil
}

func sortSFTPSavedHosts(hosts []SFTPSavedHost) {
	for i := 0; i < len(hosts); i++ {
		for j := i + 1; j < len(hosts); j++ {
			a := savedHostSortKey(hosts[i])
			b := savedHostSortKey(hosts[j])
			if b > a {
				hosts[i], hosts[j] = hosts[j], hosts[i]
			}
		}
	}
}

func savedHostSortKey(h SFTPSavedHost) string {
	t := h.UpdatedAt
	if h.LastUsedAt != nil {
		t = *h.LastUsedAt
	}
	return t.Format(time.RFC3339Nano) + h.DisplayName
}

// GetSFTPSavedHost returns one host with secrets decrypted for reconnect.
func (d *DB) GetSFTPSavedHost(id string) (SFTPSavedHost, error) {
	id = strings.TrimSpace(id)
	if id == "" {
		return SFTPSavedHost{}, fmt.Errorf("saved host id is required")
	}
	var h SFTPSavedHost
	err := d.view(func(txn *badger.Txn) error {
		return getJSON(txn, keySftpSaved(id), &h)
	})
	if errors.Is(err, ErrNotFound) {
		return SFTPSavedHost{}, fmt.Errorf("saved host not found")
	}
	if err != nil {
		return SFTPSavedHost{}, err
	}
	secrets, err := d.decryptSFTPSavedSecrets(h.SecretsBlob)
	if err != nil {
		return SFTPSavedHost{}, fmt.Errorf("decrypt saved host secrets: %w", err)
	}
	h.Password = secrets.Password
	h.PrivateKey = secrets.PrivateKey
	h.KeyPassphrase = secrets.KeyPassphrase
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
	host.SecretsBlob = blob

	now := time.Now().UTC()
	err = d.update(func(txn *badger.Txn) error {
		if host.ID == "" {
			if id, lookupErr := lookupID(txn, keySftpSavedEp(host.Host, host.Port, host.Username)); lookupErr == nil {
				host.ID = id
			} else if errors.Is(lookupErr, ErrNotFound) {
				host.ID = newSFTPSavedHostID()
			} else {
				return lookupErr
			}
		}

		var existing SFTPSavedHost
		exists := getJSON(txn, keySftpSaved(host.ID), &existing) == nil
		if exists {
			host.CreatedAt = existing.CreatedAt
			if !strings.EqualFold(existing.Host, host.Host) || existing.Port != host.Port || existing.Username != host.Username {
				if err := clearIndex(txn, keySftpSavedEp(existing.Host, existing.Port, existing.Username)); err != nil {
					return err
				}
			}
		} else {
			host.CreatedAt = now
		}
		host.UpdatedAt = now
		host.LastUsedAt = &now

		if err := putJSON(txn, keySftpSaved(host.ID), host); err != nil {
			return fmt.Errorf("upsert sftp saved host: %w", err)
		}
		return setIndex(txn, keySftpSavedEp(host.Host, host.Port, host.Username), host.ID)
	})
	if err != nil {
		return SFTPSavedHost{}, err
	}

	host.Password = ""
	host.PrivateKey = ""
	host.KeyPassphrase = ""
	host.SecretsBlob = nil
	return host, nil
}

// TouchSFTPSavedHostLastUsed updates last_used_at after a successful reconnect.
func (d *DB) TouchSFTPSavedHostLastUsed(id string) error {
	id = strings.TrimSpace(id)
	if id == "" {
		return nil
	}
	return d.update(func(txn *badger.Txn) error {
		var h SFTPSavedHost
		if err := getJSON(txn, keySftpSaved(id), &h); err != nil {
			return err
		}
		now := time.Now().UTC()
		h.LastUsedAt = &now
		return putJSON(txn, keySftpSaved(id), h)
	})
}

// DeleteSFTPSavedHost removes one remembered host.
func (d *DB) DeleteSFTPSavedHost(id string) error {
	id = strings.TrimSpace(id)
	if id == "" {
		return fmt.Errorf("saved host id is required")
	}
	return d.update(func(txn *badger.Txn) error {
		var h SFTPSavedHost
		if err := getJSON(txn, keySftpSaved(id), &h); err != nil {
			if errors.Is(err, ErrNotFound) {
				return fmt.Errorf("saved host not found")
			}
			return err
		}
		if err := clearIndex(txn, keySftpSavedEp(h.Host, h.Port, h.Username)); err != nil {
			return err
		}
		return deleteKey(txn, keySftpSaved(id))
	})
}

// DeleteAllSFTPSavedHosts clears remembered SFTP sites (wipe-install).
func (d *DB) DeleteAllSFTPSavedHosts() error {
	return d.update(func(txn *badger.Txn) error {
		if err := deletePrefix(txn, prefixSftpSavedEp); err != nil {
			return err
		}
		return deletePrefix(txn, prefixSftpSaved)
	})
}

func newSFTPSavedHostID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return fmt.Sprintf("%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(b[:])
}
