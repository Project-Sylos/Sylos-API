package apidb

import (
	"fmt"
	"strings"
	"time"

	badger "github.com/dgraph-io/badger/v4"
)

// UserRecord is persisted user state in the API store.
type UserRecord struct {
	ID                     string     `json:"id"`
	Username               string     `json:"username"`
	PasswordHash           string     `json:"passwordHash"`
	Role                   string     `json:"role"`
	CreatedAt              time.Time  `json:"createdAt"`
	Disabled               bool       `json:"disabled"`
	Preferences            string     `json:"preferences,omitempty"`
	RecoveryCodeHash       string     `json:"recoveryCodeHash,omitempty"`
	RecoveryReissueOnLogin bool       `json:"recoveryReissueOnLogin"`
	RecoveryAckPending     bool       `json:"recoveryAckPending"`
	LastLoginAt            *time.Time `json:"lastLoginAt,omitempty"`
	LastLogoutAt           *time.Time `json:"lastLogoutAt,omitempty"`
}

// UserAuditRecord is one user audit event.
type UserAuditRecord struct {
	ID           string    `json:"id"`
	OccurredAt   time.Time `json:"occurredAt"`
	ActorUserID  string    `json:"actorUserId,omitempty"`
	TargetUserID string    `json:"targetUserId,omitempty"`
	Action       string    `json:"action"`
	Metadata     string    `json:"metadata,omitempty"`
}

func (d *DB) PutUser(rec UserRecord) error {
	rec.Username = strings.TrimSpace(rec.Username)
	return d.update(func(txn *badger.Txn) error {
		var existing UserRecord
		exists := getJSON(txn, keyUser(rec.ID), &existing) == nil
		if exists && !strings.EqualFold(existing.Username, rec.Username) {
			if err := clearIndex(txn, keyUserName(existing.Username)); err != nil {
				return err
			}
		}
		if !exists {
			if _, err := txn.Get(keyUserName(rec.Username)); err == nil {
				return fmt.Errorf("username already exists")
			}
		}
		if err := putJSON(txn, keyUser(rec.ID), rec); err != nil {
			return err
		}
		return setIndex(txn, keyUserName(rec.Username), rec.ID)
	})
}

func (d *DB) CreateUser(rec UserRecord) error {
	rec.Username = strings.TrimSpace(rec.Username)
	return d.update(func(txn *badger.Txn) error {
		if _, err := txn.Get(keyUserName(rec.Username)); err == nil {
			return fmt.Errorf("username already exists")
		}
		if err := putJSON(txn, keyUser(rec.ID), rec); err != nil {
			return err
		}
		return setIndex(txn, keyUserName(rec.Username), rec.ID)
	})
}

func (d *DB) GetUser(id string) (UserRecord, error) {
	var rec UserRecord
	err := d.view(func(txn *badger.Txn) error {
		return getJSON(txn, keyUser(id), &rec)
	})
	return rec, err
}

func (d *DB) GetUserByUsername(username string) (UserRecord, error) {
	var rec UserRecord
	err := d.view(func(txn *badger.Txn) error {
		id, err := lookupID(txn, keyUserName(username))
		if err != nil {
			return err
		}
		return getJSON(txn, keyUser(id), &rec)
	})
	return rec, err
}

func (d *DB) UpdateUser(rec UserRecord) error {
	return d.update(func(txn *badger.Txn) error {
		var existing UserRecord
		if err := getJSON(txn, keyUser(rec.ID), &existing); err != nil {
			return err
		}
		if !strings.EqualFold(existing.Username, rec.Username) {
			if err := clearIndex(txn, keyUserName(existing.Username)); err != nil {
				return err
			}
			if _, err := txn.Get(keyUserName(rec.Username)); err == nil {
				return fmt.Errorf("username already exists")
			}
		}
		if err := putJSON(txn, keyUser(rec.ID), rec); err != nil {
			return err
		}
		return setIndex(txn, keyUserName(rec.Username), rec.ID)
	})
}

func (d *DB) DeleteUser(id string) error {
	return d.update(func(txn *badger.Txn) error {
		var existing UserRecord
		if err := getJSON(txn, keyUser(id), &existing); err != nil {
			return err
		}
		if err := clearIndex(txn, keyUserName(existing.Username)); err != nil {
			return err
		}
		return deleteKey(txn, keyUser(id))
	})
}

func (d *DB) ListUsers() ([]UserRecord, error) {
	var out []UserRecord
	err := d.view(func(txn *badger.Txn) error {
		recs, err := listPrefixJSON[UserRecord](txn, prefixUser, func(k []byte) bool {
			return strings.HasPrefix(string(k), prefixUserAudit)
		})
		if err != nil {
			return err
		}
		out = recs
		return nil
	})
	if err != nil {
		return nil, err
	}
	sortUsersByUsername(out)
	return out, nil
}

func sortUsersByUsername(users []UserRecord) {
	for i := 0; i < len(users); i++ {
		for j := i + 1; j < len(users); j++ {
			if strings.ToLower(users[j].Username) < strings.ToLower(users[i].Username) {
				users[i], users[j] = users[j], users[i]
			}
		}
	}
}

func (d *DB) CountUsers() (int, error) {
	var n int
	err := d.view(func(txn *badger.Txn) error {
		var err error
		n, err = countPrefix(txn, prefixUser)
		return err
	})
	return n, err
}

func (d *DB) CountActiveAdminsExcept(id string) (int, error) {
	users, err := d.ListUsers()
	if err != nil {
		return 0, err
	}
	n := 0
	for _, u := range users {
		if u.ID == id {
			continue
		}
		if u.Role == "admin" && !u.Disabled {
			n++
		}
	}
	return n, nil
}

func (d *DB) PutUserAudit(rec UserAuditRecord) error {
	return d.update(func(txn *badger.Txn) error {
		return putJSON(txn, keyUserAudit(rec.ID), rec)
	})
}

func (d *DB) GetUserPreferences(id string) (string, error) {
	rec, err := d.GetUser(id)
	if err != nil {
		return "", err
	}
	return rec.Preferences, nil
}

func (d *DB) SetUserPreferences(id, prefsJSON string) error {
	return d.update(func(txn *badger.Txn) error {
		var rec UserRecord
		if err := getJSON(txn, keyUser(id), &rec); err != nil {
			return err
		}
		rec.Preferences = prefsJSON
		return putJSON(txn, keyUser(id), rec)
	})
}
