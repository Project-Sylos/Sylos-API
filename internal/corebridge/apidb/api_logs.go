package apidb

import (
	"encoding/json"
	"fmt"
	"time"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/google/uuid"
)

const prefixAPILog = "apilog:"

// APILogRecord is one install-wide API/process log line.
type APILogRecord struct {
	ID        string    `json:"id"`
	At        time.Time `json:"at"`
	Level     string    `json:"level"`
	Message   string    `json:"message"`
	Component string    `json:"component,omitempty"`
}

func keyAPILog(unixNano int64, id string) []byte {
	return fmt.Appendf(nil, "%s%020d:%s", prefixAPILog, unixNano, id)
}

// AppendAPILog persists one API/process log into sylos.api/.
func (d *DB) AppendAPILog(level, message, component string) error {
	if d == nil || d.db == nil {
		return nil
	}
	rec := APILogRecord{
		ID:        uuid.New().String(),
		At:        time.Now().UTC(),
		Level:     level,
		Message:   message,
		Component: component,
	}
	b, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	return d.db.Update(func(txn *badger.Txn) error {
		return txn.Set(keyAPILog(rec.At.UnixNano(), rec.ID), b)
	})
}
