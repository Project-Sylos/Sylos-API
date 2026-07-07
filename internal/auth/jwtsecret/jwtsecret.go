package jwtsecret

import (
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"

	"codeberg.org/Sylos/Sylos-API/internal/corebridge/apidb"
)

// LoadOrGenerate returns the JWT signing secret. configuredSecret from config/env
// overrides the encrypted API database. Otherwise the secret is read from or stored
// in install_config inside sylos.duckdb.
func LoadOrGenerate(db *sql.DB, configuredSecret string) (string, bool, error) {
	if strings.TrimSpace(configuredSecret) != "" {
		return strings.TrimSpace(configuredSecret), false, nil
	}

	var stored string
	err := db.QueryRow(
		`SELECT value FROM install_config WHERE key = ?`, apidb.InstallConfigJWTSecret,
	).Scan(&stored)
	if err == nil {
		stored = strings.TrimSpace(stored)
		if stored != "" {
			return stored, false, nil
		}
	} else if !errors.Is(err, sql.ErrNoRows) {
		return "", false, fmt.Errorf("read jwt secret from API database: %w", err)
	}

	secret, err := generateSecret()
	if err != nil {
		return "", false, err
	}
	_, err = db.Exec(
		`INSERT INTO install_config (key, value) VALUES (?, ?)
		 ON CONFLICT (key) DO UPDATE SET value = excluded.value`,
		apidb.InstallConfigJWTSecret, secret,
	)
	if err != nil {
		return "", false, fmt.Errorf("persist jwt secret in API database: %w", err)
	}
	return secret, true, nil
}

func generateSecret() (string, error) {
	buf := make([]byte, 32)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return hex.EncodeToString(buf), nil
}
