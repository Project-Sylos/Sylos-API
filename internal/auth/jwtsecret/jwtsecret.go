package jwtsecret

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"

	"codeberg.org/Sylos/Sylos-API/internal/corebridge/apidb"
)

// LoadOrGenerate returns the JWT signing secret. configuredSecret from config/env
// overrides the encrypted API database. Otherwise the secret is read from or stored
// in install config inside sylos.api.
func LoadOrGenerate(db *apidb.DB, configuredSecret string) (string, bool, error) {
	if strings.TrimSpace(configuredSecret) != "" {
		return strings.TrimSpace(configuredSecret), false, nil
	}
	if db == nil {
		return "", false, fmt.Errorf("api database is required")
	}

	stored, err := db.GetInstallConfig(apidb.InstallConfigJWTSecret)
	if err == nil {
		stored = strings.TrimSpace(stored)
		if stored != "" {
			return stored, false, nil
		}
	} else if !errors.Is(err, apidb.ErrNotFound) {
		return "", false, fmt.Errorf("read jwt secret from API database: %w", err)
	}

	secret, err := generateSecret()
	if err != nil {
		return "", false, err
	}
	if err := db.SetInstallConfig(apidb.InstallConfigJWTSecret, secret); err != nil {
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
