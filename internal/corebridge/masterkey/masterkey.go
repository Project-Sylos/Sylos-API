package masterkey

import (
	"encoding/base64"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"codeberg.org/Sylos/Sylos-FS/pkg/credentials"
	"github.com/zalando/go-keyring"
)

const (
	keyringService = "Sylos"
	keyringUser    = "install-master-key"
	envMasterKey   = "SYLOS_MASTER_KEY"
	envKeyFile     = "SYLOS_KEY_FILE"
)

// Config controls how the install master key is resolved and stored.
type Config struct {
	DataDir       string
	OAuthCredsDir string
	UseEnvKeys    bool
}

// Resolve returns the per-install 32-byte AES master key for DuckDB file encryption.
func Resolve(cfg Config) ([]byte, error) {
	if cfg.UseEnvKeys {
		return resolveFromEnv(cfg)
	}
	if err := rejectEnvOverrides(); err != nil {
		return nil, err
	}
	return resolveFromKeyring()
}

func rejectEnvOverrides() error {
	if raw := strings.TrimSpace(os.Getenv(envMasterKey)); raw != "" {
		return fmt.Errorf("%s is set but Sylos was not started with --use-env-keys", envMasterKey)
	}
	if raw := strings.TrimSpace(os.Getenv(envKeyFile)); raw != "" {
		return fmt.Errorf("%s is set but Sylos was not started with --use-env-keys", envKeyFile)
	}
	return nil
}

func resolveFromKeyring() ([]byte, error) {
	encoded, err := keyring.Get(keyringService, keyringUser)
	if err == nil {
		if encoded == "" {
			return nil, fmt.Errorf("OS keyring entry for service %q / user %q is empty", keyringService, keyringUser)
		}
		key, decErr := decodeKey(encoded)
		if decErr != nil {
			return nil, fmt.Errorf("OS keyring entry for service %q / user %q is invalid: %w", keyringService, keyringUser, decErr)
		}
		return key, nil
	}
	if !errors.Is(err, keyring.ErrNotFound) {
		return nil, fmt.Errorf(
			"failed to read install master key from OS keyring (service %q, user %q): %w; ensure the OS keyring is available and unlocked, or start with --use-env-keys",
			keyringService, keyringUser, err,
		)
	}

	key, err := credentials.GenerateMasterKey()
	if err != nil {
		return nil, err
	}
	encoded = base64.StdEncoding.EncodeToString(key)
	if err := keyring.Set(keyringService, keyringUser, encoded); err != nil {
		return nil, fmt.Errorf(
			"failed to store new install master key in OS keyring (service %q, user %q): %w; ensure the OS keyring is available and unlocked, or start with --use-env-keys",
			keyringService, keyringUser, err,
		)
	}
	return key, nil
}

func resolveFromEnv(cfg Config) ([]byte, error) {
	if raw := strings.TrimSpace(os.Getenv(envMasterKey)); raw != "" {
		key, err := decodeKey(raw)
		if err != nil {
			return nil, fmt.Errorf("master key: %s: %w", envMasterKey, err)
		}
		return key, nil
	}

	envPath := envFilePath(cfg)
	if data, err := os.ReadFile(envPath); err == nil {
		key, decErr := decodeKey(strings.TrimSpace(string(data)))
		if decErr == nil {
			return key, nil
		}
		return nil, fmt.Errorf("master key file %s: %w", envPath, decErr)
	} else if !os.IsNotExist(err) {
		return nil, fmt.Errorf("read master key file %s: %w", envPath, err)
	}

	key, err := credentials.GenerateMasterKey()
	if err != nil {
		return nil, err
	}
	if err := writeEnvFile(envPath, key); err != nil {
		return nil, fmt.Errorf("write master key file %s: %w", envPath, err)
	}
	return key, nil
}

func envFilePath(cfg Config) string {
	dir := cfg.OAuthCredsDir
	if dir == "" {
		dir = "creds"
	}
	return filepath.Join(dir, ".env")
}

func writeEnvFile(path string, key []byte) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	content := fmt.Sprintf("%s=%s\n", envMasterKey, base64.StdEncoding.EncodeToString(key))
	return os.WriteFile(path, []byte(content), 0o600)
}

func decodeKey(raw string) ([]byte, error) {
	if strings.HasPrefix(raw, envMasterKey+"=") {
		raw = strings.TrimPrefix(raw, envMasterKey+"=")
	}
	raw = strings.TrimSpace(raw)
	if key, err := base64.StdEncoding.DecodeString(raw); err == nil {
		if len(key) == credentials.KeySize {
			return key, nil
		}
	}
	if len(raw) == credentials.KeySize*2 {
		key := make([]byte, credentials.KeySize)
		for i := 0; i < credentials.KeySize; i++ {
			var b byte
			if _, err := fmt.Sscanf(raw[i*2:i*2+2], "%02x", &b); err != nil {
				return nil, errors.New("invalid hex key")
			}
			key[i] = b
		}
		return key, nil
	}
	return nil, fmt.Errorf("key must be %d bytes (base64 or hex)", credentials.KeySize)
}
