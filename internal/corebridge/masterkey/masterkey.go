package masterkey

import (
	"encoding/base64"
	"errors"
	"fmt"
	"strings"

	"codeberg.org/Sylos/Sylos-FS/pkg/credentials"
	"github.com/zalando/go-keyring"
)

const (
	keyringService = "Sylos"
	keyringUser    = "install-master-key"
)

// Resolve returns the per-install 32-byte AES master key used to wrap per-migration keys and API-side secrets.
// The key is stored only in the OS keyring.
func Resolve() ([]byte, error) {
	return resolveFromKeyring()
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
			"failed to read install master key from OS keyring (service %q, user %q): %w; unlock or configure the desktop keyring and retry",
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
			"failed to store new install master key in OS keyring (service %q, user %q): %w; unlock or configure the desktop keyring and retry",
			keyringService, keyringUser, err,
		)
	}
	return key, nil
}

func decodeKey(raw string) ([]byte, error) {
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
