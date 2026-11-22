// Package oauthobscure stores Sylos OAuth app credentials as enc:… values in oauth.env,
// revealed with a committed oauth.key at build or startup. This keeps plaintext out of
// casual diffs; it is not a secrecy boundary once key and ciphertext are both available.
package oauthobscure

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
)

const obscuredPrefix = "enc:"

// LoadKeyFile reads a 32-byte AES key from path. Accepts raw 32 bytes, hex (64 chars), or standard base64.
func LoadKeyFile(path string) ([]byte, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return parseKey(string(raw))
}

func parseKey(s string) ([]byte, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil, errors.New("oauth key is empty")
	}
	if key, err := hex.DecodeString(s); err == nil && len(key) == 32 {
		return key, nil
	}
	if key, err := base64.StdEncoding.DecodeString(s); err == nil && len(key) == 32 {
		return key, nil
	}
	if key, err := base64.RawStdEncoding.DecodeString(s); err == nil && len(key) == 32 {
		return key, nil
	}
	b := []byte(s)
	if len(b) == 32 {
		return b, nil
	}
	return nil, fmt.Errorf("oauth key must be 32 bytes (hex, base64, or raw), got %d bytes after parse", len(b))
}

// Obscure encrypts plaintext with AES-256-GCM. Output is enc:<base64(nonce|ciphertext|tag)>.
func Obscure(key []byte, plaintext string) (string, error) {
	if len(key) != 32 {
		return "", errors.New("oauth key must be 32 bytes")
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return "", err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", err
	}
	nonce := make([]byte, gcm.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return "", err
	}
	out := gcm.Seal(nonce, nonce, []byte(plaintext), nil)
	return obscuredPrefix + base64.RawURLEncoding.EncodeToString(out), nil
}

// Reveal decrypts a value from Obscure. Plaintext without enc: prefix is returned as-is (migration aid).
func Reveal(key []byte, value string) (string, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return "", nil
	}
	if !strings.HasPrefix(value, obscuredPrefix) {
		return value, nil
	}
	if len(key) != 32 {
		return "", errors.New("oauth key must be 32 bytes")
	}
	raw, err := base64.RawURLEncoding.DecodeString(strings.TrimPrefix(value, obscuredPrefix))
	if err != nil {
		return "", fmt.Errorf("decode obscured oauth value: %w", err)
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return "", err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", err
	}
	ns := gcm.NonceSize()
	if len(raw) < ns {
		return "", errors.New("obscured oauth value too short")
	}
	plain, err := gcm.Open(nil, raw[:ns], raw[ns:], nil)
	if err != nil {
		return "", fmt.Errorf("reveal obscured oauth value: %w", err)
	}
	return string(plain), nil
}
