// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package users

import (
	"crypto/rand"
	"errors"
	"fmt"
	"math/big"
	"strings"

	"golang.org/x/crypto/bcrypt"
)

var ErrInvalidRecoveryCode = errors.New("invalid recovery code")

const recoveryCodeLength = 24
const recoveryCodeAlphabet = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"

// NormalizeRecoveryCode strips separators and uppercases user input.
func NormalizeRecoveryCode(code string) string {
	code = strings.ToUpper(strings.TrimSpace(code))
	code = strings.ReplaceAll(code, "-", "")
	code = strings.ReplaceAll(code, " ", "")
	return code
}

// FormatRecoveryCode groups a normalized code for display.
func FormatRecoveryCode(normalized string) string {
	normalized = NormalizeRecoveryCode(normalized)
	if len(normalized) != recoveryCodeLength {
		return normalized
	}
	return fmt.Sprintf("%s-%s-%s-%s",
		normalized[0:6],
		normalized[6:12],
		normalized[12:18],
		normalized[18:24],
	)
}

// GenerateRecoveryCode returns a display-formatted one-time recovery code.
func GenerateRecoveryCode() (string, error) {
	var b strings.Builder
	b.Grow(recoveryCodeLength)
	for i := 0; i < recoveryCodeLength; i++ {
		n, err := rand.Int(rand.Reader, big.NewInt(int64(len(recoveryCodeAlphabet))))
		if err != nil {
			return "", err
		}
		b.WriteByte(recoveryCodeAlphabet[n.Int64()])
	}
	return FormatRecoveryCode(b.String()), nil
}

func hashRecoveryCode(normalized string, cost int) (string, error) {
	if normalized == "" {
		return "", fmt.Errorf("empty recovery code")
	}
	hash, err := bcrypt.GenerateFromPassword([]byte(normalized), cost)
	if err != nil {
		return "", err
	}
	return string(hash), nil
}

func verifyRecoveryCode(normalized, hash string) bool {
	if normalized == "" || hash == "" {
		return false
	}
	return bcrypt.CompareHashAndPassword([]byte(hash), []byte(normalized)) == nil
}

func (s *Store) newRecoveryCode() (display, normalized string, err error) {
	display, err = GenerateRecoveryCode()
	if err != nil {
		return "", "", err
	}
	normalized = NormalizeRecoveryCode(display)
	return display, normalized, nil
}
