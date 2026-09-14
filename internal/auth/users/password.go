// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package users

import (
	"errors"
	"fmt"
	"strings"

	"codeberg.org/Sylos/Sylos-API/internal/corebridge/apidb"
	"golang.org/x/crypto/bcrypt"
)

// RecoveryCodeStatus reports whether the account has an active recovery code.
type RecoveryCodeStatus struct {
	HasRecoveryCode bool `json:"hasRecoveryCode"`
	NeedsAttention  bool `json:"needsAttention"`
}

// ChangePassword verifies the current password and sets a new one.
func (s *Store) ChangePassword(userID, currentPassword, newPassword string) error {
	currentPassword = strings.TrimSpace(currentPassword)
	newPassword = strings.TrimSpace(newPassword)
	if currentPassword == "" || newPassword == "" {
		return fmt.Errorf("current and new password are required")
	}
	if currentPassword == newPassword {
		return fmt.Errorf("new password must differ from current password")
	}

	user, hash, err := s.findByID(userID)
	if err != nil {
		return err
	}
	if user.Disabled {
		return ErrDisabled
	}
	if err := bcrypt.CompareHashAndPassword([]byte(hash), []byte(currentPassword)); err != nil {
		return ErrInvalidCreds
	}

	newHash, err := bcrypt.GenerateFromPassword([]byte(newPassword), s.bcryptCost)
	if err != nil {
		return err
	}
	rec, err := s.db.GetUser(userID)
	if err != nil {
		return err
	}
	rec.PasswordHash = string(newHash)
	return s.db.UpdateUser(rec)
}

// IssueRecoveryCode replaces any existing recovery code and returns the plaintext once.
func (s *Store) IssueRecoveryCode(userID string, pendingAck bool) (string, error) {
	if _, err := s.GetByID(userID); err != nil {
		return "", err
	}
	display, normalized, err := s.newRecoveryCode()
	if err != nil {
		return "", err
	}
	recoveryHash, err := hashRecoveryCode(normalized, s.bcryptCost)
	if err != nil {
		return "", err
	}
	rec, err := s.db.GetUser(userID)
	if err != nil {
		return "", err
	}
	rec.RecoveryCodeHash = recoveryHash
	rec.RecoveryReissueOnLogin = false
	rec.RecoveryAckPending = pendingAck
	if err := s.db.UpdateUser(rec); err != nil {
		return "", err
	}
	return display, nil
}

// AcknowledgeRecoveryCode clears the pending acknowledgement flag after the user confirms storage.
func (s *Store) AcknowledgeRecoveryCode(userID string) error {
	rec, err := s.db.GetUser(userID)
	if err != nil {
		if errors.Is(err, apidb.ErrNotFound) {
			return ErrNotFound
		}
		return err
	}
	rec.RecoveryAckPending = false
	return s.db.UpdateUser(rec)
}

// RecoveryCodeStatusFor returns whether the user currently has a recovery code on file.
func (s *Store) RecoveryCodeStatusFor(userID string) (RecoveryCodeStatus, error) {
	rec, err := s.db.GetUser(userID)
	if err != nil {
		return RecoveryCodeStatus{}, err
	}
	hasCode := rec.RecoveryCodeHash != ""
	return RecoveryCodeStatus{
		HasRecoveryCode: hasCode,
		NeedsAttention:  !hasCode || rec.RecoveryAckPending,
	}, nil
}

// TakePendingRecoveryReissue issues a new recovery code when the user must receive one after login.
func (s *Store) TakePendingRecoveryReissue(userID string) (string, bool, error) {
	rec, err := s.db.GetUser(userID)
	if err != nil {
		return "", false, err
	}
	if !rec.RecoveryReissueOnLogin {
		return "", false, nil
	}
	code, err := s.IssueRecoveryCode(userID, true)
	if err != nil {
		return "", false, err
	}
	return code, true, nil
}

// ResetPasswordWithRecoveryCode consumes a one-time recovery code and sets a new password.
func (s *Store) ResetPasswordWithRecoveryCode(username, recoveryCode, newPassword string) error {
	newPassword = strings.TrimSpace(newPassword)
	if newPassword == "" {
		return fmt.Errorf("new password is required")
	}
	normalized := NormalizeRecoveryCode(recoveryCode)
	if normalized == "" {
		return ErrInvalidRecoveryCode
	}

	user, _, err := s.findByUsername(username)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return ErrInvalidRecoveryCode
		}
		return err
	}
	if user.Disabled {
		return ErrDisabled
	}

	rec, err := s.db.GetUser(user.ID)
	if err != nil {
		return err
	}
	if rec.RecoveryCodeHash == "" || !verifyRecoveryCode(normalized, rec.RecoveryCodeHash) {
		return ErrInvalidRecoveryCode
	}

	pwHash, err := bcrypt.GenerateFromPassword([]byte(newPassword), s.bcryptCost)
	if err != nil {
		return err
	}
	rec.PasswordHash = string(pwHash)
	rec.RecoveryCodeHash = ""
	rec.RecoveryReissueOnLogin = true
	return s.db.UpdateUser(rec)
}
