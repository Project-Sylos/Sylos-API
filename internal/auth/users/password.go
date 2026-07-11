// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package users

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"

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
	_, err = s.db.Exec(`UPDATE users SET password_hash = ? WHERE id = ?`, string(newHash), userID)
	return err
}

// IssueRecoveryCode replaces any existing recovery code and returns the plaintext once.
// When pendingAck is true, the user must acknowledge storing the code before the attention flag clears.
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
	_, err = s.db.Exec(
		`UPDATE users SET recovery_code_hash = ?, recovery_reissue_on_login = false, recovery_ack_pending = ? WHERE id = ?`,
		recoveryHash, pendingAck, userID,
	)
	if err != nil {
		return "", err
	}
	return display, nil
}

// AcknowledgeRecoveryCode clears the pending acknowledgement flag after the user confirms storage.
func (s *Store) AcknowledgeRecoveryCode(userID string) error {
	res, err := s.db.Exec(`UPDATE users SET recovery_ack_pending = false WHERE id = ?`, userID)
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return ErrNotFound
	}
	return nil
}

// RecoveryCodeStatusFor returns whether the user currently has a recovery code on file.
func (s *Store) RecoveryCodeStatusFor(userID string) (RecoveryCodeStatus, error) {
	var hash sql.NullString
	var ackPending sql.NullBool
	err := s.db.QueryRow(
		`SELECT recovery_code_hash, recovery_ack_pending FROM users WHERE id = ?`,
		userID,
	).Scan(&hash, &ackPending)
	if err != nil {
		return RecoveryCodeStatus{}, err
	}
	hasCode := hash.Valid && hash.String != ""
	needsAck := ackPending.Valid && ackPending.Bool
	return RecoveryCodeStatus{
		HasRecoveryCode: hasCode,
		NeedsAttention:  !hasCode || needsAck,
	}, nil
}

// TakePendingRecoveryReissue issues a new recovery code when the user must receive one after login.
func (s *Store) TakePendingRecoveryReissue(userID string) (string, bool, error) {
	var pending sql.NullBool
	err := s.db.QueryRow(
		`SELECT recovery_reissue_on_login FROM users WHERE id = ?`,
		userID,
	).Scan(&pending)
	if err != nil {
		return "", false, err
	}
	if !pending.Valid || !pending.Bool {
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

	var recoveryHash sql.NullString
	err = s.db.QueryRow(
		`SELECT recovery_code_hash FROM users WHERE id = ?`,
		user.ID,
	).Scan(&recoveryHash)
	if err != nil {
		return err
	}
	if !recoveryHash.Valid || recoveryHash.String == "" || !verifyRecoveryCode(normalized, recoveryHash.String) {
		return ErrInvalidRecoveryCode
	}

	pwHash, err := bcrypt.GenerateFromPassword([]byte(newPassword), s.bcryptCost)
	if err != nil {
		return err
	}
	_, err = s.db.Exec(
		`UPDATE users SET password_hash = ?, recovery_code_hash = NULL, recovery_reissue_on_login = true WHERE id = ?`,
		string(pwHash), user.ID,
	)
	return err
}
