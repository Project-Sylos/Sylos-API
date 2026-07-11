package users

import (
	"encoding/json"
	"strings"
	"time"

	"github.com/oklog/ulid/v2"
)

const (
	AuditLoginSuccess = "login.success"
	AuditLoginFailed  = "login.failed"
	AuditLogout       = "logout"
	AuditUserCreate   = "user.create"
	AuditUserUpdate   = "user.update"
	AuditUserDelete   = "user.delete"
)

type AuditEvent struct {
	ID           string
	OccurredAt   time.Time
	ActorUserID  string
	TargetUserID string
	Action       string
	Metadata     string
}

func (s *Store) RecordAuditEvent(ev AuditEvent) error {
	if strings.TrimSpace(ev.Action) == "" {
		return nil
	}
	if ev.ID == "" {
		ev.ID = ulid.Make().String()
	}
	if ev.OccurredAt.IsZero() {
		ev.OccurredAt = time.Now().UTC()
	}
	_, err := s.db.Exec(
		`INSERT INTO user_audit_events (id, occurred_at, actor_user_id, target_user_id, action, metadata)
		 VALUES (?, ?, ?, ?, ?, ?)`,
		ev.ID,
		ev.OccurredAt.Format(time.RFC3339),
		nullIfEmpty(ev.ActorUserID),
		nullIfEmpty(ev.TargetUserID),
		ev.Action,
		nullIfEmpty(ev.Metadata),
	)
	return err
}

func (s *Store) RecordLogin(userID string) error {
	now := time.Now().UTC()
	if _, err := s.db.Exec(
		`UPDATE users SET last_login_at = ? WHERE id = ?`,
		now.Format(time.RFC3339),
		userID,
	); err != nil {
		return err
	}
	return s.RecordAuditEvent(AuditEvent{
		ActorUserID:  userID,
		TargetUserID: userID,
		Action:       AuditLoginSuccess,
		OccurredAt:   now,
	})
}

func (s *Store) RecordLogout(userID string) error {
	now := time.Now().UTC()
	if _, err := s.db.Exec(
		`UPDATE users SET last_logout_at = ? WHERE id = ?`,
		now.Format(time.RFC3339),
		userID,
	); err != nil {
		return err
	}
	return s.RecordAuditEvent(AuditEvent{
		ActorUserID:  userID,
		TargetUserID: userID,
		Action:       AuditLogout,
		OccurredAt:   now,
	})
}

func (s *Store) RecordLoginFailed(username string) error {
	meta, _ := json.Marshal(map[string]string{"username": strings.TrimSpace(username)})
	return s.RecordAuditEvent(AuditEvent{
		Action:   AuditLoginFailed,
		Metadata: string(meta),
	})
}

func nullIfEmpty(value string) any {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil
	}
	return value
}
