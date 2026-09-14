package users

import (
	"testing"

	"codeberg.org/Sylos/Sylos-API/internal/corebridge/apidb"
)

func openTestStore(t *testing.T) *Store {
	t.Helper()
	dir := t.TempDir()
	key := make([]byte, 32)
	for i := range key {
		key[i] = byte(i + 3)
	}
	db, err := apidb.Open(dir, key)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	store, err := Open(db, 4)
	if err != nil {
		t.Fatal(err)
	}
	return store
}

func TestRecoveryCodeRoundTrip(t *testing.T) {
	store := openTestStore(t)
	user, err := store.Create("alice", "password123", RoleUser)
	if err != nil {
		t.Fatal(err)
	}
	code, err := store.IssueRecoveryCode(user.ID, true)
	if err != nil {
		t.Fatal(err)
	}
	status, err := store.RecoveryCodeStatusFor(user.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !status.HasRecoveryCode || !status.NeedsAttention {
		t.Fatalf("status=%+v", status)
	}
	if err := store.AcknowledgeRecoveryCode(user.ID); err != nil {
		t.Fatal(err)
	}
	status, err = store.RecoveryCodeStatusFor(user.ID)
	if err != nil {
		t.Fatal(err)
	}
	if status.NeedsAttention {
		t.Fatalf("expected ack to clear attention: %+v", status)
	}
	if err := store.ResetPasswordWithRecoveryCode(user.Username, code, "newpassword456"); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Authenticate(user.Username, "newpassword456"); err != nil {
		t.Fatalf("login with new password: %v", err)
	}
}

func TestRecoveryReissueOnLogin(t *testing.T) {
	store := openTestStore(t)
	user, err := store.Create("bob", "password123", RoleUser)
	if err != nil {
		t.Fatal(err)
	}
	code, err := store.IssueRecoveryCode(user.ID, false)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.ResetPasswordWithRecoveryCode(user.Username, code, "newpassword456"); err != nil {
		t.Fatal(err)
	}
	reissue, issued, err := store.TakePendingRecoveryReissue(user.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !issued || reissue == "" {
		t.Fatalf("expected reissue code, got issued=%v code=%q", issued, reissue)
	}
}
