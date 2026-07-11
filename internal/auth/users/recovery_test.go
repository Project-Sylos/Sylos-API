package users

import (
	"path/filepath"
	"testing"
)

func TestRecoveryCodeRoundTrip(t *testing.T) {
	dir := t.TempDir()
	store, err := OpenConn(nil, filepath.Join(dir, "users.duckdb"), 4)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	user, err := store.Create("alice", "secret-pass", RoleUser)
	if err != nil {
		t.Fatal(err)
	}

	code, err := store.IssueRecoveryCode(user.ID, false)
	if err != nil {
		t.Fatal(err)
	}
	status, err := store.RecoveryCodeStatusFor(user.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !status.HasRecoveryCode {
		t.Fatal("expected recovery code on file")
	}

	if err := store.ResetPasswordWithRecoveryCode("alice", code, "new-secret-pass"); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Authenticate("alice", "secret-pass"); err == nil {
		t.Fatal("old password should not work")
	}
	if _, err := store.Authenticate("alice", "new-secret-pass"); err != nil {
		t.Fatalf("new password should work: %v", err)
	}

	if err := store.ResetPasswordWithRecoveryCode("alice", code, "another-pass"); err != ErrInvalidRecoveryCode {
		t.Fatalf("expected invalid recovery code, got %v", err)
	}

	reissued, ok, err := store.TakePendingRecoveryReissue(user.ID)
	if err != nil || !ok || reissued == "" {
		t.Fatalf("expected pending reissue, got code=%q ok=%v err=%v", reissued, ok, err)
	}
	if _, ok, err := store.TakePendingRecoveryReissue(user.ID); err != nil || ok {
		t.Fatalf("expected no second reissue without flag, ok=%v err=%v", ok, err)
	}
}

func TestChangePassword(t *testing.T) {
	dir := t.TempDir()
	store, err := OpenConn(nil, filepath.Join(dir, "users.duckdb"), 4)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	user, err := store.Create("bob", "old-pass", RoleUser)
	if err != nil {
		t.Fatal(err)
	}

	if err := store.ChangePassword(user.ID, "wrong", "new-pass"); err != ErrInvalidCreds {
		t.Fatalf("expected invalid creds, got %v", err)
	}
	if err := store.ChangePassword(user.ID, "old-pass", "new-pass"); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Authenticate("bob", "new-pass"); err != nil {
		t.Fatal(err)
	}
}
