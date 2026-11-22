package oauthobscure

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestObscureRevealRoundTrip(t *testing.T) {
	key := make([]byte, 32)
	for i := range key {
		key[i] = byte(i + 3)
	}
	enc, err := Obscure(key, "secret-value")
	if err != nil {
		t.Fatal(err)
	}
	if enc == "secret-value" || enc[:4] != "enc:" {
		t.Fatalf("enc=%q", enc)
	}
	got, err := Reveal(key, enc)
	if err != nil {
		t.Fatal(err)
	}
	if got != "secret-value" {
		t.Fatalf("got %q", got)
	}
	plain, err := Reveal(key, "already-plain")
	if err != nil {
		t.Fatal(err)
	}
	if plain != "already-plain" {
		t.Fatalf("plain passthrough=%q", plain)
	}
}

func TestEncryptEnvFileRoundTrip(t *testing.T) {
	dir := t.TempDir()
	key := make([]byte, 32)
	for i := range key {
		key[i] = byte(i + 9)
	}
	envPath := filepath.Join(dir, "oauth.env")
	content := `# test
SYLOS_DROPBOX_CLIENT_ID?=dbx-id
SYLOS_DROPBOX_CLIENT_SECRET?=dbx-secret
SYLOS_BOX_CLIENT_ID?=
SYLOS_BOX_CLIENT_SECRET?=
`
	if err := os.WriteFile(envPath, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := EncryptEnvFileInPlace(envPath, key); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(envPath)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(raw), "enc:") {
		t.Fatalf("expected enc value in file:\n%s", raw)
	}
	vals, _, err := LoadEnvFile(envPath, key)
	if err != nil {
		t.Fatal(err)
	}
	if vals.DropboxClientID != "dbx-id" || vals.DropboxClientSecret != "dbx-secret" {
		t.Fatalf("vals=%+v", vals)
	}
}
