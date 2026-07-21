package apidb

import "testing"

func TestSFTPSavedHostRoundTrip(t *testing.T) {
	dir := t.TempDir()
	key := make([]byte, 32)
	for i := range key {
		key[i] = byte(i + 3)
	}
	db, err := Open(dir, key)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })

	saved, err := db.UpsertSFTPSavedHost(SFTPSavedHost{
		DisplayName: "Lab NAS",
		Host:        "NAS.local",
		Port:        22,
		Username:    "backup",
		AuthMethod:  "password",
		Password:    "secret",
		HostKey:     "ssh-ed25519 AAAA",
	})
	if err != nil {
		t.Fatal(err)
	}
	if saved.ID == "" || saved.Host != "nas.local" || saved.Password != "" {
		t.Fatalf("upsert summary = %+v", saved)
	}

	list, err := db.ListSFTPSavedHosts()
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 1 || list[0].DisplayName != "Lab NAS" {
		t.Fatalf("list = %+v", list)
	}

	detail, err := db.GetSFTPSavedHost(saved.ID)
	if err != nil {
		t.Fatal(err)
	}
	if detail.Password != "secret" || detail.HostKey != "ssh-ed25519 AAAA" {
		t.Fatalf("detail = %+v", detail)
	}

	// Same endpoint upserts in place.
	again, err := db.UpsertSFTPSavedHost(SFTPSavedHost{
		Host:       "nas.local",
		Port:       22,
		Username:   "backup",
		AuthMethod: "password",
		Password:   "rotated",
	})
	if err != nil {
		t.Fatal(err)
	}
	if again.ID != saved.ID {
		t.Fatalf("expected same id, got %q want %q", again.ID, saved.ID)
	}
	detail, err = db.GetSFTPSavedHost(saved.ID)
	if err != nil {
		t.Fatal(err)
	}
	if detail.Password != "rotated" {
		t.Fatalf("password = %q", detail.Password)
	}

	if err := db.DeleteSFTPSavedHost(saved.ID); err != nil {
		t.Fatal(err)
	}
	list, err = db.ListSFTPSavedHosts()
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 0 {
		t.Fatalf("after delete list=%+v", list)
	}
}
