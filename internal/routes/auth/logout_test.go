package auth

import (
	"bytes"
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

// TestLogoutSourceNeverStopsMigrations is a regression guard: logout must stay
// an audit-only route with no migration manager / stop / teardown coupling.
func TestLogoutSourceNeverStopsMigrations(t *testing.T) {
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	src, err := os.ReadFile(filepath.Join(filepath.Dir(thisFile), "logout.go"))
	if err != nil {
		t.Fatalf("read logout.go: %v", err)
	}

	forbidden := []string{
		"StopMigration",
		"ForceStop",
		"force_stop",
		"corebridge",
		"engineMgr",
		"runtimeByID",
		"Shutdown",
		"migration.Manager",
		"Manager)",
	}
	for _, needle := range forbidden {
		if bytes.Contains(src, []byte(needle)) {
			t.Fatalf("logout.go must not reference %q (logout must not touch migrations)", needle)
		}
	}

	regSrc, err := os.ReadFile(filepath.Join(filepath.Dir(thisFile), "register.go"))
	if err != nil {
		t.Fatalf("read register.go: %v", err)
	}
	// Protected auth routes take only userStore (+ middleware), never a migration manager.
	if !bytes.Contains(regSrc, []byte("RegisterProtected(router chi.Router, logger zerolog.Logger, userStore *users.Store, mw *middleware.Middleware)")) {
		t.Fatal("RegisterProtected signature changed; re-check that logout cannot receive a migration manager")
	}
}
