package config

import (
	"path/filepath"
	"testing"
)

func TestNormalizeRuntimeKeepsMigrationsOutOfDataDir(t *testing.T) {
	t.Parallel()

	dataDir := t.TempDir()
	cfg := Config{Runtime: RuntimeConfig{
		DataDir:               dataDir,
		MigrationDBStorageDir: dataDir,
	}}
	if err := cfg.normalizeRuntime(); err != nil {
		t.Fatal(err)
	}

	want := filepath.Join(dataDir, migrationDBStorageSubdir)
	if cfg.Runtime.MigrationDBStorageDir != want {
		t.Fatalf("MigrationDBStorageDir=%q want %q", cfg.Runtime.MigrationDBStorageDir, want)
	}
	if cfg.Runtime.DataDir != dataDir {
		t.Fatalf("DataDir=%q want %q", cfg.Runtime.DataDir, dataDir)
	}
}

func TestLoadCreatesAppDataDirs(t *testing.T) {
	t.Parallel()

	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Runtime.DataDir == "" {
		t.Fatal("empty DataDir")
	}
	if filepath.Base(cfg.Runtime.MigrationDBStorageDir) != migrationDBStorageSubdir {
		t.Fatalf("MigrationDBStorageDir=%q", cfg.Runtime.MigrationDBStorageDir)
	}
	if cfg.Environment != "production" {
		t.Fatalf("Environment=%q", cfg.Environment)
	}
	if cfg.HTTP.Port != defaultHTTPPort {
		t.Fatalf("Port=%d", cfg.HTTP.Port)
	}
	if len(cfg.Providers) == 0 {
		t.Fatal("expected provider catalog")
	}
	if len(cfg.Services.Local) == 0 {
		t.Fatal("expected local service")
	}
	if len(cfg.Services.Spectra) != 0 {
		t.Fatalf("shipped defaults must not register Spectra, got %d", len(cfg.Services.Spectra))
	}
}
