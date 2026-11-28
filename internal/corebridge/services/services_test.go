package services

import (
	"context"
	"path/filepath"
	"testing"

	"codeberg.org/Sylos/Sylos-API/pkg/config"
)

func TestLoadServicesRegistersDefaultSpectraWorlds(t *testing.T) {
	t.Parallel()

	dataDir := t.TempDir()
	cfg := config.DefaultConfig()
	cfg.Runtime.DataDir = dataDir
	if len(cfg.Services.Spectra) != 0 {
		t.Fatal("expected DefaultConfig to leave Spectra unregistered")
	}

	mgr := NewServiceManager()
	if err := mgr.LoadServices(cfg); err != nil {
		t.Fatalf("LoadServices: %v", err)
	}

	if _, err := mgr.GetServiceDefinitionByWorld("primary"); err != nil {
		t.Fatalf("primary world: %v", err)
	}
	if _, err := mgr.GetServiceDefinitionByWorld("s1"); err != nil {
		t.Fatalf("s1 world: %v", err)
	}

	sources, err := mgr.ListSources(context.Background())
	if err != nil {
		t.Fatalf("ListSources: %v", err)
	}
	found := false
	for _, src := range sources {
		if src.ID == "spectra" && src.Type == ServiceTypeSpectra {
			found = true
			break
		}
	}
	if !found {
		t.Fatal("expected virtual spectra catalog entry in ListSources")
	}

	wantCfg := filepath.Join(dataDir, "spectra", "default-config.json")
	if _, err := mgr.GetServiceDefinition("spectra-primary"); err != nil {
		t.Fatalf("spectra-primary: %v", err)
	}
	def, _ := mgr.GetServiceDefinition("spectra-primary")
	if def.Spectra == nil || def.Spectra.ConfigPath != wantCfg {
		t.Fatalf("ConfigPath=%v want %s", def.Spectra, wantCfg)
	}
}
