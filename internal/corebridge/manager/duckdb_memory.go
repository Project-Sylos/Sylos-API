package manager

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	enginedb "codeberg.org/Sylos/Migration-Engine/pkg/db"
	"codeberg.org/Sylos/Sylos-API/internal/corebridge/apidb"
)

const installConfigDuckDBMemoryLimitGB = "duckdb_memory_limit_gb"

// DuckDBMemorySettings is the install-level DuckDB PRAGMA memory_limit control.
type DuckDBMemorySettings struct {
	// MemoryLimitGB is the saved preference. 0 means auto (host free RAM).
	MemoryLimitGB int `json:"memoryLimitGB"`
	// Auto is true when MemoryLimitGB is 0.
	Auto bool `json:"auto"`
	// EffectiveGB is the limit that would be applied now (after resolve/clamp).
	EffectiveGB int `json:"effectiveGB"`
	// AutoSuggestedGB is DefaultMemoryLimitGB() for UI copy.
	AutoSuggestedGB int `json:"autoSuggestedGB"`
	// MinGB / MaxGB are allowed explicit bounds.
	MinGB int `json:"minGB"`
	MaxGB int `json:"maxGB"`
}

func (m *Manager) GetDuckDBMemorySettings(_ context.Context) (DuckDBMemorySettings, error) {
	autoSuggested := enginedb.DefaultMemoryLimitGB()
	settings := DuckDBMemorySettings{
		MemoryLimitGB:   0,
		Auto:            true,
		EffectiveGB:     autoSuggested,
		AutoSuggestedGB: autoSuggested,
		MinGB:           enginedb.MinMemoryLimitGB,
		MaxGB:           enginedb.MaxMemoryLimitGB,
	}
	if m.apiDB == nil {
		return settings, nil
	}
	raw, err := m.apiDB.GetInstallConfig(installConfigDuckDBMemoryLimitGB)
	if err != nil {
		if errors.Is(err, apidb.ErrNotFound) {
			return settings, nil
		}
		return settings, err
	}
	n, err := strconv.Atoi(strings.TrimSpace(raw))
	if err != nil {
		return settings, nil
	}
	settings.MemoryLimitGB = n
	settings.Auto = n <= 0
	settings.EffectiveGB = enginedb.ResolveMemoryLimitGB(n)
	return settings, nil
}

func (m *Manager) SaveDuckDBMemorySettings(_ context.Context, settings DuckDBMemorySettings) error {
	if m.apiDB == nil {
		return nil
	}
	gb := settings.MemoryLimitGB
	if gb < 0 {
		return fmt.Errorf("duckdb memory limit cannot be negative")
	}
	if gb > 0 {
		if gb < enginedb.MinMemoryLimitGB || gb > enginedb.MaxMemoryLimitGB {
			return fmt.Errorf("duckdb memory limit must be 0 (auto) or %d-%d GB", enginedb.MinMemoryLimitGB, enginedb.MaxMemoryLimitGB)
		}
	}
	if err := m.apiDB.SetInstallConfig(installConfigDuckDBMemoryLimitGB, strconv.Itoa(gb)); err != nil {
		return err
	}
	m.applyDuckDBMemoryLimit(gb)
	return nil
}

// ApplyStoredDuckDBMemoryLimit loads install_config and applies it to the migration engine.
// Call once after Manager construction.
func (m *Manager) ApplyStoredDuckDBMemoryLimit() {
	if m == nil {
		return
	}
	settings, err := m.GetDuckDBMemorySettings(context.Background())
	if err != nil {
		m.applyDuckDBMemoryLimit(0)
		return
	}
	m.applyDuckDBMemoryLimit(settings.MemoryLimitGB)
}

func (m *Manager) applyDuckDBMemoryLimit(gb int) {
	if m == nil || m.engineMgr == nil {
		return
	}
	m.engineMgr.SetDuckDBMemoryLimitGB(gb)
}
