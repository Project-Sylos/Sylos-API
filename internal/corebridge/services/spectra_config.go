package services

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

// SpectraConfig represents the structure of a Spectra config file
type SpectraConfig struct {
	Seed struct {
		MaxDepth   int    `json:"max_depth"`
		MinFolders int    `json:"min_folders"`
		MaxFolders int    `json:"max_folders"`
		MinFiles   int    `json:"min_files"`
		MaxFiles   int    `json:"max_files"`
		Seed       int    `json:"seed"`
		DBPath     string `json:"db_path"`
	} `json:"seed"`
	API struct {
		Host string `json:"host"`
		Port int    `json:"port"`
	} `json:"api"`
	SecondaryTables map[string]float64 `json:"secondary_tables"`
}

// SaveSpectraConfigOverride creates a Spectra config override file with a custom db_path
func SaveSpectraConfigOverride(dataDir, migrationID, originalConfigPath string) (string, error) {
	// Read original config
	data, err := os.ReadFile(originalConfigPath)
	if err != nil {
		return "", fmt.Errorf("failed to read original Spectra config: %w", err)
	}

	var config SpectraConfig
	if err := json.Unmarshal(data, &config); err != nil {
		return "", fmt.Errorf("failed to parse original Spectra config: %w", err)
	}

	// Override db_path
	spectraDBPath := ResolveSpectraDBPath(dataDir, migrationID)
	absSpectraDBPath, err := filepath.Abs(spectraDBPath)
	if err != nil {
		return "", fmt.Errorf("failed to resolve absolute path for Spectra DB: %w", err)
	}
	config.Seed.DBPath = absSpectraDBPath

	// Create override config file
	overrideConfigPath := filepath.Join(dataDir, fmt.Sprintf("%s-spectra-config.json", migrationID))

	// Ensure directory exists
	if err := os.MkdirAll(filepath.Dir(overrideConfigPath), 0o755); err != nil {
		return "", fmt.Errorf("failed to create directory for override config: %w", err)
	}

	// Write override config
	overrideData, err := json.MarshalIndent(config, "", "  ")
	if err != nil {
		return "", fmt.Errorf("failed to marshal override config: %w", err)
	}

	if err := os.WriteFile(overrideConfigPath, overrideData, 0o644); err != nil {
		return "", fmt.Errorf("failed to write override config: %w", err)
	}

	return overrideConfigPath, nil
}

// ResolveSpectraDBPath returns the path to the Spectra DB for a migration
func ResolveSpectraDBPath(dataDir, migrationID string) string {
	filename := fmt.Sprintf("%s-spectra.db", migrationID)
	return filepath.Join(dataDir, filename)
}

// LoadSpectraConfigOverride checks if an override config exists and returns its path
func LoadSpectraConfigOverride(dataDir, migrationID string) (string, bool, error) {
	overrideConfigPath := filepath.Join(dataDir, fmt.Sprintf("%s-spectra-config.json", migrationID))

	_, err := os.Stat(overrideConfigPath)
	if os.IsNotExist(err) {
		return "", false, nil
	}
	if err != nil {
		return "", false, fmt.Errorf("failed to check override config: %w", err)
	}

	return overrideConfigPath, true, nil
}
