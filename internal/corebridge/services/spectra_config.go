package services

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// SpectraConfig represents the structure of a Spectra config file
// Supports both old format (min_folders/min_files) and new format (weighted distribution)
type SpectraConfig struct {
	Mode string `json:"mode,omitempty"` // "ephemeral" for ephemeral mode, empty or anything else for persistent mode
	Seed struct {
		MaxDepth int    `json:"max_depth"`
		DBPath   string `json:"db_path"`
		Seed     int    `json:"seed,omitempty"`

		// Old format (deprecated, kept for backwards compatibility)
		MinFolders *int `json:"min_folders,omitempty"`
		MinFiles   *int `json:"min_files,omitempty"`

		// New format - weighted distribution (required)
		MaxFolders             int     `json:"max_folders"`
		FolderBackoffFactor    float64 `json:"folder_backoff_factor,omitempty"`
		FolderDepthDecayFactor float64 `json:"folder_depth_decay_factor,omitempty"`
		MaxFiles               int     `json:"max_files"`
		FileBackoffFactor      float64 `json:"file_backoff_factor,omitempty"`
		FileDepthDecayFactor   float64 `json:"file_depth_decay_factor,omitempty"`

		// Cache configuration (optional, defaults to false)
		EnableCache bool `json:"enable_cache,omitempty"`
	} `json:"seed"`
	API struct {
		Host string `json:"host"`
		Port int    `json:"port"`
	} `json:"api"`
	SecondaryTables map[string]float64 `json:"secondary_tables"`
}

// SaveSpectraConfigOverride creates a Spectra config override file with a custom db_path
// Preserves the original JSON structure to avoid introducing zero values for missing fields
func SaveSpectraConfigOverride(dataDir, migrationID, originalConfigPath string) (string, error) {
	// Read original config
	data, err := os.ReadFile(originalConfigPath)
	if err != nil {
		return "", fmt.Errorf("failed to read original Spectra config: %w", err)
	}

	// Parse into a map to preserve all fields and structure
	var configMap map[string]any
	if err := json.Unmarshal(data, &configMap); err != nil {
		return "", fmt.Errorf("failed to parse original Spectra config: %w", err)
	}

	// Override db_path - put Spectra DB in migration-specific folder
	spectraDBPath := ResolveSpectraDBPath(dataDir, migrationID)
	absSpectraDBPath, err := filepath.Abs(spectraDBPath)
	if err != nil {
		return "", fmt.Errorf("failed to resolve absolute path for Spectra DB: %w", err)
	}

	// Navigate to seed.db_path and update it
	if seed, ok := configMap["seed"].(map[string]any); ok {
		seed["db_path"] = absSpectraDBPath
	} else {
		return "", fmt.Errorf("invalid config structure: 'seed' field is missing or not an object")
	}

	// Create override config file in migration-specific folder (same as migration DB/YAML)
	migrationDir := filepath.Join(dataDir, migrationID)
	overrideConfigPath := filepath.Join(migrationDir, "spectra-config.json")

	// Ensure directory exists
	if err := os.MkdirAll(filepath.Dir(overrideConfigPath), 0o755); err != nil {
		return "", fmt.Errorf("failed to create directory for override config: %w", err)
	}

	// Write override config, preserving original formatting as much as possible
	overrideData, err := json.MarshalIndent(configMap, "", "  ")
	if err != nil {
		return "", fmt.Errorf("failed to marshal override config: %w", err)
	}

	if err := os.WriteFile(overrideConfigPath, overrideData, 0o644); err != nil {
		return "", fmt.Errorf("failed to write override config: %w", err)
	}

	return overrideConfigPath, nil
}

// ResolveSpectraDBPath returns the path to the Spectra DB for a migration
// Puts the DB in the migration-specific folder (same as migration DB/YAML)
func ResolveSpectraDBPath(dataDir, migrationID string) string {
	migrationDir := filepath.Join(dataDir, migrationID)
	return filepath.Join(migrationDir, "spectra.db")
}

// LoadSpectraConfigOverride checks if an override config exists and returns its path
// Looks in the migration-specific folder (same as migration DB/YAML)
func LoadSpectraConfigOverride(dataDir, migrationID string) (string, bool, error) {
	migrationDir := filepath.Join(dataDir, migrationID)
	overrideConfigPath := filepath.Join(migrationDir, "spectra-config.json")

	_, err := os.Stat(overrideConfigPath)
	if os.IsNotExist(err) {
		return "", false, nil
	}
	if err != nil {
		return "", false, fmt.Errorf("failed to check override config: %w", err)
	}

	return overrideConfigPath, true, nil
}

// SaveSpectraConfigFromData saves a Spectra config from JSON data (map[string]any)
// to the override config path with an absolute db_path
// Preserves the original JSON structure to avoid losing fields not defined in SpectraConfig struct
func SaveSpectraConfigFromData(dataDir, migrationID string, configData map[string]any) (string, error) {
	// Work directly with the map to preserve all fields
	// Deep copy the map to avoid modifying the original
	configMap := make(map[string]any)
	for k, v := range configData {
		configMap[k] = v
	}

	// Override db_path - put Spectra DB in migration-specific folder
	spectraDBPath := ResolveSpectraDBPath(dataDir, migrationID)
	absSpectraDBPath, err := filepath.Abs(spectraDBPath)
	if err != nil {
		return "", fmt.Errorf("failed to resolve absolute path for Spectra DB: %w", err)
	}

	// Navigate to seed.db_path and update it
	if seed, ok := configMap["seed"].(map[string]any); ok {
		seed["db_path"] = absSpectraDBPath
	} else {
		return "", fmt.Errorf("invalid config structure: 'seed' field is missing or not an object")
	}

	// Create override config file in migration-specific folder
	migrationDir := filepath.Join(dataDir, migrationID)
	overrideConfigPath := filepath.Join(migrationDir, "spectra-config.json")

	// Ensure directory exists
	if err := os.MkdirAll(filepath.Dir(overrideConfigPath), 0o755); err != nil {
		return "", fmt.Errorf("failed to create directory for override config: %w", err)
	}

	// Write override config, preserving all original fields
	overrideData, err := json.MarshalIndent(configMap, "", "  ")
	if err != nil {
		return "", fmt.Errorf("failed to marshal override config: %w", err)
	}

	if err := os.WriteFile(overrideConfigPath, overrideData, 0o644); err != nil {
		return "", fmt.Errorf("failed to write override config: %w", err)
	}

	return overrideConfigPath, nil
}

// IsEphemeralMode checks if a Spectra config file is configured for ephemeral mode
// Returns true if mode is "ephemeral", false otherwise (including when mode is missing/empty)
// On file read or parse errors, returns false and logs a warning (defaults to persistent mode for safety)
func IsEphemeralMode(configPath string) (bool, error) {
	if configPath == "" {
		return false, fmt.Errorf("config path is empty")
	}

	// Read the config file
	data, err := os.ReadFile(configPath)
	if err != nil {
		return false, fmt.Errorf("failed to read Spectra config file: %w", err)
	}

	// Parse the config
	var config SpectraConfig
	if err := json.Unmarshal(data, &config); err != nil {
		return false, fmt.Errorf("failed to parse Spectra config file: %w", err)
	}

	// Check if mode is "ephemeral" (case-insensitive)
	return strings.ToLower(strings.TrimSpace(config.Mode)) == "ephemeral", nil
}
