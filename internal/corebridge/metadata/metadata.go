package metadata

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"gopkg.in/yaml.v3"
)

// MigrationMetadata represents minimal metadata for a migration
// The Migration Engine's YAML config file contains all the detailed state
type MigrationMetadata struct {
	ID             string    `yaml:"id"`
	Name           string    `yaml:"name"`
	ConfigPath     string    `yaml:"configPath"` // Path to the Migration Engine's YAML config file
	CreatedAt      time.Time `yaml:"createdAt"`
	IsNewMigration bool      `yaml:"isNewMigration"` // Flag to indicate this is a new migration (not a resume)
}

// MigrationsMetadata is the root structure for all migrations
type MigrationsMetadata struct {
	Migrations map[string]MigrationMetadata `yaml:"migrations"`
}

// Manager handles metadata operations with thread safety
type Manager struct {
	dataDir string
	mu      sync.Mutex
}

// NewManager creates a new metadata manager
func NewManager(dataDir string) *Manager {
	return &Manager{
		dataDir: dataDir,
	}
}

// metadataFilePath returns the path to the migrations.yaml file
func (m *Manager) metadataFilePath() string {
	return filepath.Join(m.dataDir, "migrations.yaml")
}

// LoadAllMetadata loads all migration metadata from the YAML file
func (m *Manager) LoadAllMetadata() (MigrationsMetadata, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	filePath := m.metadataFilePath()

	// If file doesn't exist, return empty structure
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		return MigrationsMetadata{
			Migrations: make(map[string]MigrationMetadata),
		}, nil
	}

	data, err := os.ReadFile(filePath)
	if err != nil {
		return MigrationsMetadata{}, fmt.Errorf("failed to read metadata file: %w", err)
	}

	var meta MigrationsMetadata
	if err := yaml.Unmarshal(data, &meta); err != nil {
		return MigrationsMetadata{}, fmt.Errorf("failed to parse metadata file: %w", err)
	}

	if meta.Migrations == nil {
		meta.Migrations = make(map[string]MigrationMetadata)
	}

	return meta, nil
}

// SaveAllMetadata saves all migration metadata to the YAML file
func (m *Manager) SaveAllMetadata(meta MigrationsMetadata) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	filePath := m.metadataFilePath()

	// Ensure directory exists
	if err := os.MkdirAll(filepath.Dir(filePath), 0o755); err != nil {
		return fmt.Errorf("failed to create metadata directory: %w", err)
	}

	// Write to temporary file first, then rename (atomic write)
	tempPath := filePath + ".tmp"
	data, err := yaml.Marshal(&meta)
	if err != nil {
		return fmt.Errorf("failed to marshal metadata: %w", err)
	}

	if err := os.WriteFile(tempPath, data, 0o644); err != nil {
		return fmt.Errorf("failed to write metadata file: %w", err)
	}

	if err := os.Rename(tempPath, filePath); err != nil {
		_ = os.Remove(tempPath)
		return fmt.Errorf("failed to rename metadata file: %w", err)
	}

	return nil
}

// GetMigrationMetadata retrieves metadata for a specific migration
func (m *Manager) GetMigrationMetadata(migrationID string) (MigrationMetadata, error) {
	all, err := m.LoadAllMetadata()
	if err != nil {
		return MigrationMetadata{}, err
	}

	meta, exists := all.Migrations[migrationID]
	if !exists {
		return MigrationMetadata{}, fmt.Errorf("migration metadata not found: %s", migrationID)
	}

	return meta, nil
}

// UpdateMigrationMetadata updates or creates metadata for a migration
func (m *Manager) UpdateMigrationMetadata(meta MigrationMetadata) error {
	// If name is empty, use ID
	if meta.Name == "" {
		meta.Name = meta.ID
	}

	all, err := m.LoadAllMetadata()
	if err != nil {
		return err
	}

	if all.Migrations == nil {
		all.Migrations = make(map[string]MigrationMetadata)
	}

	// If this is a new migration, set CreatedAt
	if existing, exists := all.Migrations[meta.ID]; !exists {
		if meta.CreatedAt.IsZero() {
			meta.CreatedAt = time.Now().UTC()
		}
	} else {
		// Preserve CreatedAt if it exists
		if !existing.CreatedAt.IsZero() {
			meta.CreatedAt = existing.CreatedAt
		}
	}

	all.Migrations[meta.ID] = meta

	return m.SaveAllMetadata(all)
}

// DeleteMigrationMetadata removes metadata for a migration
func (m *Manager) DeleteMigrationMetadata(migrationID string) error {
	all, err := m.LoadAllMetadata()
	if err != nil {
		return err
	}

	if all.Migrations == nil {
		return nil
	}

	delete(all.Migrations, migrationID)

	return m.SaveAllMetadata(all)
}

// ListAllMigrations returns all migration metadata
func (m *Manager) ListAllMigrations() ([]MigrationMetadata, error) {
	all, err := m.LoadAllMetadata()
	if err != nil {
		return nil, err
	}

	result := make([]MigrationMetadata, 0, len(all.Migrations))
	for _, meta := range all.Migrations {
		result = append(result, meta)
	}

	return result, nil
}
