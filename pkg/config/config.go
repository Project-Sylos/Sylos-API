package config

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/spf13/viper"
)

type Config struct {
	Environment string
	HTTP        HTTPConfig
	JWT         JWTConfig
	Runtime     RuntimeConfig
	Services    ServicesConfig
	Providers   ProvidersConfig
}

type HTTPConfig struct {
	Port int
}

type JWTConfig struct {
	Secret         string
	AccessTokenTTL time.Duration
	Generated      bool
}

func Load() (Config, error) {
	v := viper.New()

	v.SetEnvPrefix("SYLOS")
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()

	v.SetConfigName("config")
	v.SetConfigType("yaml")
	v.AddConfigPath(".")

	if customPath := v.GetString("config.path"); customPath != "" {
		v.SetConfigFile(customPath)
	}

	v.SetDefault("environment", "development")
	v.SetDefault("http.port", 8080)
	v.SetDefault("jwt.access_token_ttl", "15m")
	v.SetDefault("runtime.data_dir", "data")
	v.SetDefault("runtime.migration_db_storage_dir", "data/migration-dbs")
	v.SetDefault("runtime.log_level", "info")
	v.SetDefault("runtime.default_worker_count", 10)
	v.SetDefault("runtime.default_max_retries", 3)
	v.SetDefault("runtime.default_coordinator_lead", 4)

	_ = v.ReadInConfig() // optional: ignore not found errors

	cfg := Config{
		Environment: v.GetString("environment"),
		HTTP: HTTPConfig{
			Port: v.GetInt("http.port"),
		},
		JWT: JWTConfig{
			Secret:         v.GetString("jwt.secret"),
			AccessTokenTTL: v.GetDuration("jwt.access_token_ttl"),
		},
	}

	if err := v.UnmarshalKey("runtime", &cfg.Runtime); err != nil {
		return Config{}, fmt.Errorf("failed to parse runtime config: %w", err)
	}

	if err := cfg.normalizeRuntime(); err != nil {
		return Config{}, err
	}

	if err := v.UnmarshalKey("services", &cfg.Services); err != nil {
		return Config{}, fmt.Errorf("failed to parse services config: %w", err)
	}

	if err := v.UnmarshalKey("providers", &cfg.Providers); err != nil {
		return Config{}, fmt.Errorf("failed to parse providers config: %w", err)
	}
	if cfg.Providers == nil {
		cfg.Providers = make(ProvidersConfig)
	}
	cfg.Providers.applyDefaults()

	if cfg.JWT.Secret == "" {
		cfg.JWT.Secret = generateEphemeralSecret()
		cfg.JWT.Generated = true
	}

	if cfg.JWT.AccessTokenTTL <= 0 {
		cfg.JWT.AccessTokenTTL = 15 * time.Minute
	}

	return cfg, nil
}

func (c *Config) normalizeRuntime() error {
	dataDir := c.Runtime.DataDir
	if dataDir == "" {
		dataDir = "data"
	}

	absDir, err := filepath.Abs(dataDir)
	if err != nil {
		return fmt.Errorf("failed to determine absolute data dir: %w", err)
	}

	if err := os.MkdirAll(absDir, 0o755); err != nil {
		return fmt.Errorf("failed to create data dir %s: %w", absDir, err)
	}

	c.Runtime.DataDir = absDir

	// Normalize migration DB storage directory
	migrationDBDir := c.Runtime.MigrationDBStorageDir
	if migrationDBDir == "" {
		migrationDBDir = filepath.Join(absDir, "migration-dbs")
	}

	absMigrationDBDir, err := filepath.Abs(migrationDBDir)
	if err != nil {
		return fmt.Errorf("failed to determine absolute migration DB storage dir: %w", err)
	}

	if err := os.MkdirAll(absMigrationDBDir, 0o755); err != nil {
		return fmt.Errorf("failed to create migration DB storage dir %s: %w", absMigrationDBDir, err)
	}

	c.Runtime.MigrationDBStorageDir = absMigrationDBDir

	return nil
}

func generateEphemeralSecret() string {
	const secretBytes = 32
	b := make([]byte, secretBytes)
	if _, err := rand.Read(b); err != nil {
		return fmt.Sprintf("ephemeral-%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(b)
}

type RuntimeConfig struct {
	DataDir                string `mapstructure:"data_dir"`
	MigrationDBStorageDir  string `mapstructure:"migration_db_storage_dir"`
	LogAddress             string `mapstructure:"log_address"`
	LogLevel               string `mapstructure:"log_level"`
	EnableLoggingTerminal  bool   `mapstructure:"enable_logging_terminal"`
	DefaultWorkerCount     int    `mapstructure:"default_worker_count"`
	DefaultMaxRetries      int    `mapstructure:"default_max_retries"`
	DefaultCoordinatorLead int    `mapstructure:"default_coordinator_lead"`
}

type ServicesConfig struct {
	Local   []LocalServiceConfig   `mapstructure:"local"`
	Spectra []SpectraServiceConfig `mapstructure:"spectra"`
	Cloud   []CloudServiceConfig   `mapstructure:"cloud"`
}

type LocalServiceConfig struct {
	ID       string `mapstructure:"id"`
	Name     string `mapstructure:"name"`
	RootPath string `mapstructure:"root_path"`
}

type SpectraServiceConfig struct {
	ID         string `mapstructure:"id"`
	Name       string `mapstructure:"name"`
	ConfigPath string `mapstructure:"config_path"`
	World      string `mapstructure:"world"`
	RootID     string `mapstructure:"root_id"`
}

type CloudServiceConfig struct {
	ID         string   `mapstructure:"id"`
	Name       string   `mapstructure:"name"`
	ProviderID string   `mapstructure:"provider_id"`
	Scopes     []string `mapstructure:"scopes"`
}

// ProvidersConfig is the static catalog of enabled cloud connectors.
type ProvidersConfig map[string]ProviderConfig

type ProviderConfig struct {
	Enabled     bool     `mapstructure:"enabled"`
	DisplayName string   `mapstructure:"display_name"`
	ServiceID   string   `mapstructure:"service_id"`
	Scopes      []string `mapstructure:"scopes"`
}

func (p ProvidersConfig) applyDefaults() {
	if p == nil {
		return
	}
	if _, ok := p["google_drive"]; !ok {
		p["google_drive"] = ProviderConfig{
			Enabled:     true,
			DisplayName: "Google Drive",
			ServiceID:   "google-drive",
			Scopes: []string{
				"https://www.googleapis.com/auth/drive",
			},
		}
	}
	if _, ok := p["dropbox"]; !ok {
		p["dropbox"] = ProviderConfig{
			Enabled:     false,
			DisplayName: "Dropbox",
			ServiceID:   "dropbox",
		}
	}
	for id, cfg := range p {
		if cfg.ServiceID == "" {
			cfg.ServiceID = strings.ReplaceAll(id, "_", "-")
			p[id] = cfg
		}
		if cfg.DisplayName == "" {
			cfg.DisplayName = id
			p[id] = cfg
		}
	}
}
