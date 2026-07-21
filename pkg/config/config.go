package config

import (
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
	Auth        AuthConfig
	Runtime     RuntimeConfig
	Services    ServicesConfig
	Providers   ProvidersConfig
}

type AuthConfig struct {
	BcryptCost int `mapstructure:"bcrypt_cost"`
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
	v.SetDefault("http.port", 8086)
	v.SetDefault("jwt.access_token_ttl", "24h")
	v.SetDefault("auth.bcrypt_cost", 12)
	v.SetDefault("runtime.data_dir", "data")
	v.SetDefault("runtime.migration_db_storage_dir", "data/migration-dbs")
	v.SetDefault("runtime.log_level", "info")
	v.SetDefault("runtime.default_worker_count", 10)
	v.SetDefault("runtime.default_max_retries", 3)
	v.SetDefault("runtime.default_coordinator_lead", 4)

	v.SetDefault("runtime.oauth_creds_dir", "creds")

	_ = v.ReadInConfig() // optional: ignore not found errors

	configFile := v.ConfigFileUsed()

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

	if err := v.UnmarshalKey("auth", &cfg.Auth); err != nil {
		return Config{}, fmt.Errorf("failed to parse auth config: %w", err)
	}
	if cfg.Auth.BcryptCost <= 0 {
		cfg.Auth.BcryptCost = 12
	}

	if err := cfg.normalizeRuntime(configFile); err != nil {
		return Config{}, err
	}

	if err := v.UnmarshalKey("services", &cfg.Services); err != nil {
		return Config{}, fmt.Errorf("failed to parse services config: %w", err)
	}
	cfg.Services.applyDefaults()

	if err := v.UnmarshalKey("providers", &cfg.Providers); err != nil {
		return Config{}, fmt.Errorf("failed to parse providers config: %w", err)
	}
	if cfg.Providers == nil {
		cfg.Providers = make(ProvidersConfig)
	}
	cfg.Providers.applyDefaults()

	if cfg.JWT.AccessTokenTTL <= 0 {
		cfg.JWT.AccessTokenTTL = 24 * time.Hour
	}

	return cfg, nil
}

func (c *Config) normalizeRuntime(configFile string) error {
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

	oauthCredsDir := c.Runtime.OAuthCredsDir
	if oauthCredsDir == "" {
		if configFile != "" {
			oauthCredsDir = filepath.Join(filepath.Dir(configFile), "creds")
		} else {
			oauthCredsDir = "creds"
		}
	}

	absOAuthCredsDir, err := filepath.Abs(oauthCredsDir)
	if err != nil {
		return fmt.Errorf("failed to determine absolute oauth creds dir: %w", err)
	}

	c.Runtime.OAuthCredsDir = absOAuthCredsDir

	return nil
}

type RuntimeConfig struct {
	DataDir                string `mapstructure:"data_dir"`
	MigrationDBStorageDir  string `mapstructure:"migration_db_storage_dir"`
	OAuthCredsDir          string `mapstructure:"oauth_creds_dir"`
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
			Enabled:     true,
			DisplayName: "Dropbox",
			ServiceID:   "dropbox",
			Scopes: []string{
				"files.metadata.read",
				"files.content.read",
				"files.content.write",
				"account_info.read",
				"sharing.read",
				"team_data.team_space",
			},
		}
	}
	if _, ok := p["sftp"]; !ok {
		p["sftp"] = ProviderConfig{
			Enabled:     true,
			DisplayName: "SFTP",
			ServiceID:   "sftp",
		}
	}
	if _, ok := p["onedrive"]; !ok {
		p["onedrive"] = ProviderConfig{
			Enabled:     true,
			DisplayName: "OneDrive",
			ServiceID:   "onedrive",
			Scopes: []string{
				"Files.ReadWrite.All",
				"offline_access",
				"User.Read",
			},
		}
	}
	if _, ok := p["sharepoint"]; !ok {
		p["sharepoint"] = ProviderConfig{
			Enabled:     true,
			DisplayName: "SharePoint",
			ServiceID:   "sharepoint",
			Scopes: []string{
				"Sites.ReadWrite.All",
				"Files.ReadWrite.All",
				"offline_access",
				"User.Read",
			},
		}
	}
	if _, ok := p["box"]; !ok {
		p["box"] = ProviderConfig{
			Enabled:     true,
			DisplayName: "Box",
			ServiceID:   "box",
			Scopes: []string{
				"root_readwrite",
			},
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

func (s *ServicesConfig) applyDefaults() {
	if len(s.Local) == 0 {
		s.Local = []LocalServiceConfig{{
			ID:   "local",
			Name: "Local Filesystem",
		}}
	}
	// Spectra is a developer test connector. Auto-register when unset and the
	// default Migration-Engine config is present next to the process cwd.
	if len(s.Spectra) == 0 {
		const defaultSpectraConfig = "../Migration-Engine/pkg/configs/spectra.json"
		if abs, err := filepath.Abs(defaultSpectraConfig); err == nil {
			if _, err := os.Stat(abs); err == nil {
				s.Spectra = []SpectraServiceConfig{
					{
						ID:         "spectra-primary",
						Name:       "Spectra Primary",
						ConfigPath: defaultSpectraConfig,
						World:      "primary",
						RootID:     "root",
					},
					{
						ID:         "spectra-s1",
						Name:       "Spectra S1",
						ConfigPath: defaultSpectraConfig,
						World:      "s1",
						RootID:     "root",
					},
				}
			}
		}
	}
}
