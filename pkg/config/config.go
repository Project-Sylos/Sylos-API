package config

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"
)

const (
	defaultHTTPPort          = 8086
	defaultBcryptCost        = 12
	defaultWorkerCount       = 10
	defaultMaxRetries        = 3
	defaultCoordinatorLead   = 4
	appDataDirName           = "org.sylos.app"
	migrationDBStorageSubdir = "migration-dbs"
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
	BcryptCost int
}

type HTTPConfig struct {
	Port int
}

type JWTConfig struct {
	Secret         string
	AccessTokenTTL time.Duration
	Generated      bool
}

type RuntimeConfig struct {
	DataDir                string
	MigrationDBStorageDir  string
	LogAddress             string
	LogLevel               string
	EnableLoggingTerminal  bool
	DefaultWorkerCount     int
	DefaultMaxRetries      int
	DefaultCoordinatorLead int
}

type ServicesConfig struct {
	Local   []LocalServiceConfig
	Spectra []SpectraServiceConfig
	Cloud   []CloudServiceConfig
}

type LocalServiceConfig struct {
	ID       string
	Name     string
	RootPath string
}

type SpectraServiceConfig struct {
	ID         string
	Name       string
	ConfigPath string
	World      string
	RootID     string
}

type CloudServiceConfig struct {
	ID         string
	Name       string
	ProviderID string
	Scopes     []string
}

// ProvidersConfig is the static catalog of enabled cloud connectors.
type ProvidersConfig map[string]ProviderConfig

type ProviderConfig struct {
	Enabled     bool
	DisplayName string
	ServiceID   string
	Scopes      []string
}

// Load returns the baked-in product configuration and ensures data directories exist.
func Load() (Config, error) {
	cfg := DefaultConfig()
	if err := cfg.normalizeRuntime(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

// DefaultConfig returns production defaults with no file or env config loading.
func DefaultConfig() Config {
	providers := make(ProvidersConfig)
	providers.applyDefaults()

	services := ServicesConfig{}
	services.applyDefaults()

	return Config{
		Environment: "production",
		HTTP: HTTPConfig{
			Port: defaultHTTPPort,
		},
		JWT: JWTConfig{
			AccessTokenTTL: 24 * time.Hour,
		},
		Auth: AuthConfig{
			BcryptCost: defaultBcryptCost,
		},
		Runtime: RuntimeConfig{
			LogLevel:               "info",
			EnableLoggingTerminal:  false,
			DefaultWorkerCount:     defaultWorkerCount,
			DefaultMaxRetries:      defaultMaxRetries,
			DefaultCoordinatorLead: defaultCoordinatorLead,
		},
		Services:  services,
		Providers: providers,
	}
}

func (c *Config) normalizeRuntime() error {
	dataDir := c.Runtime.DataDir
	if dataDir == "" {
		var err error
		dataDir, err = defaultAppDataDir()
		if err != nil {
			return err
		}
	}

	absDir, err := filepath.Abs(dataDir)
	if err != nil {
		return fmt.Errorf("failed to determine absolute data dir: %w", err)
	}
	if err := os.MkdirAll(absDir, 0o755); err != nil {
		return fmt.Errorf("failed to create data dir %s: %w", absDir, err)
	}
	c.Runtime.DataDir = absDir

	migrationDBDir := c.Runtime.MigrationDBStorageDir
	if migrationDBDir == "" {
		migrationDBDir = filepath.Join(absDir, migrationDBStorageSubdir)
	}
	absMigrationDBDir, err := filepath.Abs(migrationDBDir)
	if err != nil {
		return fmt.Errorf("failed to determine absolute migration DB storage dir: %w", err)
	}
	if absMigrationDBDir == absDir {
		absMigrationDBDir = filepath.Join(absDir, migrationDBStorageSubdir)
	}
	if err := os.MkdirAll(absMigrationDBDir, 0o755); err != nil {
		return fmt.Errorf("failed to create migration DB storage dir %s: %w", absMigrationDBDir, err)
	}
	c.Runtime.MigrationDBStorageDir = absMigrationDBDir

	if c.Auth.BcryptCost <= 0 {
		c.Auth.BcryptCost = defaultBcryptCost
	}
	if c.JWT.AccessTokenTTL <= 0 {
		c.JWT.AccessTokenTTL = 24 * time.Hour
	}
	if c.HTTP.Port <= 0 {
		c.HTTP.Port = defaultHTTPPort
	}
	return nil
}

func defaultAppDataDir() (string, error) {
	switch runtime.GOOS {
	case "windows":
		base := os.Getenv("AppData")
		if base == "" {
			home, err := os.UserHomeDir()
			if err != nil {
				return "", fmt.Errorf("resolve home dir: %w", err)
			}
			base = filepath.Join(home, "AppData", "Roaming")
		}
		return filepath.Join(base, appDataDirName), nil
	case "darwin":
		home, err := os.UserHomeDir()
		if err != nil {
			return "", fmt.Errorf("resolve home dir: %w", err)
		}
		return filepath.Join(home, "Library", "Application Support", appDataDirName), nil
	default:
		if xdg := strings.TrimSpace(os.Getenv("XDG_DATA_HOME")); xdg != "" {
			return filepath.Join(xdg, appDataDirName), nil
		}
		home, err := os.UserHomeDir()
		if err != nil {
			return "", fmt.Errorf("resolve home dir: %w", err)
		}
		return filepath.Join(home, ".local", "share", appDataDirName), nil
	}
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
}
