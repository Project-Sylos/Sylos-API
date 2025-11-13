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
	Runtime     RuntimeConfig
	Services    ServicesConfig
}

type HTTPConfig struct {
	Port int
}

type JWTConfig struct {
	Secret         string
	AccessTokenTTL time.Duration
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

	if cfg.JWT.Secret == "" {
		return Config{}, fmt.Errorf("jwt.secret is required (set SYLOS_JWT_SECRET)")
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

	return nil
}

type RuntimeConfig struct {
	DataDir                string `mapstructure:"data_dir"`
	LogAddress             string `mapstructure:"log_address"`
	LogLevel               string `mapstructure:"log_level"`
	SkipLogListener        bool   `mapstructure:"skip_log_listener"`
	DefaultWorkerCount     int    `mapstructure:"default_worker_count"`
	DefaultMaxRetries      int    `mapstructure:"default_max_retries"`
	DefaultCoordinatorLead int    `mapstructure:"default_coordinator_lead"`
}

type ServicesConfig struct {
	Local   []LocalServiceConfig   `mapstructure:"local"`
	Spectra []SpectraServiceConfig `mapstructure:"spectra"`
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
