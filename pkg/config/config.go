package config

import (
	"fmt"
	"strings"
	"time"

	"github.com/spf13/viper"
)

type Config struct {
	Environment string
	HTTP        HTTPConfig
	JWT         JWTConfig
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

	if cfg.JWT.Secret == "" {
		return Config{}, fmt.Errorf("jwt.secret is required (set SYLOS_JWT_SECRET)")
	}

	if cfg.JWT.AccessTokenTTL <= 0 {
		cfg.JWT.AccessTokenTTL = 15 * time.Minute
	}

	return cfg, nil
}
