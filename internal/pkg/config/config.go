package config

import (
	"fmt"
	"strings"

	"quicknotify/internal/adapters/outbound/email"

	"github.com/spf13/viper"
)

// ServerConfig defines HTTP server settings.
type ServerConfig struct {
	HTTPPort        int      `mapstructure:"http_port"`
	ReadTimeoutSec  int      `mapstructure:"read_timeout_sec"`
	WriteTimeoutSec int      `mapstructure:"write_timeout_sec"`
	AllowedOrigins  []string `mapstructure:"allowed_origins"`
}

// AuthConfig defines authentication parameters.
type AuthConfig struct {
	JWTSecret string `mapstructure:"jwt_secret"`
}

// KafkaConfig defines connection settings for Kafka consumers.
type KafkaConfig struct {
	Brokers             []string `mapstructure:"brokers"`
	ConsumerGroup       string   `mapstructure:"consumer_group"`
	StandardTopic       string   `mapstructure:"standard_topic"`
	ConsumersConfigPath string   `mapstructure:"consumers_config_path"`
}

// LogConfig defines logging verbosity and format.
type LogConfig struct {
	Level       string `mapstructure:"level"`
	Development bool   `mapstructure:"development"`
}

// RedisConfig defines connection settings for Redis caching.
type RedisConfig struct {
	Host     string `mapstructure:"host"`
	Port     int    `mapstructure:"port"`
	Username string `mapstructure:"username"`
	Password string `mapstructure:"password"`
	DB       int    `mapstructure:"db"`
	Enabled  bool   `mapstructure:"enabled"`
}

// AppConfig represents the root application configuration.
type AppConfig struct {
	Server ServerConfig `mapstructure:"server"`
	Auth   AuthConfig   `mapstructure:"auth"`
	Email  email.Config `mapstructure:"email"`
	Kafka  KafkaConfig  `mapstructure:"kafka"`
	Redis  RedisConfig  `mapstructure:"redis"`
	Log    LogConfig    `mapstructure:"log"`
}

// ClusterConfig represents an individual Kafka cluster.
type ClusterConfig struct {
	ID      string   `mapstructure:"id"`
	Brokers []string `mapstructure:"brokers"`
}

// ConsumerEntry maps a Kafka topic to a cluster and a specific translator.
type ConsumerEntry struct {
	Topic      string `mapstructure:"topic"`
	ClusterID  string `mapstructure:"cluster_id"`
	Translator string `mapstructure:"translator"`
}

// ConsumersConfig represents the multi-cluster consumer registry.
type ConsumersConfig struct {
	Clusters  []ClusterConfig `mapstructure:"clusters"`
	Consumers []ConsumerEntry `mapstructure:"consumers"`
}

// LoadConfig reads the application config from a given path (e.g., "configs/config.yaml").
func LoadConfig(configPath string) (*AppConfig, error) {
	v := viper.New()
	v.SetConfigFile(configPath)
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()

	// Defaults
	v.SetDefault("server.http_port", 8080)
	v.SetDefault("server.read_timeout_sec", 15)
	v.SetDefault("server.write_timeout_sec", 15)
	v.SetDefault("server.allowed_origins", []string{"*"})
	v.SetDefault("auth.jwt_secret", "quicknotify_dev_secret_key_2026")
	v.SetDefault("email.enabled", false)
	v.SetDefault("log.level", "debug")
	v.SetDefault("log.development", true)

	if err := v.ReadInConfig(); err != nil {
		return nil, fmt.Errorf("failed to read config file '%s': %w", configPath, err)
	}

	var cfg AppConfig
	if err := v.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("failed to unmarshal config: %w", err)
	}

	return &cfg, nil
}

// LoadConsumersConfig reads the consumers registry config from a given path.
func LoadConsumersConfig(configPath string) (*ConsumersConfig, error) {
	v := viper.New()
	v.SetConfigFile(configPath)
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()

	if err := v.ReadInConfig(); err != nil {
		return nil, fmt.Errorf("failed to read consumers config file '%s': %w", configPath, err)
	}

	var cfg ConsumersConfig
	if err := v.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("failed to unmarshal consumers config: %w", err)
	}

	return &cfg, nil
}

