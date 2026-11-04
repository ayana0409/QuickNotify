package config

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"quicknotify/internal/adapters/outbound/email"
)

// ServerConfig defines HTTP server settings.
type ServerConfig struct {
	HTTPPort        int      `mapstructure:"http_port"`
	ReadTimeoutSec  int      `mapstructure:"read_timeout_sec"`
	WriteTimeoutSec int      `mapstructure:"write_timeout_sec"`
	AllowedOrigins  []string `mapstructure:"allowed_origins"`
	BasePath        string   `mapstructure:"base_path"`
}

// AuthConfig defines authentication parameters.
type AuthConfig struct {
	JWTSecret string `mapstructure:"jwt_secret"`
}

// AdminConfig defines credentials and settings for the Admin Dashboard.
type AdminConfig struct {
	Enabled   bool   `mapstructure:"enabled"`
	Username  string `mapstructure:"username"`
	Password  string `mapstructure:"password"`
	JWTSecret string `mapstructure:"jwt_secret"`
}

// KafkaConfig defines core Kafka connection settings for QuickNotify's own cluster.
type KafkaConfig struct {
	// Core cluster: QuickNotify's own Kafka cluster and standard topic
	Brokers       []string `mapstructure:"brokers"`
	ConsumerGroup string   `mapstructure:"consumer_group"`
	StandardTopic string   `mapstructure:"standard_topic"`

	// Multi-cluster consumer mapping for integrating external/legacy project Kafka clusters.
	// Loaded from KAFKA_CLUSTERS (JSON) and KAFKA_CONSUMERS (JSON) in the .env file.
	ConsumersConfig *ConsumersConfig
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
	Admin  AdminConfig  `mapstructure:"admin"`
	Email  email.Config `mapstructure:"email"`
	Kafka  KafkaConfig  `mapstructure:"kafka"`
	Redis  RedisConfig  `mapstructure:"redis"`
	Log    LogConfig    `mapstructure:"log"`
}

// ClusterConfig represents an individual Kafka cluster.
type ClusterConfig struct {
	ID      string   `json:"id"      mapstructure:"id"`
	Brokers []string `json:"brokers" mapstructure:"brokers"`
}

// ConsumerEntry maps a Kafka topic to a cluster and a specific translator.
type ConsumerEntry struct {
	Topic      string `json:"topic"       mapstructure:"topic"`
	ClusterID  string `json:"cluster_id"  mapstructure:"cluster_id"`
	Translator string `json:"translator"  mapstructure:"translator"`
}

// ConsumersConfig represents the multi-cluster consumer registry (Kafka 1 -> topics 1.x, Kafka 2 -> topics 2.x).
type ConsumersConfig struct {
	Clusters  []ClusterConfig `json:"clusters"  mapstructure:"clusters"`
	Consumers []ConsumerEntry `json:"consumers" mapstructure:"consumers"`
}

// LoadConfig loads full application configuration from a .env file + OS environment variables.
// Priority: OS env vars > .env file values > built-in defaults.
// envPath: path to the .env file (e.g. ".env", "/run/secrets/.env"). Empty = auto-discover ".env".
func LoadConfig(envPath string) (*AppConfig, error) {
	env := loadEnvFile(envPath)

	cfg := &AppConfig{}

	// --- Server ---
	cfg.Server.HTTPPort = getInt(env, "SERVER_HTTP_PORT", 8080)
	cfg.Server.ReadTimeoutSec = getInt(env, "SERVER_READ_TIMEOUT_SEC", 15)
	cfg.Server.WriteTimeoutSec = getInt(env, "SERVER_WRITE_TIMEOUT_SEC", 15)
	cfg.Server.AllowedOrigins = getStringSlice(env, "SERVER_ALLOWED_ORIGINS", []string{"*"})
	cfg.Server.BasePath = strings.TrimSuffix(getString(env, "SERVER_BASE_PATH", ""), "/")

	// --- Auth ---
	cfg.Auth.JWTSecret = getString(env, "AUTH_JWT_SECRET", "quicknotify_dev_secret_key_2026")

	// --- Admin Dashboard ---
	cfg.Admin.Enabled = getBool(env, "ADMIN_ENABLED", true)
	cfg.Admin.Username = getString(env, "ADMIN_USERNAME", "admin")
	cfg.Admin.Password = getString(env, "ADMIN_PASSWORD", "admin123")
	cfg.Admin.JWTSecret = getString(env, "ADMIN_JWT_SECRET", "quicknotify_admin_secret_key_2026")

	// --- Kafka Core (QuickNotify's own cluster) ---
	cfg.Kafka.Brokers = getStringSlice(env, "KAFKA_BROKERS", []string{"localhost:9092"})
	cfg.Kafka.ConsumerGroup = getString(env, "KAFKA_CONSUMER_GROUP", "quicknotify-core-group")
	cfg.Kafka.StandardTopic = getString(env, "KAFKA_STANDARD_TOPIC", "global-notify-events")

	// --- Kafka Multi-cluster Consumer Mapping (External/Legacy project clusters) ---
	// KAFKA_CLUSTERS: JSON array of {id, brokers[]}
	// KAFKA_CONSUMERS: JSON array of {topic, cluster_id, translator}
	clustersJSON := getString(env, "KAFKA_CLUSTERS", "")
	consumersJSON := getString(env, "KAFKA_CONSUMERS", "")
	if clustersJSON != "" || consumersJSON != "" {
		consumersCfg, err := parseConsumersConfig(clustersJSON, consumersJSON)
		if err != nil {
			return nil, fmt.Errorf("parse KAFKA_CLUSTERS/KAFKA_CONSUMERS: %w", err)
		}
		cfg.Kafka.ConsumersConfig = consumersCfg
	}

	// --- Redis ---
	cfg.Redis.Enabled = getBool(env, "REDIS_ENABLED", false)
	cfg.Redis.Host = getString(env, "REDIS_HOST", "localhost")
	cfg.Redis.Port = getInt(env, "REDIS_PORT", 6379)
	cfg.Redis.Username = getString(env, "REDIS_USERNAME", "")
	cfg.Redis.Password = getString(env, "REDIS_PASSWORD", "")
	cfg.Redis.DB = getInt(env, "REDIS_DB", 0)

	// --- Email ---
	cfg.Email.Enabled = getBool(env, "EMAIL_ENABLED", false)
	cfg.Email.Host = getString(env, "EMAIL_HOST", "localhost")
	cfg.Email.Port = getInt(env, "EMAIL_PORT", 587)
	cfg.Email.Username = getString(env, "EMAIL_USERNAME", "")
	cfg.Email.Password = getString(env, "EMAIL_PASSWORD", "")
	cfg.Email.From = getString(env, "EMAIL_FROM", "no-reply@quicknotify.local")

	// --- Log ---
	cfg.Log.Level = getString(env, "LOG_LEVEL", "debug")
	cfg.Log.Development = getBool(env, "LOG_DEVELOPMENT", true)

	return cfg, nil
}

// LoadConsumersConfig is kept for backward compatibility with tests.
// When migrated to env-based config, the caller should use AppConfig.Kafka.ConsumersConfig directly.
func LoadConsumersConfig(_ string) (*ConsumersConfig, error) {
	return &ConsumersConfig{}, nil
}

// parseConsumersConfig unmarshals KAFKA_CLUSTERS and KAFKA_CONSUMERS JSON strings.
func parseConsumersConfig(clustersJSON, consumersJSON string) (*ConsumersConfig, error) {
	cfg := &ConsumersConfig{}

	if clustersJSON != "" {
		s := strings.TrimSpace(clustersJSON)
		if len(s) >= 2 && ((s[0] == '\'' && s[len(s)-1] == '\'') || (s[0] == '"' && s[len(s)-1] == '"')) {
			s = s[1 : len(s)-1]
		}
		if err := json.Unmarshal([]byte(s), &cfg.Clusters); err != nil {
			return nil, fmt.Errorf("invalid KAFKA_CLUSTERS JSON: %w", err)
		}
	}
	if consumersJSON != "" {
		s := strings.TrimSpace(consumersJSON)
		if len(s) >= 2 && ((s[0] == '\'' && s[len(s)-1] == '\'') || (s[0] == '"' && s[len(s)-1] == '"')) {
			s = s[1 : len(s)-1]
		}
		if err := json.Unmarshal([]byte(s), &cfg.Consumers); err != nil {
			return nil, fmt.Errorf("invalid KAFKA_CONSUMERS JSON: %w", err)
		}
	}
	return cfg, nil
}

// loadEnvFile reads key=value pairs from a .env file into a map.
// OS environment variables always take priority over file values.
func loadEnvFile(envPath string) map[string]string {
	env := make(map[string]string)

	candidates := []string{envPath, ".env"}
	for _, base := range []string{envPath, ".env"} {
		candidates = append(candidates,
			filepath.Join("..", base),
			filepath.Join("..", "..", base),
			filepath.Join("..", "..", "..", base),
		)
	}

	for _, path := range candidates {
		if path == "" {
			continue
		}
		f, err := os.Open(path)
		if err != nil {
			continue
		}
		scanner := bufio.NewScanner(f)
		for scanner.Scan() {
			line := strings.TrimSpace(scanner.Text())
			if line == "" || strings.HasPrefix(line, "#") {
				continue
			}
			parts := strings.SplitN(line, "=", 2)
			if len(parts) != 2 {
				continue
			}
			k := strings.TrimSpace(parts[0])
			v := strings.TrimSpace(parts[1])
			// Strip surrounding quotes
			if len(v) >= 2 && ((v[0] == '"' && v[len(v)-1] == '"') || (v[0] == '\'' && v[len(v)-1] == '\'')) {
				v = v[1 : len(v)-1]
			}
			env[k] = v
		}
		f.Close()
		break // Use the first file found
	}
	return env
}

// getString returns the value for key: OS env > .env file > defaultVal.
func getString(env map[string]string, key, defaultVal string) string {
	if v, ok := os.LookupEnv(key); ok {
		return v
	}
	if v, ok := env[key]; ok {
		return v
	}
	return defaultVal
}

// getInt returns integer value for key: OS env > .env file > defaultVal.
func getInt(env map[string]string, key string, defaultVal int) int {
	if s := getString(env, key, ""); s != "" {
		if n, err := strconv.Atoi(s); err == nil {
			return n
		}
	}
	return defaultVal
}

// getBool returns boolean value for key: OS env > .env file > defaultVal.
func getBool(env map[string]string, key string, defaultVal bool) bool {
	if s := getString(env, key, ""); s != "" {
		if b, err := strconv.ParseBool(s); err == nil {
			return b
		}
	}
	return defaultVal
}

// getStringSlice splits a comma-separated env value into a []string.
func getStringSlice(env map[string]string, key string, defaultVal []string) []string {
	s := getString(env, key, "")
	if s == "" {
		return defaultVal
	}
	parts := strings.Split(s, ",")
	result := make([]string, 0, len(parts))
	for _, p := range parts {
		if t := strings.TrimSpace(p); t != "" {
			result = append(result, t)
		}
	}
	return result
}
