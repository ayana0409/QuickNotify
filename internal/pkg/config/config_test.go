package config

import (
	"os"
	"testing"
)

func TestLoadConfig_FromEnvFile(t *testing.T) {
	// Create a temporary .env file
	f, err := os.CreateTemp("", "quicknotify-*.env")
	if err != nil {
		t.Fatalf("create temp file: %v", err)
	}
	defer os.Remove(f.Name())

	_, _ = f.WriteString(`
SERVER_HTTP_PORT=9090
AUTH_JWT_SECRET=test_secret
ADMIN_USERNAME=testadmin
KAFKA_BROKERS=broker1:9092,broker2:9092
KAFKA_STANDARD_TOPIC=my-topic
REDIS_ENABLED=false
LOG_LEVEL=info
`)
	f.Close()

	cfg, err := LoadConfig(f.Name())
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}

	if cfg.Server.HTTPPort != 9090 {
		t.Errorf("expected port 9090, got %d", cfg.Server.HTTPPort)
	}
	if cfg.Auth.JWTSecret != "test_secret" {
		t.Errorf("expected test_secret, got %s", cfg.Auth.JWTSecret)
	}
	if cfg.Admin.Username != "testadmin" {
		t.Errorf("expected testadmin, got %s", cfg.Admin.Username)
	}
	if len(cfg.Kafka.Brokers) != 2 {
		t.Errorf("expected 2 brokers, got %d", len(cfg.Kafka.Brokers))
	}
	if cfg.Kafka.StandardTopic != "my-topic" {
		t.Errorf("expected my-topic, got %s", cfg.Kafka.StandardTopic)
	}
	if cfg.Redis.Enabled != false {
		t.Errorf("expected redis disabled")
	}
	if cfg.Log.Level != "info" {
		t.Errorf("expected log level info, got %s", cfg.Log.Level)
	}
}

func TestLoadConfig_MultiClusterFromEnvVars(t *testing.T) {
	// Set KAFKA_CLUSTERS and KAFKA_CONSUMERS via OS env with single quotes as commonly placed in .env
	os.Setenv("KAFKA_CLUSTERS", `'[{"id":"main","brokers":["k1:9092"]},{"id":"quickbite","brokers":["k2:9092"]}]'`)
	os.Setenv("KAFKA_CONSUMERS", `'[{"topic":"order-events","cluster_id":"quickbite","translator":"quickbite_order"},{"topic":"edutrack-exam-results","cluster_id":"main","translator":"edutrack_exam"}]'`)
	defer func() {
		os.Unsetenv("KAFKA_CLUSTERS")
		os.Unsetenv("KAFKA_CONSUMERS")
	}()

	cfg, err := LoadConfig("")
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}

	if cfg.Kafka.ConsumersConfig == nil {
		t.Fatal("expected ConsumersConfig to be set")
	}
	if len(cfg.Kafka.ConsumersConfig.Clusters) != 2 {
		t.Errorf("expected 2 clusters, got %d", len(cfg.Kafka.ConsumersConfig.Clusters))
	}
	if len(cfg.Kafka.ConsumersConfig.Consumers) != 2 {
		t.Errorf("expected 2 consumers, got %d", len(cfg.Kafka.ConsumersConfig.Consumers))
	}
	if cfg.Kafka.ConsumersConfig.Consumers[0].Translator != "quickbite_order" {
		t.Errorf("expected quickbite_order translator, got %s", cfg.Kafka.ConsumersConfig.Consumers[0].Translator)
	}
}

func TestLoadConfig_OsEnvOverridesFile(t *testing.T) {
	// Create a .env file with one value, then override with OS env
	f, err := os.CreateTemp("", "quicknotify-*.env")
	if err != nil {
		t.Fatalf("create temp file: %v", err)
	}
	defer os.Remove(f.Name())
	_, _ = f.WriteString("AUTH_JWT_SECRET=from_file\n")
	f.Close()

	os.Setenv("AUTH_JWT_SECRET", "from_os_env")
	defer os.Unsetenv("AUTH_JWT_SECRET")

	cfg, err := LoadConfig(f.Name())
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}

	if cfg.Auth.JWTSecret != "from_os_env" {
		t.Errorf("expected OS env to override file, got %s", cfg.Auth.JWTSecret)
	}
}
