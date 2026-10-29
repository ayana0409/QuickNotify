package config

import (
	"path/filepath"
	"testing"
)

func TestLoadConfig(t *testing.T) {
	configPath := filepath.Join("..", "..", "..", "configs", "config.yaml")
	cfg, err := LoadConfig(configPath)
	if err != nil {
		t.Fatalf("failed to load dev config: %v", err)
	}

	if cfg.Server.HTTPPort != 8080 {
		t.Errorf("expected port 8080, got %d", cfg.Server.HTTPPort)
	}

	if cfg.Auth.JWTSecret == "" {
		t.Errorf("expected non-empty jwt secret")
	}
}

func TestLoadConsumersConfig(t *testing.T) {
	configPath := filepath.Join("..", "..", "..", "configs", "consumers.yaml")
	cfg, err := LoadConsumersConfig(configPath)
	if err != nil {
		t.Fatalf("failed to load dev consumers config: %v", err)
	}

	if len(cfg.Clusters) == 0 {
		t.Errorf("expected at least 1 cluster")
	}

	if len(cfg.Consumers) == 0 {
		t.Errorf("expected at least 1 consumer entry")
	}
}

