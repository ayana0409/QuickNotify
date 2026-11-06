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

func TestLoadConfig_MultilineJsonInEnvFile(t *testing.T) {
	f, err := os.CreateTemp("", "quicknotify-multiline-*.env")
	if err != nil {
		t.Fatalf("create temp file: %v", err)
	}
	defer os.Remove(f.Name())

	_, _ = f.WriteString(`
SERVER_HTTP_PORT=8080
PROJECT_CREDENTIALS='[
  {
    "project_id": "shorterlink",
    "name": "ShorterLink Platform",
    "api_key": "qn_sec_shorterlink_live_9a8b7c6d5e4f",
    "jwt_secret": "quicknotify_dev_secret_key_2026",
    "allowed_scopes": ["user", "project_global"],
    "enabled": true
  },
  {
    "project_id": "system",
    "name": "System Root Master Key",
    "api_key": "qn_sec_root_master_system_key_2026",
    "jwt_secret": "quicknotify_admin_secret_key_2026",
    "allowed_scopes": ["*"],
    "enabled": true
  }
]'
`)
	f.Close()

	cfg, err := LoadConfig(f.Name())
	if err != nil {
		t.Fatalf("LoadConfig failed on multiline JSON in .env: %v", err)
	}

	if len(cfg.Projects.Projects) != 2 {
		t.Fatalf("expected 2 projects, got %d", len(cfg.Projects.Projects))
	}

	cred, found := cfg.Projects.GetByAPIKey("qn_sec_shorterlink_live_9a8b7c6d5e4f")
	if !found {
		t.Fatal("expected to find shorterlink credential")
	}
	if cred.ProjectID != "shorterlink" {
		t.Errorf("expected project_id shorterlink, got %s", cred.ProjectID)
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

func TestLoadConfig_ProjectCredentials(t *testing.T) {
	os.Setenv("PROJECT_CREDENTIALS", `'[
		{"project_id":"shorterlink","name":"ShorterLink","api_key":"sec_shorterlink_123","jwt_secret":"jwt_shorterlink_456","allowed_scopes":["user","project_global"],"enabled":true},
		{"project_id":"quickbite","name":"QuickBite","api_key":"sec_quickbite_789","jwt_secret":"jwt_quickbite_012","allowed_scopes":["user"],"enabled":true},
		{"project_id":"system","name":"Root System","api_key":"sec_root_999","jwt_secret":"jwt_root_888","allowed_scopes":["*"],"enabled":true}
	]'`)
	defer os.Unsetenv("PROJECT_CREDENTIALS")

	cfg, err := LoadConfig("")
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}

	if len(cfg.Projects.Projects) != 3 {
		t.Fatalf("expected 3 projects, got %d", len(cfg.Projects.Projects))
	}

	// 1. Lookup by API Key
	cred, found := cfg.Projects.GetByAPIKey("sec_shorterlink_123")
	if !found {
		t.Fatal("expected to find credentials for sec_shorterlink_123")
	}
	if cred.ProjectID != "shorterlink" {
		t.Errorf("expected project_id shorterlink, got %s", cred.ProjectID)
	}
	if !cred.HasScope("user") {
		t.Errorf("expected scope 'user' to be allowed")
	}
	if !cred.HasScope("project_global") {
		t.Errorf("expected scope 'project_global' to be allowed")
	}
	if cred.HasScope("system_global") {
		t.Errorf("expected scope 'system_global' to be rejected")
	}
	if !cred.CanPublishFor("shorterlink") {
		t.Errorf("expected CanPublishFor('shorterlink') to be true")
	}
	if cred.CanPublishFor("quickbite") {
		t.Errorf("expected CanPublishFor('quickbite') to be false")
	}

	// 2. System Root key checks
	rootCred, found := cfg.Projects.GetByAPIKey("sec_root_999")
	if !found {
		t.Fatal("expected to find root credentials")
	}
	if !rootCred.HasScope("system_global") {
		t.Errorf("expected root key to have system_global scope")
	}
	if !rootCred.CanPublishFor("shorterlink") {
		t.Errorf("expected root key to publish for any project")
	}

	// 3. Lookup JWT Secret per project with fallback
	secret := cfg.Projects.GetJWTSecret("shorterlink", "fallback_secret")
	if secret != "jwt_shorterlink_456" {
		t.Errorf("expected jwt_shorterlink_456, got %s", secret)
	}

	unknownSecret := cfg.Projects.GetJWTSecret("unknown_project", "fallback_secret")
	if unknownSecret != "fallback_secret" {
		t.Errorf("expected fallback_secret, got %s", unknownSecret)
	}
}

