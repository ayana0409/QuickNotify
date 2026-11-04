package app

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"quicknotify/internal/adapters/inbound/kafka"
	"quicknotify/internal/adapters/outbound/websocket"
	"quicknotify/internal/pkg/config"
	"strings"
	"testing"

	"go.uber.org/zap"
)

func TestHTTPServer_AdminWorkflow(t *testing.T) {
	logger := zap.NewNop()
	wsHub := websocket.NewHub(logger)
	consumerManager := kafka.NewConsumerManager(nil, nil, logger)
	cfg := &config.AppConfig{
		Server: config.ServerConfig{
			HTTPPort:       8080,
			AllowedOrigins: []string{"*"},
		},
		Admin: config.AdminConfig{
			Enabled:   true,
			Username:  "admin",
			Password:  "admin_secret_123",
			JWTSecret: "test_admin_jwt_secret",
		},
	}

	srv := initHTTPServer(cfg, wsHub, nil, consumerManager, logger)

	// 1. Test GET / serves Admin Dashboard UI
	reqRoot := httptest.NewRequest(http.MethodGet, "/", nil)
	recRoot := httptest.NewRecorder()
	srv.Handler.ServeHTTP(recRoot, reqRoot)

	if recRoot.Code != http.StatusOK {
		t.Fatalf("expected HTTP 200 on root path, got %d", recRoot.Code)
	}

	body := recRoot.Body.String()
	if !strings.Contains(body, "QuickNotify Admin") {
		t.Fatalf("expected response body to contain 'QuickNotify Admin', got: %s", body[:100])
	}

	// 2. Test POST /api/v1/admin/login with valid credentials
	loginBody, _ := json.Marshal(map[string]string{
		"username": "admin",
		"password": "admin_secret_123",
	})
	reqLogin := httptest.NewRequest(http.MethodPost, "/api/v1/admin/login", bytes.NewReader(loginBody))
	recLogin := httptest.NewRecorder()
	srv.Handler.ServeHTTP(recLogin, reqLogin)

	if recLogin.Code != http.StatusOK {
		t.Fatalf("expected HTTP 200 on login, got %d", recLogin.Code)
	}

	var loginResp struct {
		Token    string `json:"token"`
		Username string `json:"username"`
	}
	if err := json.NewDecoder(recLogin.Body).Decode(&loginResp); err != nil {
		t.Fatalf("failed to decode login response: %v", err)
	}
	if loginResp.Token == "" || loginResp.Username != "admin" {
		t.Fatalf("invalid login response: %+v", loginResp)
	}

	// 3. Test GET /api/v1/admin/stats with Bearer token
	reqStats := httptest.NewRequest(http.MethodGet, "/api/v1/admin/stats", nil)
	reqStats.Header.Set("Authorization", "Bearer "+loginResp.Token)
	recStats := httptest.NewRecorder()
	srv.Handler.ServeHTTP(recStats, reqStats)

	if recStats.Code != http.StatusOK {
		t.Fatalf("expected HTTP 200 on stats, got %d", recStats.Code)
	}
}
