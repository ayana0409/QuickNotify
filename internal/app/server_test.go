package app

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"quicknotify/internal/adapters/inbound/kafka"
	"quicknotify/internal/adapters/outbound/storage"
	"quicknotify/internal/adapters/outbound/websocket"
	"quicknotify/internal/core/domain"
	"quicknotify/internal/core/services"
	"quicknotify/internal/pkg/config"

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

	srv := initHTTPServer(cfg, wsHub, nil, consumerManager, nil, nil, logger)

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

	// 4. Test Subpath prefix support (e.g. /quick-notify)
	reqSubpathRoot := httptest.NewRequest(http.MethodGet, "/quick-notify", nil)
	recSubpathRoot := httptest.NewRecorder()
	srv.Handler.ServeHTTP(recSubpathRoot, reqSubpathRoot)
	if recSubpathRoot.Code != http.StatusOK {
		t.Fatalf("expected HTTP 200 on subpath /quick-notify, got %d", recSubpathRoot.Code)
	}

	reqSubpathLogin := httptest.NewRequest(http.MethodPost, "/quick-notify/api/v1/admin/login", bytes.NewReader(loginBody))
	recSubpathLogin := httptest.NewRecorder()
	srv.Handler.ServeHTTP(recSubpathLogin, reqSubpathLogin)
	if recSubpathLogin.Code != http.StatusOK {
		t.Fatalf("expected HTTP 200 on subpath login /quick-notify/api/v1/admin/login, got %d", recSubpathLogin.Code)
	}
}

type mockNotificationService struct {
	lastEnvelope domain.NotificationEnvelope
}

func (m *mockNotificationService) ProcessEvent(ctx context.Context, env domain.NotificationEnvelope) error {
	m.lastEnvelope = env
	return nil
}

func TestHTTPServer_InboundAPIKeyAndScopeGuard(t *testing.T) {
	logger := zap.NewNop()
	wsHub := websocket.NewHub(logger)
	consumerManager := kafka.NewConsumerManager(nil, nil, logger)
	mockSvc := &mockNotificationService{}

	cfg := &config.AppConfig{
		Server: config.ServerConfig{
			HTTPPort:       8080,
			AllowedOrigins: []string{"*"},
		},
		Auth: config.AuthConfig{
			JWTSecret: "test_shared_jwt_secret",
		},
		Admin: config.AdminConfig{
			Enabled:   true,
			Username:  "admin",
			Password:  "admin123",
			JWTSecret: "test_admin_jwt_secret",
		},
		Projects: config.ProjectRegistry{
			Projects: []domain.ProjectCredential{
				{
					ProjectID:     "shorterlink",
					Name:          "ShorterLink",
					APIKey:        "key_shorterlink_abc",
					JWTSecret:     "jwt_shorterlink_sec",
					AllowedScopes: []string{"user", "project_global"},
					Enabled:       true,
				},
				{
					ProjectID:     "quickbite",
					Name:          "QuickBite",
					APIKey:        "key_quickbite_def",
					JWTSecret:     "jwt_quickbite_sec",
					AllowedScopes: []string{"user"},
					Enabled:       true,
				},
				{
					ProjectID:     "system",
					Name:          "Root Master Key",
					APIKey:        "key_system_root",
					JWTSecret:     "jwt_system_sec",
					AllowedScopes: []string{"*"},
					Enabled:       true,
				},
			},
			APIKeyIndex: map[string]domain.ProjectCredential{
				"key_shorterlink_abc": {
					ProjectID:     "shorterlink",
					APIKey:        "key_shorterlink_abc",
					AllowedScopes: []string{"user", "project_global"},
					Enabled:       true,
				},
				"key_quickbite_def": {
					ProjectID:     "quickbite",
					APIKey:        "key_quickbite_def",
					AllowedScopes: []string{"user"},
					Enabled:       true,
				},
				"key_system_root": {
					ProjectID:     "system",
					APIKey:        "key_system_root",
					AllowedScopes: []string{"*"},
					Enabled:       true,
				},
			},
			ProjectIndex: map[string]domain.ProjectCredential{
				"shorterlink": {
					ProjectID: "shorterlink",
					JWTSecret: "jwt_shorterlink_sec",
				},
				"quickbite": {
					ProjectID: "quickbite",
					JWTSecret: "jwt_quickbite_sec",
				},
			},
		},
	}

	srv := initHTTPServer(cfg, wsHub, mockSvc, consumerManager, nil, nil, logger)

	makePayload := func(projectID string, scope domain.Scope) []byte {
		env := domain.NotificationEnvelope{
			EventID:   "evt_1",
			ProjectID: projectID,
			Scope:     scope,
			Channels:  []domain.Channel{domain.ChannelWebSocket},
			Target:    domain.TargetInfo{UserID: "usr_1"},
			Content: domain.ContentInfo{
				Type: "raw",
				Data: map[string]interface{}{"message": "hello"},
			},
		}
		b, _ := json.Marshal(env)
		return b
	}

	// 1. Success: ShorterLink API key with ShorterLink user scope
	req1 := httptest.NewRequest(http.MethodPost, "/api/v1/notify", bytes.NewReader(makePayload("shorterlink", domain.ScopeUser)))
	req1.Header.Set("X-API-Key", "key_shorterlink_abc")
	rec1 := httptest.NewRecorder()
	srv.Handler.ServeHTTP(rec1, req1)
	if rec1.Code != http.StatusOK {
		t.Fatalf("case 1: expected 200 OK, got %d: %s", rec1.Code, rec1.Body.String())
	}

	// 2. Failure: Missing API key
	req2 := httptest.NewRequest(http.MethodPost, "/api/v1/notify", bytes.NewReader(makePayload("shorterlink", domain.ScopeUser)))
	rec2 := httptest.NewRecorder()
	srv.Handler.ServeHTTP(rec2, req2)
	if rec2.Code != http.StatusUnauthorized {
		t.Fatalf("case 2: expected 401 Unauthorized for missing key, got %d", rec2.Code)
	}

	// 3. Failure: Invalid API key
	req3 := httptest.NewRequest(http.MethodPost, "/api/v1/notify", bytes.NewReader(makePayload("shorterlink", domain.ScopeUser)))
	req3.Header.Set("X-API-Key", "wrong_key_123")
	rec3 := httptest.NewRecorder()
	srv.Handler.ServeHTTP(rec3, req3)
	if rec3.Code != http.StatusUnauthorized {
		t.Fatalf("case 3: expected 401 Unauthorized for wrong key, got %d", rec3.Code)
	}

	// 4. Failure: Anti-Spoofing - ShorterLink API key attempting to dispatch for QuickBite
	req4 := httptest.NewRequest(http.MethodPost, "/api/v1/notify", bytes.NewReader(makePayload("quickbite", domain.ScopeUser)))
	req4.Header.Set("X-API-Key", "key_shorterlink_abc")
	rec4 := httptest.NewRecorder()
	srv.Handler.ServeHTTP(rec4, req4)
	if rec4.Code != http.StatusForbidden {
		t.Fatalf("case 4: expected 403 Forbidden for anti-spoofing rejection, got %d: %s", rec4.Code, rec4.Body.String())
	}

	// 5. Failure: Scope Guard - ShorterLink API key attempting to dispatch system_global
	req5 := httptest.NewRequest(http.MethodPost, "/api/v1/notify", bytes.NewReader(makePayload("shorterlink", domain.ScopeSystemGlobal)))
	req5.Header.Set("X-API-Key", "key_shorterlink_abc")
	rec5 := httptest.NewRecorder()
	srv.Handler.ServeHTTP(rec5, req5)
	if rec5.Code != http.StatusForbidden {
		t.Fatalf("case 5: expected 403 Forbidden for scope guard rejection, got %d: %s", rec5.Code, rec5.Body.String())
	}

	// 6. Success: Root Master Key can dispatch system_global for any project
	req6 := httptest.NewRequest(http.MethodPost, "/api/v1/notify", bytes.NewReader(makePayload("shorterlink", domain.ScopeSystemGlobal)))
	req6.Header.Set("Authorization", "Bearer key_system_root")
	rec6 := httptest.NewRecorder()
	srv.Handler.ServeHTTP(rec6, req6)
	if rec6.Code != http.StatusOK {
		t.Fatalf("case 6: expected 200 OK for root key, got %d: %s", rec6.Code, rec6.Body.String())
	}
}

func TestHTTPServer_ScheduleManagementAPI(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "quicknotify-test-sched-*")
	if err != nil {
		t.Fatalf("temp dir error: %v", err)
	}
	defer os.RemoveAll(tempDir)

	logger := zap.NewNop()
	wsHub := websocket.NewHub(logger)
	consumerManager := kafka.NewConsumerManager(nil, nil, logger)
	mockSvc := &mockNotificationService{}

	store, err := storage.NewJSONScheduleStore(tempDir, logger)
	if err != nil {
		t.Fatalf("store init error: %v", err)
	}
	schedulerSvc := services.NewSchedulerService(store, mockSvc, logger)

	cfg := &config.AppConfig{
		Server: config.ServerConfig{
			HTTPPort:       8080,
			AllowedOrigins: []string{"*"},
		},
		Projects: config.ProjectRegistry{
			Projects: []domain.ProjectCredential{
				{
					ProjectID:     "shorterlink",
					Name:          "ShorterLink",
					APIKey:        "key_shorterlink_123",
					AllowedScopes: []string{"user", "project_global"},
					Enabled:       true,
				},
			},
			APIKeyIndex: map[string]domain.ProjectCredential{
				"key_shorterlink_123": {
					ProjectID:     "shorterlink",
					APIKey:        "key_shorterlink_123",
					AllowedScopes: []string{"user", "project_global"},
					Enabled:       true,
				},
			},
		},
	}

	srv := initHTTPServer(cfg, wsHub, mockSvc, consumerManager, store, schedulerSvc, logger)

	// 1. Create a schedule for ShorterLink
	createPayload := map[string]interface{}{
		"name":           "Daily ShorterLink Report",
		"type":           "interval",
		"expression":     "1h",
		"execution_mode": "sequential",
		"enabled":        true,
		"actions": []map[string]interface{}{
			{
				"name":             "Broadcast Summary",
				"delay_before_sec": 0,
				"project_id":       "shorterlink",
				"scope":            "project_global",
				"channels":         []string{"websocket"},
				"content": map[string]interface{}{
					"type": "raw",
					"data": map[string]interface{}{"message": "Daily summary"},
				},
			},
		},
	}
	bodyBytes, _ := json.Marshal(createPayload)

	req1 := httptest.NewRequest(http.MethodPost, "/api/v1/schedules", bytes.NewReader(bodyBytes))
	req1.Header.Set("X-API-Key", "key_shorterlink_123")
	rec1 := httptest.NewRecorder()
	srv.Handler.ServeHTTP(rec1, req1)

	if rec1.Code != http.StatusCreated {
		t.Fatalf("create schedule failed with code %d: %s", rec1.Code, rec1.Body.String())
	}

	var createdSched domain.Schedule
	if err := json.NewDecoder(rec1.Body).Decode(&createdSched); err != nil {
		t.Fatalf("decode created schedule error: %v", err)
	}
	if createdSched.ID == "" || createdSched.OwnerProjectID != "shorterlink" {
		t.Fatalf("unexpected created schedule: %+v", createdSched)
	}

	// 2. List schedules
	req2 := httptest.NewRequest(http.MethodGet, "/api/v1/schedules", nil)
	req2.Header.Set("X-API-Key", "key_shorterlink_123")
	rec2 := httptest.NewRecorder()
	srv.Handler.ServeHTTP(rec2, req2)

	if rec2.Code != http.StatusOK {
		t.Fatalf("list schedules failed with code %d: %s", rec2.Code, rec2.Body.String())
	}

	var list []domain.Schedule
	_ = json.NewDecoder(rec2.Body).Decode(&list)
	if len(list) != 1 || list[0].ID != createdSched.ID {
		t.Fatalf("expected 1 schedule in list, got %d", len(list))
	}

	// 3. Trigger Now
	triggerURL := "/api/v1/schedules/" + createdSched.ID + "/trigger"
	req3 := httptest.NewRequest(http.MethodPost, triggerURL, nil)
	req3.Header.Set("X-API-Key", "key_shorterlink_123")
	rec3 := httptest.NewRecorder()
	srv.Handler.ServeHTTP(rec3, req3)

	if rec3.Code != http.StatusOK {
		t.Fatalf("trigger schedule failed with code %d: %s", rec3.Code, rec3.Body.String())
	}
	if mockSvc.lastEnvelope.ProjectID != "shorterlink" {
		t.Fatalf("expected dispatched envelope to have project_id shorterlink, got: %+v", mockSvc.lastEnvelope)
	}

	// 4. Toggle Active/Pause
	toggleURL := "/api/v1/schedules/" + createdSched.ID + "/toggle"
	req4 := httptest.NewRequest(http.MethodPost, toggleURL, nil)
	req4.Header.Set("X-API-Key", "key_shorterlink_123")
	rec4 := httptest.NewRecorder()
	srv.Handler.ServeHTTP(rec4, req4)

	if rec4.Code != http.StatusOK {
		t.Fatalf("toggle schedule failed with code %d: %s", rec4.Code, rec4.Body.String())
	}
	var toggled domain.Schedule
	_ = json.NewDecoder(rec4.Body).Decode(&toggled)
	if toggled.Enabled != false {
		t.Fatalf("expected enabled to be toggled to false, got true")
	}

	// 5. Scope Guard Violation: ShorterLink attempts to create action with system_global
	badScopePayload := map[string]interface{}{
		"name":           "Malicious System Alert",
		"type":           "interval",
		"expression":     "1h",
		"execution_mode": "sequential",
		"actions": []map[string]interface{}{
			{
				"name":       "System Hijack",
				"project_id": "shorterlink",
				"scope":      "system_global",
				"channels":   []string{"websocket"},
				"content":    map[string]interface{}{"type": "raw", "data": map[string]interface{}{"message": "hacked"}},
			},
		},
	}
	badScopeBytes, _ := json.Marshal(badScopePayload)
	req5 := httptest.NewRequest(http.MethodPost, "/api/v1/schedules", bytes.NewReader(badScopeBytes))
	req5.Header.Set("X-API-Key", "key_shorterlink_123")
	rec5 := httptest.NewRecorder()
	srv.Handler.ServeHTTP(rec5, req5)

	if rec5.Code != http.StatusForbidden {
		t.Fatalf("expected 403 Forbidden for scope guard violation, got %d: %s", rec5.Code, rec5.Body.String())
	}

	// 6. Delete Schedule
	delURL := "/api/v1/schedules/" + createdSched.ID
	req6 := httptest.NewRequest(http.MethodDelete, delURL, nil)
	req6.Header.Set("X-API-Key", "key_shorterlink_123")
	rec6 := httptest.NewRecorder()
	srv.Handler.ServeHTTP(rec6, req6)

	if rec6.Code != http.StatusOK {
		t.Fatalf("delete schedule failed with code %d: %s", rec6.Code, rec6.Body.String())
	}

	// Verify it's deleted
	req7 := httptest.NewRequest(http.MethodGet, "/api/v1/schedules", nil)
	req7.Header.Set("X-API-Key", "key_shorterlink_123")
	rec7 := httptest.NewRecorder()
	srv.Handler.ServeHTTP(rec7, req7)
	var emptyList []domain.Schedule
	_ = json.NewDecoder(rec7.Body).Decode(&emptyList)
	if len(emptyList) != 0 {
		t.Fatalf("expected 0 schedules after deletion, got %d", len(emptyList))
	}
}

