package websocket

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"go.uber.org/zap"
)

func generateTestJWT(projectID, userID, secret string) (string, error) {
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"project_id": projectID,
		"user_id":    userID,
		"exp":        time.Now().Add(1 * time.Hour).Unix(),
	})
	return token.SignedString([]byte(secret))
}

func TestAuthMiddleware_PerProjectSecret(t *testing.T) {
	logger := zap.NewNop()

	// Dynamic resolver simulating project-specific JWT secrets
	secrets := map[string]string{
		"shorterlink": "secret_shorterlink_123",
		"quickbite":   "secret_quickbite_456",
	}
	resolver := func(projectID string) string {
		if s, ok := secrets[projectID]; ok {
			return s
		}
		return "fallback_default_secret"
	}

	middleware := AuthMiddlewareWithResolver(resolver, logger)

	var capturedProject, capturedUser string
	handler := middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		pID, uID, ok := GetAuthInfo(r.Context())
		if ok {
			capturedProject = pID
			capturedUser = uID
		}
		w.WriteHeader(http.StatusOK)
	}))

	// Case 1: Valid ShorterLink token signed with ShorterLink secret
	shorterToken, _ := generateTestJWT("shorterlink", "user_sl_1", "secret_shorterlink_123")
	req1 := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/ws?token=%s", shorterToken), nil)
	rec1 := httptest.NewRecorder()
	handler.ServeHTTP(rec1, req1)

	if rec1.Code != http.StatusOK {
		t.Fatalf("case 1: expected 200 OK, got %d", rec1.Code)
	}
	if capturedProject != "shorterlink" || capturedUser != "user_sl_1" {
		t.Fatalf("case 1: unexpected context auth: %s / %s", capturedProject, capturedUser)
	}

	// Case 2: ShorterLink claims signed with QuickBite secret (spoofing attempt)
	spoofedToken, _ := generateTestJWT("shorterlink", "user_sl_1", "secret_quickbite_456")
	req2 := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/ws?token=%s", spoofedToken), nil)
	rec2 := httptest.NewRecorder()
	handler.ServeHTTP(rec2, req2)

	if rec2.Code != http.StatusUnauthorized {
		t.Fatalf("case 2: expected 401 Unauthorized for spoofed token, got %d", rec2.Code)
	}

	// Case 3: Unknown project falls back to default fallback secret
	fallbackToken, _ := generateTestJWT("otherapp", "user_other", "fallback_default_secret")
	req3 := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/ws?token=%s", fallbackToken), nil)
	rec3 := httptest.NewRecorder()
	handler.ServeHTTP(rec3, req3)

	if rec3.Code != http.StatusOK {
		t.Fatalf("case 3: expected 200 OK with fallback secret, got %d", rec3.Code)
	}

	// Case 4: Missing token
	req4 := httptest.NewRequest(http.MethodGet, "/ws", nil)
	rec4 := httptest.NewRecorder()
	handler.ServeHTTP(rec4, req4)

	if rec4.Code != http.StatusUnauthorized {
		t.Fatalf("case 4: expected 401 Unauthorized for missing token, got %d", rec4.Code)
	}
}

