package websocket

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestIsOriginAllowed(t *testing.T) {
	allowed := []string{
		"http://localhost:3000",
		"http://localhost:5173",
		"https://*.mydomain.com",
		"http://127.0.0.1:*",
	}

	tests := []struct {
		name     string
		origin   string
		expected bool
	}{
		{"Exact match port 3000", "http://localhost:3000", true},
		{"Exact match port 5173", "http://localhost:5173", true},
		{"Subdomain wildcard match", "https://app.mydomain.com", true},
		{"Another subdomain wildcard", "https://dashboard.mydomain.com", true},
		{"Localhost port wildcard", "http://127.0.0.1:8080", true},
		{"Empty origin (native mobile/curl)", "", true},
		{"Disallowed external domain", "https://malicious-site.com", false},
		{"Disallowed port", "http://localhost:4000", false},
		{"Different root domain", "https://mydomain.com.evil.com", false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := IsOriginAllowed(tc.origin, allowed)
			if result != tc.expected {
				t.Errorf("origin '%s': expected %v, got %v", tc.origin, tc.expected, result)
			}
		})
	}
}

func TestIsOriginAllowed_WildcardStar(t *testing.T) {
	starAllowed := []string{"*"}
	if !IsOriginAllowed("http://anything.com", starAllowed) {
		t.Errorf("expected wildcard '*' to allow any origin")
	}
}

func TestCORSMiddleware_PreflightAndHeaders(t *testing.T) {
	allowed := []string{"http://localhost:3000"}
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	cors := CORSMiddleware(allowed)(handler)

	// 1. Test OPTIONS Preflight request
	reqOpt := httptest.NewRequest(http.MethodOptions, "/ws", nil)
	reqOpt.Header.Set("Origin", "http://localhost:3000")
	recOpt := httptest.NewRecorder()

	cors.ServeHTTP(recOpt, reqOpt)

	if recOpt.Code != http.StatusNoContent {
		t.Errorf("expected status 204 for OPTIONS preflight, got %d", recOpt.Code)
	}
	if recOpt.Header().Get("Access-Control-Allow-Origin") != "http://localhost:3000" {
		t.Errorf("expected Access-Control-Allow-Origin header to match allowed origin")
	}

	// 2. Test Disallowed origin OPTIONS
	reqDis := httptest.NewRequest(http.MethodOptions, "/ws", nil)
	reqDis.Header.Set("Origin", "http://evil.com")
	recDis := httptest.NewRecorder()

	cors.ServeHTTP(recDis, reqDis)
	if recDis.Header().Get("Access-Control-Allow-Origin") != "" {
		t.Errorf("expected empty Access-Control-Allow-Origin for disallowed origin")
	}
}

