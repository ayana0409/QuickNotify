package app

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"

	"quicknotify/internal/adapters/inbound/kafka"
	"quicknotify/internal/adapters/outbound/websocket"
	"quicknotify/internal/core/domain"
	"quicknotify/internal/core/ports"
	"quicknotify/internal/pkg/config"

	"go.uber.org/zap"
)

// initHTTPServer builds the HTTP mux, registers routes (health check + WebSocket),
// wraps the mux with the CORS middleware, and returns a configured *http.Server
// ready to call ListenAndServe.
func initHTTPServer(
	cfg *config.AppConfig,
	wsHub *websocket.Hub,
	dispatcher ports.NotificationService,
	consumerManager *kafka.ConsumerManager,
	logger *zap.Logger,
) *http.Server {
	mux := http.NewServeMux()

	// --- Health / liveness probe ---
	// Returns a JSON snapshot of service state; used by load-balancers and
	// orchestrators (Docker health-check, Kubernetes liveness probe, etc.)
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"status":          "healthy",
			"service":         "quicknotify",
			"ws_connections":  wsHub.TotalConnections(),
			"kafka_consumers": consumerManager.TotalConsumers(),
			"redis_enabled":   cfg.Redis.Enabled,
			"timestamp":       time.Now().UTC().Format(time.RFC3339),
		})
	})

	// --- HTTP Inbound Notification Endpoint (REST API) ---
	// Enables publishing notifications directly over HTTP without requiring Kafka.
	// Essential for local standalone demo, webhook integrations, and microservices.
	mux.HandleFunc("/api/v1/notify", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
			return
		}

		var env domain.NotificationEnvelope
		if err := json.NewDecoder(r.Body).Decode(&env); err != nil {
			http.Error(w, fmt.Sprintf("Invalid request payload: %v", err), http.StatusBadRequest)
			return
		}

		if err := dispatcher.ProcessEvent(r.Context(), env); err != nil {
			http.Error(w, fmt.Sprintf("Failed to dispatch notification: %v", err), http.StatusBadRequest)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"status":   "dispatched",
			"event_id": env.EventID,
		})
	})

	// --- Admin Login Endpoint ---
	// Authenticates username and password from environment/config and returns a JWT session token.
	mux.HandleFunc("/api/v1/admin/login", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
			return
		}

		var req struct {
			Username string `json:"username"`
			Password string `json:"password"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "Invalid login payload", http.StatusBadRequest)
			return
		}

		if !cfg.Admin.Enabled {
			http.Error(w, "Admin dashboard is disabled in configuration", http.StatusForbidden)
			return
		}

		if req.Username != cfg.Admin.Username || req.Password != cfg.Admin.Password {
			http.Error(w, "Invalid username or password", http.StatusUnauthorized)
			return
		}

		now := time.Now()
		claims := jwt.MapClaims{
			"role":       "admin",
			"project_id": "system",
			"user_id":    req.Username,
			"exp":        now.Add(24 * time.Hour).Unix(),
			"iat":        now.Unix(),
		}

		token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
		secret := cfg.Admin.JWTSecret
		if secret == "" {
			secret = cfg.Auth.JWTSecret
		}
		tokenString, err := token.SignedString([]byte(secret))
		if err != nil {
			http.Error(w, "Failed to sign admin session", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"status":     "authenticated",
			"token":      tokenString,
			"username":   req.Username,
			"expires_in": 86400,
		})
	})

	// --- Admin Stats & Tenant Overview Endpoint ---
	// Returns active WebSocket connections, tenant overview, and cluster status.
	mux.HandleFunc("/api/v1/admin/stats", func(w http.ResponseWriter, r *http.Request) {
		authHeader := r.Header.Get("Authorization")
		if !strings.HasPrefix(authHeader, "Bearer ") {
			http.Error(w, "Unauthorized: missing admin bearer token", http.StatusUnauthorized)
			return
		}

		tokenStr := strings.TrimPrefix(authHeader, "Bearer ")
		secret := cfg.Admin.JWTSecret
		if secret == "" {
			secret = cfg.Auth.JWTSecret
		}

		token, err := jwt.Parse(tokenStr, func(token *jwt.Token) (interface{}, error) {
			if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
				return nil, fmt.Errorf("unexpected signing method")
			}
			return []byte(secret), nil
		})

		if err != nil || !token.Valid {
			http.Error(w, "Unauthorized: invalid or expired admin token", http.StatusUnauthorized)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"ws_connections":  wsHub.TotalConnections(),
			"tenants":         wsHub.GetTenantOverviews(),
			"kafka_consumers": consumerManager.TotalConsumers(),
			"redis_enabled":   cfg.Redis.Enabled,
			"timestamp":       time.Now().UTC().Format(time.RFC3339),
		})
	})

	// --- Serve Admin Dashboard UI directly at / and /admin ---
	serveAdminUI := func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" && r.URL.Path != "/admin" && r.URL.Path != "/admin.html" {
			http.NotFound(w, r)
			return
		}

		candidates := []string{
			"web/admin.html",
			"admin.html",
			"../../web/admin.html",
			"../web/admin.html",
		}

		var data []byte
		var err error
		for _, path := range candidates {
			data, err = os.ReadFile(path)
			if err == nil {
				break
			}
		}

		if err != nil {
			http.Error(w, "admin.html not found on server", http.StatusNotFound)
			return
		}

		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(data)
	}

	mux.HandleFunc("/", serveAdminUI)
	mux.HandleFunc("/admin", serveAdminUI)
	mux.HandleFunc("/admin.html", serveAdminUI)

	// Redirect legacy test page paths to admin dashboard
	mux.HandleFunc("/test", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/", http.StatusMovedPermanently)
	})
	mux.HandleFunc("/test_client.html", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/", http.StatusMovedPermanently)
	})

	// --- WebSocket endpoint ---
	// Protected by JWT AuthMiddleware; the Handler also validates the request
	// Origin against the AllowedOrigins whitelist during the WS upgrade.
	wsHandler := websocket.NewHandler(wsHub, cfg.Server.AllowedOrigins, logger)
	authMiddleware := websocket.AuthMiddleware(cfg.Auth.JWTSecret, logger)
	mux.Handle("/ws", authMiddleware(wsHandler))

	// --- Wrap the whole mux with CORS middleware ---
	// The CORS middleware handles preflight OPTIONS requests and adds
	// appropriate headers for cross-origin regular HTTP calls.
	corsHandler := websocket.CORSMiddleware(cfg.Server.AllowedOrigins)(mux)

	serverAddr := fmt.Sprintf(":%d", cfg.Server.HTTPPort)
	srv := &http.Server{
		Addr:         serverAddr,
		Handler:      corsHandler,
		ReadTimeout:  time.Duration(cfg.Server.ReadTimeoutSec) * time.Second,
		WriteTimeout: time.Duration(cfg.Server.WriteTimeoutSec) * time.Second,
	}

	return srv
}

