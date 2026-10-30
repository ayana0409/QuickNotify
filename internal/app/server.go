package app

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"quicknotify/internal/adapters/inbound/kafka"
	"quicknotify/internal/adapters/outbound/websocket"
	"quicknotify/internal/pkg/config"

	"go.uber.org/zap"
)

// initHTTPServer builds the HTTP mux, registers routes (health check + WebSocket),
// wraps the mux with the CORS middleware, and returns a configured *http.Server
// ready to call ListenAndServe.
func initHTTPServer(
	cfg *config.AppConfig,
	wsHub *websocket.Hub,
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

