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
	"quicknotify/internal/core/services"
	"quicknotify/internal/pkg/config"

	"go.uber.org/zap"
)

// initHTTPServer builds the HTTP mux, registers routes (health check + WebSocket + Schedules),
// wraps the mux with the CORS middleware, and returns a configured *http.Server
// ready to call ListenAndServe.
func initHTTPServer(
	cfg *config.AppConfig,
	wsHub *websocket.Hub,
	dispatcher ports.NotificationService,
	consumerManager *kafka.ConsumerManager,
	scheduleStore ports.ScheduleStore,
	schedulerSvc *services.SchedulerService,
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
	// Protected by multi-tenant API Key authentication, Anti-Spoofing, and Scope Guard.
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

		// Authenticate and authorize API Key & Scope Guard
		if errMsg, statusCode := authenticateInboundRequest(r, env, cfg); statusCode != http.StatusOK {
			logger.Warn("Inbound notification rejected by security guard",
				zap.String("project_id", env.ProjectID),
				zap.String("scope", string(env.Scope)),
				zap.Int("status", statusCode),
				zap.String("reason", errMsg),
				zap.String("remote_addr", r.RemoteAddr),
			)
			http.Error(w, errMsg, statusCode)
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

		configuredProjects := make([]map[string]string, 0, len(cfg.Projects.Projects))
		for _, p := range cfg.Projects.Projects {
			configuredProjects = append(configuredProjects, map[string]string{
				"project_id": p.ProjectID,
				"name":       p.Name,
			})
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"ws_connections":      wsHub.TotalConnections(),
			"tenants":             wsHub.GetTenantOverviews(),
			"configured_projects": configuredProjects,
			"kafka_consumers":     consumerManager.TotalConsumers(),
			"redis_enabled":       cfg.Redis.Enabled,
			"timestamp":           time.Now().UTC().Format(time.RFC3339),
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

	// --- Scheduled Notification Workers REST API ---
	registerScheduleRoutes(mux, cfg, scheduleStore, schedulerSvc, logger)

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
	authMiddleware := websocket.AuthMiddlewareWithResolver(func(projectID string) string {
		return cfg.Projects.GetJWTSecret(projectID, cfg.Auth.JWTSecret)
	}, logger)
	mux.Handle("/ws", authMiddleware(wsHandler))

	// --- Wrap the whole mux with CORS and Subpath middleware ---
	// The CORS middleware handles preflight OPTIONS requests and adds
	// appropriate headers for cross-origin regular HTTP calls.
	corsHandler := websocket.CORSMiddleware(cfg.Server.AllowedOrigins)(mux)

	// Subpath normalization middleware:
	// Allows QuickNotify to serve seamlessly whether accessed directly at root or
	// behind a subpath reverse proxy (e.g. /quick-notify) with or without path stripping.
	subpathHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p := r.URL.Path
		if cfg.Server.BasePath != "" && strings.HasPrefix(p, cfg.Server.BasePath) {
			trimmed := strings.TrimPrefix(p, cfg.Server.BasePath)
			if trimmed == "" {
				trimmed = "/"
			}
			r.URL.Path = trimmed
		} else if strings.HasPrefix(p, "/quick-notify") {
			trimmed := strings.TrimPrefix(p, "/quick-notify")
			if trimmed == "" {
				trimmed = "/"
			}
			r.URL.Path = trimmed
		}
		corsHandler.ServeHTTP(w, r)
	})

	serverAddr := fmt.Sprintf(":%d", cfg.Server.HTTPPort)
	srv := &http.Server{
		Addr:         serverAddr,
		Handler:      subpathHandler,
		ReadTimeout:  time.Duration(cfg.Server.ReadTimeoutSec) * time.Second,
		WriteTimeout: time.Duration(cfg.Server.WriteTimeoutSec) * time.Second,
	}

	return srv
}

// authenticateInboundRequest checks whether an inbound HTTP notification request is authenticated and authorized.
// It verifies:
// 1. API Key existence and status (enabled/disabled).
// 2. Anti-Spoofing: cred.ProjectID must match env.ProjectID (unless master system key).
// 3. Scope Guard: cred.HasScope(env.Scope).
// It also permits valid Admin JWT session tokens with system authority.
func authenticateInboundRequest(r *http.Request, env domain.NotificationEnvelope, cfg *config.AppConfig) (string, int) {
	var token string
	if apiKey := r.Header.Get("X-API-Key"); apiKey != "" {
		token = strings.TrimSpace(apiKey)
	} else if authHeader := r.Header.Get("Authorization"); authHeader != "" {
		parts := strings.SplitN(authHeader, " ", 2)
		if len(parts) == 2 && (strings.EqualFold(parts[0], "Bearer") || strings.EqualFold(parts[0], "ApiKey")) {
			token = strings.TrimSpace(parts[1])
		} else {
			token = strings.TrimSpace(authHeader)
		}
	}

	// 1. If an Admin JWT session token is provided, validate it
	if token != "" {
		if _, ok := validateAdminJWT(token, cfg); ok {
			return "", http.StatusOK
		}
	}

	// 2. If Project Credentials are configured in the system, strictly enforce API Key & Scope Guard
	if len(cfg.Projects.Projects) > 0 {
		if token == "" {
			return "Unauthorized: missing API key (X-API-Key or Authorization header required)", http.StatusUnauthorized
		}

		cred, found := cfg.Projects.GetByAPIKey(token)
		if !found {
			return "Unauthorized: invalid API key", http.StatusUnauthorized
		}

		if !cred.Enabled {
			return fmt.Sprintf("Forbidden: API key for project '%s' is disabled", cred.ProjectID), http.StatusForbidden
		}

		// Layer 2: Anti-Spoofing check
		if !cred.CanPublishFor(env.ProjectID) {
			return fmt.Sprintf("Forbidden: API key for project '%s' cannot publish for project '%s'", cred.ProjectID, env.ProjectID), http.StatusForbidden
		}

		// Layer 3: Scope Guard check
		if !cred.HasScope(env.Scope) {
			return fmt.Sprintf("Forbidden: API key for project '%s' is not authorized for scope '%s'", cred.ProjectID, env.Scope), http.StatusForbidden
		}

		return "", http.StatusOK
	}

	// 3. Fallback when no projects are configured:
	// If a token was supplied but failed admin authentication:
	if token != "" {
		return "Unauthorized: invalid API key or admin token", http.StatusUnauthorized
	}

	// Allow unauthenticated when zero project credentials are configured (backward compatibility for basic tests)
	return "", http.StatusOK
}

// validateAdminJWT verifies if the given token is a valid Admin session JWT.
func validateAdminJWT(tokenStr string, cfg *config.AppConfig) (jwt.MapClaims, bool) {
	secret := cfg.Admin.JWTSecret
	if secret == "" {
		secret = cfg.Auth.JWTSecret
	}
	token, err := jwt.Parse(tokenStr, func(token *jwt.Token) (interface{}, error) {
		if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("unexpected signing method: %v", token.Header["alg"])
		}
		return []byte(secret), nil
	})
	if err != nil || !token.Valid {
		return nil, false
	}
	claims, ok := token.Claims.(jwt.MapClaims)
	if !ok || claims["role"] != "admin" {
		return nil, false
	}
	return claims, true
}

// authenticateScheduleRequest authenticates incoming requests to schedule management APIs.
func authenticateScheduleRequest(r *http.Request, cfg *config.AppConfig) (string, domain.ProjectCredential, string, int) {
	var token string
	if apiKey := r.Header.Get("X-API-Key"); apiKey != "" {
		token = strings.TrimSpace(apiKey)
	} else if authHeader := r.Header.Get("Authorization"); authHeader != "" {
		parts := strings.SplitN(authHeader, " ", 2)
		if len(parts) == 2 && (strings.EqualFold(parts[0], "Bearer") || strings.EqualFold(parts[0], "ApiKey")) {
			token = strings.TrimSpace(parts[1])
		} else {
			token = strings.TrimSpace(authHeader)
		}
	}

	if token != "" {
		// 1. Check if token is Admin JWT Session
		if _, ok := validateAdminJWT(token, cfg); ok {
			return "system", domain.ProjectCredential{
				ProjectID:     "system",
				Name:          "System Admin",
				AllowedScopes: []string{"*"},
				Enabled:       true,
			}, "", http.StatusOK
		}

		// 2. Check if token matches a registered Project API Key
		if cred, found := cfg.Projects.GetByAPIKey(token); found {
			if !cred.Enabled {
				return "", domain.ProjectCredential{}, fmt.Sprintf("Forbidden: API key for project '%s' is disabled", cred.ProjectID), http.StatusForbidden
			}
			return cred.ProjectID, cred, "", http.StatusOK
		}

		return "", domain.ProjectCredential{}, "Unauthorized: invalid API key or admin token", http.StatusUnauthorized
	}

	// 3. Fallback when zero project credentials configured (development mode)
	if len(cfg.Projects.Projects) == 0 {
		return "system", domain.ProjectCredential{
			ProjectID:     "system",
			Name:          "System Admin",
			AllowedScopes: []string{"*"},
			Enabled:       true,
		}, "", http.StatusOK
	}

	return "", domain.ProjectCredential{}, "Unauthorized: missing API key or admin token (X-API-Key or Authorization header required)", http.StatusUnauthorized
}

// registerScheduleRoutes wires the REST endpoints for scheduled notification workers.
func registerScheduleRoutes(
	mux *http.ServeMux,
	cfg *config.AppConfig,
	store ports.ScheduleStore,
	scheduler *services.SchedulerService,
	logger *zap.Logger,
) {
	if store == nil {
		return
	}

	// 1. Collection endpoint: GET (list) and POST (create)
	mux.HandleFunc("/api/v1/schedules", func(w http.ResponseWriter, r *http.Request) {
		ownerProjectID, cred, errMsg, status := authenticateScheduleRequest(r, cfg)
		if status != http.StatusOK {
			http.Error(w, errMsg, status)
			return
		}

		switch r.Method {
		case http.MethodGet:
			schedules, err := store.List(r.Context(), ownerProjectID)
			if err != nil {
				http.Error(w, fmt.Sprintf("Failed to list schedules: %v", err), http.StatusInternalServerError)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(schedules)

		case http.MethodPost:
			var sched domain.Schedule
			if err := json.NewDecoder(r.Body).Decode(&sched); err != nil {
				http.Error(w, fmt.Sprintf("Invalid schedule payload: %v", err), http.StatusBadRequest)
				return
			}

			if strings.TrimSpace(sched.Name) == "" {
				http.Error(w, "Schedule name cannot be empty", http.StatusBadRequest)
				return
			}
			if len(sched.Actions) == 0 {
				http.Error(w, "Schedule must contain at least one notification action", http.StatusBadRequest)
				return
			}

			// Enforce ownership: non-admin callers can only create schedules for their project
			if ownerProjectID != "system" {
				sched.OwnerProjectID = ownerProjectID
			} else if sched.OwnerProjectID == "" {
				sched.OwnerProjectID = "system"
			}

			// Validate each action for anti-spoofing and scope permissions
			for i, act := range sched.Actions {
				if strings.TrimSpace(act.ProjectID) == "" {
					sched.Actions[i].ProjectID = sched.OwnerProjectID
					act.ProjectID = sched.OwnerProjectID
				}
				if !cred.CanPublishFor(act.ProjectID) {
					http.Error(w, fmt.Sprintf("Forbidden: cannot configure action for project '%s'", act.ProjectID), http.StatusForbidden)
					return
				}
				if !cred.HasScope(act.Scope) {
					http.Error(w, fmt.Sprintf("Forbidden: not authorized for scope '%s' on project '%s'", act.Scope, act.ProjectID), http.StatusForbidden)
					return
				}
				if act.ActionID == "" {
					sched.Actions[i].ActionID = fmt.Sprintf("act_%d", i+1)
				}
			}

			if sched.ExecutionMode == "" {
				sched.ExecutionMode = domain.ExecutionSequential
			}

			if sched.ID == "" {
				sched.ID = fmt.Sprintf("sched_%d", time.Now().UnixNano())
			}

			// Compute NextRunAt if scheduler is active and schedule is enabled
			if scheduler != nil && sched.Enabled {
				next, err := scheduler.ComputeNextRun(&sched, time.Now().UTC())
				if err != nil {
					http.Error(w, fmt.Sprintf("Invalid schedule timing configuration: %v", err), http.StatusBadRequest)
					return
				}
				sched.NextRunAt = next
			}

			if err := store.Save(r.Context(), &sched); err != nil {
				http.Error(w, fmt.Sprintf("Failed to save schedule: %v", err), http.StatusInternalServerError)
				return
			}

			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusCreated)
			_ = json.NewEncoder(w).Encode(sched)

		default:
			http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		}
	})

	// 2. Item endpoint: GET /api/v1/schedules/{id}, PUT, DELETE, /trigger, /toggle
	mux.HandleFunc("/api/v1/schedules/", func(w http.ResponseWriter, r *http.Request) {
		ownerProjectID, cred, errMsg, status := authenticateScheduleRequest(r, cfg)
		if status != http.StatusOK {
			http.Error(w, errMsg, status)
			return
		}

		subPath := strings.TrimPrefix(r.URL.Path, "/api/v1/schedules/")
		parts := strings.Split(strings.Trim(subPath, "/"), "/")
		if len(parts) == 0 || parts[0] == "" {
			http.Error(w, "Schedule ID required", http.StatusBadRequest)
			return
		}

		scheduleID := parts[0]
		action := ""
		if len(parts) > 1 {
			action = parts[1]
		}

		// Handle sub-action: /trigger (Run Now)
		if action == "trigger" {
			if r.Method != http.MethodPost {
				http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
				return
			}
			existing, err := store.Get(r.Context(), scheduleID)
			if err != nil {
				http.Error(w, "Schedule not found", http.StatusNotFound)
				return
			}
			if ownerProjectID != "system" && !strings.EqualFold(existing.OwnerProjectID, ownerProjectID) {
				http.Error(w, "Forbidden: you do not own this schedule", http.StatusForbidden)
				return
			}

			if scheduler != nil {
				if err := scheduler.TriggerNow(r.Context(), scheduleID); err != nil {
					http.Error(w, fmt.Sprintf("Failed to trigger schedule: %v", err), http.StatusInternalServerError)
					return
				}
			}

			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"status":      "triggered",
				"schedule_id": scheduleID,
				"message":     "Lịch trình đã được kích hoạt chạy ngay lập tức",
			})
			return
		}

		// Handle sub-action: /toggle (Active / Pause)
		if action == "toggle" {
			if r.Method != http.MethodPost && r.Method != http.MethodPatch {
				http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
				return
			}
			existing, err := store.Get(r.Context(), scheduleID)
			if err != nil {
				http.Error(w, "Schedule not found", http.StatusNotFound)
				return
			}
			if ownerProjectID != "system" && !strings.EqualFold(existing.OwnerProjectID, ownerProjectID) {
				http.Error(w, "Forbidden: you do not own this schedule", http.StatusForbidden)
				return
			}

			existing.Enabled = !existing.Enabled
			if existing.Enabled && scheduler != nil {
				next, _ := scheduler.ComputeNextRun(existing, time.Now().UTC())
				existing.NextRunAt = next
			} else {
				existing.NextRunAt = nil
			}

			if err := store.Save(r.Context(), existing); err != nil {
				http.Error(w, fmt.Sprintf("Failed to update toggle: %v", err), http.StatusInternalServerError)
				return
			}

			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(existing)
			return
		}

		switch r.Method {
		case http.MethodGet:
			existing, err := store.Get(r.Context(), scheduleID)
			if err != nil {
				http.Error(w, "Schedule not found", http.StatusNotFound)
				return
			}
			if ownerProjectID != "system" && !strings.EqualFold(existing.OwnerProjectID, ownerProjectID) {
				http.Error(w, "Forbidden: you do not own this schedule", http.StatusForbidden)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(existing)

		case http.MethodPut:
			existing, err := store.Get(r.Context(), scheduleID)
			if err != nil {
				http.Error(w, "Schedule not found", http.StatusNotFound)
				return
			}
			if ownerProjectID != "system" && !strings.EqualFold(existing.OwnerProjectID, ownerProjectID) {
				http.Error(w, "Forbidden: you do not own this schedule", http.StatusForbidden)
				return
			}

			var updated domain.Schedule
			if err := json.NewDecoder(r.Body).Decode(&updated); err != nil {
				http.Error(w, fmt.Sprintf("Invalid schedule payload: %v", err), http.StatusBadRequest)
				return
			}

			updated.ID = scheduleID
			updated.OwnerProjectID = existing.OwnerProjectID
			updated.CreatedAt = existing.CreatedAt
			updated.RunCount = existing.RunCount
			updated.LastRunAt = existing.LastRunAt

			for i, act := range updated.Actions {
				if strings.TrimSpace(act.ProjectID) == "" {
					updated.Actions[i].ProjectID = updated.OwnerProjectID
					act.ProjectID = updated.OwnerProjectID
				}
				if !cred.CanPublishFor(act.ProjectID) {
					http.Error(w, fmt.Sprintf("Forbidden: cannot configure action for project '%s'", act.ProjectID), http.StatusForbidden)
					return
				}
				if !cred.HasScope(act.Scope) {
					http.Error(w, fmt.Sprintf("Forbidden: not authorized for scope '%s' on project '%s'", act.Scope, act.ProjectID), http.StatusForbidden)
					return
				}
				if act.ActionID == "" {
					updated.Actions[i].ActionID = fmt.Sprintf("act_%d", i+1)
				}
			}

			if scheduler != nil && updated.Enabled {
				next, err := scheduler.ComputeNextRun(&updated, time.Now().UTC())
				if err != nil {
					http.Error(w, fmt.Sprintf("Invalid schedule configuration: %v", err), http.StatusBadRequest)
					return
				}
				updated.NextRunAt = next
			} else if !updated.Enabled {
				updated.NextRunAt = nil
			}

			if err := store.Save(r.Context(), &updated); err != nil {
				http.Error(w, fmt.Sprintf("Failed to update schedule: %v", err), http.StatusInternalServerError)
				return
			}

			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(updated)

		case http.MethodDelete:
			existing, err := store.Get(r.Context(), scheduleID)
			if err != nil {
				http.Error(w, "Schedule not found", http.StatusNotFound)
				return
			}
			if ownerProjectID != "system" && !strings.EqualFold(existing.OwnerProjectID, ownerProjectID) {
				http.Error(w, "Forbidden: you do not own this schedule", http.StatusForbidden)
				return
			}

			if err := store.Delete(r.Context(), scheduleID); err != nil {
				http.Error(w, fmt.Sprintf("Failed to delete schedule: %v", err), http.StatusInternalServerError)
				return
			}

			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"status":  "deleted",
				"id":      scheduleID,
				"message": "Đã xóa lịch trình thành công",
			})

		default:
			http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		}
	})
}

