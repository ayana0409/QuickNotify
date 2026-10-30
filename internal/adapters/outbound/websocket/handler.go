package websocket

import (
	"net/http"

	"github.com/gorilla/websocket"
	"go.uber.org/zap"
)

// Handler handles incoming WebSocket upgrade requests with strict Origin checking.
type Handler struct {
	hub            *Hub
	allowedOrigins []string
	upgrader       websocket.Upgrader
	logger         *zap.Logger
}

// NewHandler creates a new WebSocket route Handler with configurable allowed origins.
func NewHandler(hub *Hub, allowedOrigins []string, logger *zap.Logger) *Handler {
	return &Handler{
		hub:            hub,
		allowedOrigins: allowedOrigins,
		logger:         logger,
		upgrader: websocket.Upgrader{
			ReadBufferSize:  1024,
			WriteBufferSize: 1024,
			CheckOrigin: func(r *http.Request) bool {
				origin := r.Header.Get("Origin")
				allowed := IsOriginAllowed(origin, allowedOrigins)
				if !allowed {
					logger.Warn("WebSocket handshake rejected: Origin not allowed",
						zap.String("origin", origin),
						zap.String("remote_addr", r.RemoteAddr),
					)
				}
				return allowed
			},
		},
	}
}

// ServeHTTP handles upgrading HTTP connection to WebSocket, registers the client,
// and starts the read and write pumps.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	projectID, userID, ok := GetAuthInfo(r.Context())
	if !ok {
		h.logger.Error("WebSocket upgrade requested without authenticated context")
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	conn, err := h.upgrader.Upgrade(w, r, nil)
	if err != nil {
		h.logger.Error("Failed to upgrade connection to WebSocket",
			zap.String("project_id", projectID),
			zap.String("user_id", userID),
			zap.Error(err),
		)
		return
	}

	client := NewClient(h.hub, conn, projectID, userID, h.logger)
	h.hub.Register(client)

	h.logger.Info("Client connected successfully",
		zap.String("project_id", projectID),
		zap.String("user_id", userID),
	)

	// Start client goroutines
	go client.WritePump()
	go client.ReadPump()
}
