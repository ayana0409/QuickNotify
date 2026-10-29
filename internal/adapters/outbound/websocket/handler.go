package websocket

import (
	"net/http"

	"github.com/gorilla/websocket"
	"go.uber.org/zap"
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	// Allow all origins for internal and demo environments
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

// Handler handles incoming WebSocket upgrade requests.
type Handler struct {
	hub    *Hub
	logger *zap.Logger
}

// NewHandler creates a new WebSocket route Handler.
func NewHandler(hub *Hub, logger *zap.Logger) *Handler {
	return &Handler{
		hub:    hub,
		logger: logger,
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

	conn, err := upgrader.Upgrade(w, r, nil)
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

