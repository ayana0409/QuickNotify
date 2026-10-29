package websocket

import (
	"sync"

	"go.uber.org/zap"
)

// Hub maintains the set of active clients across multi-tenant projects
// and coordinates broadcasting messages with non-blocking guarantees.
type Hub struct {
	// clients: map[ProjectID]map[UserID]map[*Client]bool
	// Tier 1: ProjectID (tenant isolation)
	// Tier 2: UserID (target routing)
	// Tier 3: Set of Client pointers (multi-session / multiple tabs support)
	clients map[string]map[string]map[*Client]bool
	mu      sync.RWMutex
	logger  *zap.Logger
}

// NewHub initializes and returns a new Hub instance.
func NewHub(logger *zap.Logger) *Hub {
	return &Hub{
		clients: make(map[string]map[string]map[*Client]bool),
		logger:  logger,
	}
}

// Register adds a client connection to the hub under its respective project and user.
func (h *Hub) Register(client *Client) {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.clients[client.projectID] == nil {
		h.clients[client.projectID] = make(map[string]map[*Client]bool)
	}
	if h.clients[client.projectID][client.userID] == nil {
		h.clients[client.projectID][client.userID] = make(map[*Client]bool)
	}

	h.clients[client.projectID][client.userID][client] = true

	h.logger.Debug("Registered new WebSocket client",
		zap.String("project_id", client.projectID),
		zap.String("user_id", client.userID),
	)
}

// Unregister removes a client connection from the hub and safely releases resources.
func (h *Hub) Unregister(client *Client) {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.removeClientLocked(client)
}

// removeClientLocked cleans up client references within an active lock.
func (h *Hub) removeClientLocked(client *Client) {
	projectUsers, projectExists := h.clients[client.projectID]
	if !projectExists {
		return
	}

	userSessions, userExists := projectUsers[client.userID]
	if !userExists {
		return
	}

	if _, isActive := userSessions[client]; isActive {
		delete(userSessions, client)
		close(client.send)

		h.logger.Debug("Unregistered WebSocket client",
			zap.String("project_id", client.projectID),
			zap.String("user_id", client.userID),
		)
	}

	// Clean up empty maps to prevent memory leaks
	if len(userSessions) == 0 {
		delete(projectUsers, client.userID)
	}
	if len(projectUsers) == 0 {
		delete(h.clients, client.projectID)
	}
}

// pushToClientSafe attempts to deliver the payload into the client's send buffer.
// If the buffer is full (slow consumer), it drops the client asynchronously to prevent deadlocks.
func (h *Hub) pushToClientSafe(client *Client, payload []byte) {
	select {
	case client.send <- payload:
		// Message successfully queued in client buffer
	default:
		// Client buffer is saturated; evict client asynchronously to avoid lock inversion
		h.logger.Warn("Client buffer full, dropping slow connection",
			zap.String("project_id", client.projectID),
			zap.String("user_id", client.userID),
		)
		go h.Unregister(client)
	}
}

// PushToClient pushes a message payload to all active sessions of a specific user.
// Implements ports.WSPort.
func (h *Hub) PushToClient(projectID, userID string, payload []byte) error {
	h.mu.RLock()
	defer h.mu.RUnlock()

	projectUsers, ok := h.clients[projectID]
	if !ok {
		return nil
	}

	userSessions, ok := projectUsers[userID]
	if !ok {
		return nil
	}

	for client := range userSessions {
		h.pushToClientSafe(client, payload)
	}
	return nil
}

// PushToProject broadcasts a message payload to all online clients belonging to a project.
// Implements ports.WSPort.
func (h *Hub) PushToProject(projectID string, payload []byte) error {
	h.mu.RLock()
	defer h.mu.RUnlock()

	projectUsers, ok := h.clients[projectID]
	if !ok {
		return nil
	}

	for _, userSessions := range projectUsers {
		for client := range userSessions {
			h.pushToClientSafe(client, payload)
		}
	}
	return nil
}

// PushToAll broadcasts a message payload to every active client across all projects.
// Implements ports.WSPort.
func (h *Hub) PushToAll(payload []byte) error {
	h.mu.RLock()
	defer h.mu.RUnlock()

	for _, projectUsers := range h.clients {
		for _, userSessions := range projectUsers {
			for client := range userSessions {
				h.pushToClientSafe(client, payload)
			}
		}
	}
	return nil
}

// CloseAll closes all client connections gracefully.
func (h *Hub) CloseAll() {
	h.mu.Lock()
	defer h.mu.Unlock()

	for _, projectUsers := range h.clients {
		for _, userSessions := range projectUsers {
			for client := range userSessions {
				close(client.send)
				_ = client.conn.Close()
			}
		}
	}
	h.clients = make(map[string]map[string]map[*Client]bool)
	h.logger.Info("All WebSocket connections have been closed.")
}

// TotalConnections returns the total number of connected WebSocket clients.
func (h *Hub) TotalConnections() int {
	h.mu.RLock()
	defer h.mu.RUnlock()

	count := 0
	for _, projectUsers := range h.clients {
		for _, userSessions := range projectUsers {
			count += len(userSessions)
		}
	}
	return count
}

