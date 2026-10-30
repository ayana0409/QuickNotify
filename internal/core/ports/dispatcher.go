package ports

import (
	"context"
	"quicknotify/internal/core/domain"
)

// NotificationService defines the primary inbound port for processing incoming notifications.
type NotificationService interface {
	// ProcessEvent orchestrates the routing and delivery of a notification envelope to designated channels.
	ProcessEvent(ctx context.Context, env domain.NotificationEnvelope) error
}

// WSPort defines the outbound port for WebSocket delivery across different scopes.
type WSPort interface {
	// PushToClient sends a payload to all active connections of a specific user within a project.
	PushToClient(projectID, userID string, payload []byte) error

	// PushToProject broadcasts a payload to all connected clients in a project.
	PushToProject(projectID string, payload []byte) error

	// PushToAll broadcasts a payload to all connected clients across all projects in the system.
	PushToAll(payload []byte) error
}

// EmailPort defines the outbound port for email delivery.
type EmailPort interface {
	// SendEmail delivers an email notification to the specified recipient.
	SendEmail(ctx context.Context, target domain.TargetInfo, content domain.ContentInfo) error
}

// MessageTranslator defines the port for converting raw provider-specific messages into standardized envelopes.
type MessageTranslator interface {
	// Translate converts a raw key and byte payload from Kafka into a domain NotificationEnvelope.
	Translate(key []byte, value []byte) (domain.NotificationEnvelope, error)
}
