package domain

// Scope represents the broadcasting scope level of a notification.
type Scope string

const (
	// ScopeUser targets a specific individual user within a project.
	ScopeUser Scope = "user"

	// ScopeProjectGlobal broadcasts to all connected clients within a specific project.
	ScopeProjectGlobal Scope = "project_global"

	// ScopeSystemGlobal broadcasts across the entire platform to all connected clients in all projects.
	ScopeSystemGlobal Scope = "system_global"
)

// Channel represents an output delivery channel.
type Channel string

const (
	ChannelWebSocket Channel = "websocket"
	ChannelEmail     Channel = "email"
)

// TargetInfo defines the target recipient details for personalized notifications.
type TargetInfo struct {
	UserID string `json:"user_id,omitempty"`
	Email  string `json:"email,omitempty"`
}

// ContentInfo defines the actual notification content and payload structure.
type ContentInfo struct {
	Type       string                 `json:"type"`                  // "raw" or "template"
	TemplateID string                 `json:"template_id,omitempty"` // Identifier when type is "template"
	Data       map[string]interface{} `json:"data"`                  // Dynamic key-value payload
}

// NotificationEnvelope is the standardized message envelope across the QuickNotify core.
type NotificationEnvelope struct {
	EventID   string      `json:"event_id" validate:"required"`
	ProjectID string      `json:"project_id" validate:"required"`
	Scope     Scope       `json:"scope" validate:"required,oneof=user project_global system_global"`
	Priority  string      `json:"priority,omitempty"` // e.g., "low", "normal", "high"
	Channels  []Channel   `json:"channels" validate:"required,min=1"`
	Target    TargetInfo  `json:"target"`
	Content   ContentInfo `json:"content" validate:"required"`
}

