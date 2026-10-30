package services

import (
	"context"
	"encoding/json"
	"fmt"

	"quicknotify/internal/core/domain"
	"quicknotify/internal/core/ports"

	"github.com/go-playground/validator/v10"
	"go.uber.org/zap"
)

// OutgoingWSPayload defines the envelope delivered to WebSocket clients.
type OutgoingWSPayload struct {
	EventID   string             `json:"event_id"`
	ProjectID string             `json:"project_id"`
	Scope     domain.Scope       `json:"scope"`
	Priority  string             `json:"priority,omitempty"`
	Content   domain.ContentInfo `json:"content"`
}

// Dispatcher coordinates incoming notifications and delivers them to target channels based on scope.
type Dispatcher struct {
	wsAdapter    ports.WSPort
	emailAdapter ports.EmailPort
	validator    *validator.Validate
	logger       *zap.Logger
}

// NewDispatcher creates a new Dispatcher service instance.
func NewDispatcher(
	wsAdapter ports.WSPort,
	emailAdapter ports.EmailPort,
	logger *zap.Logger,
) *Dispatcher {
	return &Dispatcher{
		wsAdapter:    wsAdapter,
		emailAdapter: emailAdapter,
		validator:    validator.New(),
		logger:       logger,
	}
}

// ProcessEvent validates and dispatches a notification event to configured delivery channels.
func (d *Dispatcher) ProcessEvent(ctx context.Context, env domain.NotificationEnvelope) error {
	// 1. Validate envelope payload integrity
	if err := d.validator.Struct(env); err != nil {
		d.logger.Error("Invalid notification envelope",
			zap.String("event_id", env.EventID),
			zap.Error(err),
		)
		return fmt.Errorf("envelope validation failed: %w", err)
	}

	d.logger.Info("Processing notification event",
		zap.String("event_id", env.EventID),
		zap.String("project_id", env.ProjectID),
		zap.String("scope", string(env.Scope)),
	)

	// 2. Iterate through requested output channels
	for _, channel := range env.Channels {
		switch channel {
		case domain.ChannelWebSocket:
			d.dispatchWebSocket(env)

		case domain.ChannelEmail:
			d.dispatchEmail(ctx, env)

		default:
			d.logger.Warn("Unsupported channel skipped",
				zap.String("channel", string(channel)),
				zap.String("event_id", env.EventID),
			)
		}
	}

	return nil
}

// dispatchWebSocket handles serialization and delivery via the WebSocket port.
func (d *Dispatcher) dispatchWebSocket(env domain.NotificationEnvelope) {
	if d.wsAdapter == nil {
		d.logger.Warn("WebSocket adapter is not configured")
		return
	}

	payloadObj := OutgoingWSPayload{
		EventID:   env.EventID,
		ProjectID: env.ProjectID,
		Scope:     env.Scope,
		Priority:  env.Priority,
		Content:   env.Content,
	}

	payloadBytes, err := json.Marshal(payloadObj)
	if err != nil {
		d.logger.Error("Failed to serialize WebSocket outgoing payload",
			zap.String("event_id", env.EventID),
			zap.Error(err),
		)
		return
	}

	switch env.Scope {
	case domain.ScopeUser:
		if env.Target.UserID == "" {
			d.logger.Warn("Scope 'user' specified but target user_id is empty",
				zap.String("event_id", env.EventID),
			)
			return
		}
		if err := d.wsAdapter.PushToClient(env.ProjectID, env.Target.UserID, payloadBytes); err != nil {
			d.logger.Error("Failed to push to client", zap.Error(err))
		}

	case domain.ScopeProjectGlobal:
		if err := d.wsAdapter.PushToProject(env.ProjectID, payloadBytes); err != nil {
			d.logger.Error("Failed to push to project", zap.Error(err))
		}

	case domain.ScopeSystemGlobal:
		if err := d.wsAdapter.PushToAll(payloadBytes); err != nil {
			d.logger.Error("Failed to broadcast to all", zap.Error(err))
		}
	}
}

// dispatchEmail handles asynchronous delivery via the Email port.
func (d *Dispatcher) dispatchEmail(ctx context.Context, env domain.NotificationEnvelope) {
	if d.emailAdapter == nil {
		d.logger.Warn("Email adapter is not configured")
		return
	}

	if env.Target.Email == "" {
		d.logger.Warn("Email channel requested but target email is empty",
			zap.String("event_id", env.EventID),
		)
		return
	}

	if err := d.emailAdapter.SendEmail(ctx, env.Target, env.Content); err != nil {
		d.logger.Error("Failed to dispatch email",
			zap.String("event_id", env.EventID),
			zap.Error(err),
		)
	}
}

