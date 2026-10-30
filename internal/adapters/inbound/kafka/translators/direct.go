package translators

import (
	"encoding/json"
	"fmt"

	"quicknotify/internal/core/domain"
	"quicknotify/internal/core/ports"
)

// DirectTranslator performs 1:1 JSON unmarshaling into domain.NotificationEnvelope.
// Used for standard topics like "global-notify-events".
type DirectTranslator struct{}

// NewDirectTranslator creates a new DirectTranslator instance.
func NewDirectTranslator() ports.MessageTranslator {
	return &DirectTranslator{}
}

// Translate unmarshals the raw JSON byte array directly into a NotificationEnvelope.
func (d *DirectTranslator) Translate(key []byte, value []byte) (domain.NotificationEnvelope, error) {
	var env domain.NotificationEnvelope
	if err := json.Unmarshal(value, &env); err != nil {
		return domain.NotificationEnvelope{}, fmt.Errorf("direct json unmarshal failed: %w", err)
	}
	return env, nil
}

