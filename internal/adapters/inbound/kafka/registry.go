package kafka

import (
	"fmt"
	"sync"

	"quicknotify/internal/adapters/inbound/kafka/translators"
	"quicknotify/internal/core/ports"

	"go.uber.org/zap"
)

// TranslatorRegistry maintains the mapping between translator identifier names and their implementations.
type TranslatorRegistry struct {
	translators map[string]ports.MessageTranslator
	mu          sync.RWMutex
}

// NewTranslatorRegistry initializes a new TranslatorRegistry with standard built-in translators.
func NewTranslatorRegistry(orderCache ports.OrderCache, logger *zap.Logger) *TranslatorRegistry {
	r := &TranslatorRegistry{
		translators: make(map[string]ports.MessageTranslator),
	}

	// Register standard built-in translators
	r.Register("direct", translators.NewDirectTranslator())
	r.Register("quickbite_order", translators.NewQuickbiteOrderTranslator(orderCache, logger))
	r.Register("edutrack_exam", translators.NewEduTrackExamTranslator())

	return r
}

// Register stores a translator implementation under a unique name key.
func (r *TranslatorRegistry) Register(name string, t ports.MessageTranslator) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.translators[name] = t
}

// Get retrieves a translator by name. Returns an error if not found.
func (r *TranslatorRegistry) Get(name string) (ports.MessageTranslator, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	t, ok := r.translators[name]
	if !ok {
		return nil, fmt.Errorf("translator '%s' is not registered", name)
	}
	return t, nil
}
