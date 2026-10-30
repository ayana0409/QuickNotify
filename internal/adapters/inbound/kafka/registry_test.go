package kafka

import (
	"testing"

	"quicknotify/internal/core/domain"
)

type mockCustomTranslator struct{}

func (m *mockCustomTranslator) Translate(key []byte, value []byte) (domain.NotificationEnvelope, error) {
	return domain.NotificationEnvelope{EventID: "custom_mock"}, nil
}

func TestTranslatorRegistry(t *testing.T) {
	registry := NewTranslatorRegistry(nil, nil)

	// Verify standard built-in translators exist
	builtins := []string{"direct", "quickbite_order", "edutrack_exam"}
	for _, name := range builtins {
		tr, err := registry.Get(name)
		if err != nil {
			t.Errorf("expected builtin translator '%s' to be registered, got error: %v", name, err)
		}
		if tr == nil {
			t.Errorf("expected translator '%s' not to be nil", name)
		}
	}

	// Test registering a new custom translator
	registry.Register("custom_test", &mockCustomTranslator{})
	tr, err := registry.Get("custom_test")
	if err != nil {
		t.Fatalf("unexpected error fetching registered custom translator: %v", err)
	}

	env, _ := tr.Translate(nil, nil)
	if env.EventID != "custom_mock" {
		t.Errorf("expected custom translator to return 'custom_mock', got '%s'", env.EventID)
	}

	// Test getting non-existent translator
	_, err = registry.Get("non_existent")
	if err == nil {
		t.Errorf("expected error when fetching unregistered translator")
	}
}

