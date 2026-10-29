package websocket

import (
	"testing"

	"go.uber.org/zap"
)

func TestHub_RegisterAndUnregister(t *testing.T) {
	logger := zap.NewNop()
	hub := NewHub(logger)

	client1 := &Client{
		hub:       hub,
		send:      make(chan []byte, 10),
		projectID: "quickbite",
		userID:    "user_1",
		logger:    logger,
	}

	client2 := &Client{
		hub:       hub,
		send:      make(chan []byte, 10),
		projectID: "quickbite",
		userID:    "user_1", // Same user, different session
		logger:    logger,
	}

	hub.Register(client1)
	hub.Register(client2)

	if total := hub.TotalConnections(); total != 2 {
		t.Fatalf("expected 2 connections, got %d", total)
	}

	// Test PushToClient (both sessions should receive)
	msg := []byte(`{"test":"hello"}`)
	if err := hub.PushToClient("quickbite", "user_1", msg); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(client1.send) != 1 || len(client2.send) != 1 {
		t.Fatalf("both sessions should have received the message")
	}

	// Unregister client1
	hub.Unregister(client1)
	if total := hub.TotalConnections(); total != 1 {
		t.Fatalf("expected 1 connection left, got %d", total)
	}

	// Unregister client2
	hub.Unregister(client2)
	if total := hub.TotalConnections(); total != 0 {
		t.Fatalf("expected 0 connections left, got %d", total)
	}
}

