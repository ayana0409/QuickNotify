package services

import (
	"context"
	"quicknotify/internal/core/domain"
	"testing"

	"go.uber.org/zap"
)

type mockWSPort struct {
	pushedClient  []string
	pushedProject []string
	pushedAll     bool
}

func (m *mockWSPort) PushToClient(projectID, userID string, payload []byte) error {
	m.pushedClient = append(m.pushedClient, projectID+":"+userID)
	return nil
}

func (m *mockWSPort) PushToProject(projectID string, payload []byte) error {
	m.pushedProject = append(m.pushedProject, projectID)
	return nil
}

func (m *mockWSPort) PushToAll(payload []byte) error {
	m.pushedAll = true
	return nil
}

type mockEmailPort struct {
	sentEmails []string
}

func (m *mockEmailPort) SendEmail(ctx context.Context, target domain.TargetInfo, content domain.ContentInfo) error {
	m.sentEmails = append(m.sentEmails, target.Email)
	return nil
}

func TestDispatcher_ProcessEvent(t *testing.T) {
	logger := zap.NewNop()
	wsMock := &mockWSPort{}
	emailMock := &mockEmailPort{}
	dispatcher := NewDispatcher(wsMock, emailMock, logger)
	ctx := context.Background()

	// 1. Test User Scope WebSocket & Email
	envUser := domain.NotificationEnvelope{
		EventID:   "evt_001",
		ProjectID: "quicktest",
		Scope:     domain.ScopeUser,
		Channels:  []domain.Channel{domain.ChannelWebSocket, domain.ChannelEmail},
		Target: domain.TargetInfo{
			UserID: "usr_123",
			Email:  "user@example.com",
		},
		Content: domain.ContentInfo{
			Type: "raw",
			Data: map[string]interface{}{"message": "hello"},
		},
	}

	if err := dispatcher.ProcessEvent(ctx, envUser); err != nil {
		t.Fatalf("failed to process user event: %v", err)
	}

	if len(wsMock.pushedClient) != 1 || wsMock.pushedClient[0] != "quicktest:usr_123" {
		t.Fatalf("expected ws push to client quicktest:usr_123, got: %v", wsMock.pushedClient)
	}
	if len(emailMock.sentEmails) != 1 || emailMock.sentEmails[0] != "user@example.com" {
		t.Fatalf("expected email to user@example.com, got: %v", emailMock.sentEmails)
	}

	// 2. Test Project Global Scope
	envProject := domain.NotificationEnvelope{
		EventID:   "evt_002",
		ProjectID: "quickbite",
		Scope:     domain.ScopeProjectGlobal,
		Channels:  []domain.Channel{domain.ChannelWebSocket},
		Content: domain.ContentInfo{
			Type: "raw",
			Data: map[string]interface{}{"message": "voucher"},
		},
	}

	if err := dispatcher.ProcessEvent(ctx, envProject); err != nil {
		t.Fatalf("failed to process project event: %v", err)
	}
	if len(wsMock.pushedProject) != 1 || wsMock.pushedProject[0] != "quickbite" {
		t.Fatalf("expected project broadcast to quickbite, got: %v", wsMock.pushedProject)
	}

	// 3. Test System Global Scope
	envSystem := domain.NotificationEnvelope{
		EventID:   "evt_003",
		ProjectID: "system",
		Scope:     domain.ScopeSystemGlobal,
		Channels:  []domain.Channel{domain.ChannelWebSocket},
		Content: domain.ContentInfo{
			Type: "raw",
			Data: map[string]interface{}{"message": "maintenance"},
		},
	}

	if err := dispatcher.ProcessEvent(ctx, envSystem); err != nil {
		t.Fatalf("failed to process system event: %v", err)
	}
	if !wsMock.pushedAll {
		t.Fatalf("expected system-wide broadcast")
	}

	// 4. Test Validation Failure (missing EventID)
	envInvalid := domain.NotificationEnvelope{
		ProjectID: "quicktest",
		Scope:     domain.ScopeUser,
		Channels:  []domain.Channel{domain.ChannelWebSocket},
	}
	if err := dispatcher.ProcessEvent(ctx, envInvalid); err == nil {
		t.Fatalf("expected validation error for missing required fields")
	}
}

