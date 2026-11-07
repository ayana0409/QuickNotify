package services

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"quicknotify/internal/core/domain"

	"go.uber.org/zap"
)

type mockScheduleStore struct {
	mu        sync.RWMutex
	schedules map[string]*domain.Schedule
}

func newMockScheduleStore() *mockScheduleStore {
	return &mockScheduleStore{schedules: make(map[string]*domain.Schedule)}
}

func (m *mockScheduleStore) List(ctx context.Context, ownerProjectID string) ([]domain.Schedule, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var res []domain.Schedule
	for _, s := range m.schedules {
		if ownerProjectID == "" || ownerProjectID == "system" || ownerProjectID == "*" || s.OwnerProjectID == ownerProjectID {
			res = append(res, *s)
		}
	}
	return res, nil
}

func (m *mockScheduleStore) Get(ctx context.Context, id string) (*domain.Schedule, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	s, ok := m.schedules[id]
	if !ok {
		return nil, errors.New("schedule not found")
	}
	cpy := *s
	return &cpy, nil
}

func (m *mockScheduleStore) Save(ctx context.Context, schedule *domain.Schedule) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	cpy := *schedule
	m.schedules[schedule.ID] = &cpy
	return nil
}

func (m *mockScheduleStore) Delete(ctx context.Context, id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.schedules, id)
	return nil
}

type mockNotificationDispatcher struct {
	mu        sync.Mutex
	dispatched []domain.NotificationEnvelope
}

func (d *mockNotificationDispatcher) ProcessEvent(ctx context.Context, env domain.NotificationEnvelope) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.dispatched = append(d.dispatched, env)
	return nil
}

func TestSchedulerService_ComputeNextRun(t *testing.T) {
	logger := zap.NewNop()
	svc := NewSchedulerService(newMockScheduleStore(), &mockNotificationDispatcher{}, logger)

	baseTime := time.Date(2026, 11, 7, 7, 0, 0, 0, time.UTC)

	// 1. Cron: every day at 08:00
	schedCron := &domain.Schedule{
		Type:       domain.ScheduleTypeCron,
		Expression: "0 8 * * *",
		Timezone:   "UTC",
	}
	nextCron, err := svc.ComputeNextRun(schedCron, baseTime)
	if err != nil {
		t.Fatalf("ComputeNextRun cron error: %v", err)
	}
	expectedCron := time.Date(2026, 11, 7, 8, 0, 0, 0, time.UTC)
	if !nextCron.Equal(expectedCron) {
		t.Fatalf("expected next cron %v, got %v", expectedCron, *nextCron)
	}

	// 2. Interval: 15 minutes
	schedInterval := &domain.Schedule{
		Type:       domain.ScheduleTypeInterval,
		Expression: "15m",
	}
	nextInterval, err := svc.ComputeNextRun(schedInterval, baseTime)
	if err != nil {
		t.Fatalf("ComputeNextRun interval error: %v", err)
	}
	expectedInterval := baseTime.Add(15 * time.Minute)
	if !nextInterval.Equal(expectedInterval) {
		t.Fatalf("expected next interval %v, got %v", expectedInterval, *nextInterval)
	}

	// 3. Once: future timestamp
	futureTime := baseTime.Add(2 * time.Hour)
	schedOnce := &domain.Schedule{
		Type:       domain.ScheduleTypeOnce,
		Expression: futureTime.Format(time.RFC3339),
	}
	nextOnce, err := svc.ComputeNextRun(schedOnce, baseTime)
	if err != nil {
		t.Fatalf("ComputeNextRun once error: %v", err)
	}
	if !nextOnce.Equal(futureTime) {
		t.Fatalf("expected next once %v, got %v", futureTime, *nextOnce)
	}

	// 4. Once: past timestamp
	pastTime := baseTime.Add(-1 * time.Hour)
	schedOncePast := &domain.Schedule{
		Type:       domain.ScheduleTypeOnce,
		Expression: pastTime.Format(time.RFC3339),
	}
	nextOncePast, err := svc.ComputeNextRun(schedOncePast, baseTime)
	if err != nil {
		t.Fatalf("ComputeNextRun once past error: %v", err)
	}
	if nextOncePast != nil {
		t.Fatalf("expected nil for past once schedule, got %v", *nextOncePast)
	}
}

func TestSchedulerService_ExecuteSchedule_MultiAction(t *testing.T) {
	logger := zap.NewNop()
	store := newMockScheduleStore()
	dispatcher := &mockNotificationDispatcher{}
	svc := NewSchedulerService(store, dispatcher, logger)

	ctx := context.Background()
	sched := &domain.Schedule{
		ID:             "sched_test_multi",
		Name:           "Multi-Action Test",
		OwnerProjectID: "shorterlink",
		ExecutionMode:  domain.ExecutionParallel,
		RunCount:       1,
		Actions: []domain.NotificationAction{
			{
				ActionID:  "act_1",
				ProjectID: "shorterlink",
				Scope:     domain.ScopeProjectGlobal,
				Content: domain.ContentInfo{
					Type: "raw",
					Data: map[string]interface{}{
						"message": "Hello from {{schedule_name}} run #{{run_count}}",
					},
				},
			},
			{
				ActionID:  "act_2",
				ProjectID: "shorterlink",
				Scope:     domain.ScopeUser,
				Targets: []domain.TargetInfo{
					{UserID: "usr_alice"},
					{UserID: "usr_bob"},
				},
				Content: domain.ContentInfo{
					Type: "raw",
					Data: map[string]interface{}{
						"message": "Direct message to VIP",
					},
				},
			},
		},
	}

	if err := svc.ExecuteSchedule(ctx, sched); err != nil {
		t.Fatalf("ExecuteSchedule failed: %v", err)
	}

	dispatcher.mu.Lock()
	count := len(dispatcher.dispatched)
	dispatcher.mu.Unlock()

	// 1 project_global + 2 users = 3 dispatched notifications
	if count != 3 {
		t.Fatalf("expected 3 dispatched envelopes, got %d", count)
	}

	// Verify placeholder was substituted in the broadcast action
	dispatcher.mu.Lock()
	var foundMsg string
	for _, env := range dispatcher.dispatched {
		if msg, ok := env.Content.Data["message"].(string); ok && strings.HasPrefix(msg, "Hello from") {
			foundMsg = msg
			break
		}
	}
	dispatcher.mu.Unlock()

	if foundMsg != "Hello from Multi-Action Test run #1" {
		t.Fatalf("expected placeholder substituted, got '%s'", foundMsg)
	}
}

func TestSchedulerService_TriggerNow(t *testing.T) {
	logger := zap.NewNop()
	store := newMockScheduleStore()
	dispatcher := &mockNotificationDispatcher{}
	svc := NewSchedulerService(store, dispatcher, logger)

	ctx := context.Background()
	sched := &domain.Schedule{
		ID:             "sched_manual_1",
		Name:           "Manual Trigger Test",
		OwnerProjectID: "system",
		Type:           domain.ScheduleTypeInterval,
		Expression:     "1h",
		Enabled:        true,
		Actions: []domain.NotificationAction{
			{
				ActionID:  "act_sys",
				ProjectID: "system",
				Scope:     domain.ScopeSystemGlobal,
				Content: domain.ContentInfo{
					Type: "raw",
					Data: map[string]interface{}{"message": "System ping"},
				},
			},
		},
	}
	_ = store.Save(ctx, sched)

	if err := svc.TriggerNow(ctx, sched.ID); err != nil {
		t.Fatalf("TriggerNow failed: %v", err)
	}

	updated, _ := store.Get(ctx, sched.ID)
	if updated.RunCount != 1 || updated.LastRunAt == nil {
		t.Fatalf("expected RunCount=1 and LastRunAt set, got %+v", updated)
	}

	dispatcher.mu.Lock()
	count := len(dispatcher.dispatched)
	dispatcher.mu.Unlock()
	if count != 1 {
		t.Fatalf("expected 1 notification dispatched, got %d", count)
	}
}
