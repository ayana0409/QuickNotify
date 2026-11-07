package storage

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"quicknotify/internal/core/domain"

	"go.uber.org/zap"
)

func TestJSONScheduleStore_CRUDAndHashing(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "quicknotify-sched-store-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	logger := zap.NewNop()
	store, err := NewJSONScheduleStore(tempDir, logger)
	if err != nil {
		t.Fatalf("NewJSONScheduleStore failed: %v", err)
	}

	ctx := context.Background()

	// 1. Create a schedule for ShorterLink
	sched1 := &domain.Schedule{
		ID:             "shorterlink_morning_report",
		Name:           "ShorterLink Morning Summary",
		Description:    "Daily click statistics summary",
		OwnerProjectID: "shorterlink",
		Type:           domain.ScheduleTypeCron,
		Expression:     "0 8 * * *",
		Timezone:       "Asia/Ho_Chi_Minh",
		ExecutionMode:  domain.ExecutionSequential,
		Enabled:        true,
		Actions: []domain.NotificationAction{
			{
				ActionID:       "act_1",
				Name:           "Broadcast Project Alert",
				DelayBeforeSec: 0,
				ProjectID:      "shorterlink",
				Scope:          domain.ScopeProjectGlobal,
				Channels:       []domain.Channel{domain.ChannelWebSocket},
				Content: domain.ContentInfo{
					Type: "raw",
					Data: map[string]interface{}{"message": "Daily report is ready"},
				},
			},
			{
				ActionID:       "act_2",
				Name:           "Notify VIP User",
				DelayBeforeSec: 5,
				ProjectID:      "shorterlink",
				Scope:          domain.ScopeUser,
				Channels:       []domain.Channel{domain.ChannelWebSocket},
				Targets: []domain.TargetInfo{
					{UserID: "usr_vip_99"},
				},
				Content: domain.ContentInfo{
					Type: "raw",
					Data: map[string]interface{}{"message": "VIP personal statistics"},
				},
			},
		},
	}

	// 2. Save Schedule 1
	if err := store.Save(ctx, sched1); err != nil {
		t.Fatalf("failed to save sched1: %v", err)
	}

	// Verify hashed filename exists on disk
	expectedHashedName := store.hashFilename(sched1.ID)
	if !strings.HasPrefix(expectedHashedName, "sched_") || !strings.HasSuffix(expectedHashedName, ".json") {
		t.Fatalf("unexpected hashed filename format: %s", expectedHashedName)
	}
	if _, err := os.Stat(filepath.Join(tempDir, expectedHashedName)); os.IsNotExist(err) {
		t.Fatalf("expected file %s does not exist on disk", expectedHashedName)
	}

	// 3. Get Schedule 1
	got1, err := store.Get(ctx, sched1.ID)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if got1.Name != sched1.Name || len(got1.Actions) != 2 {
		t.Fatalf("retrieved schedule mismatch: %+v", got1)
	}

	// 4. Create Schedule 2 for QuickBite
	sched2 := &domain.Schedule{
		ID:             "quickbite_lunch_remind",
		Name:           "QuickBite Lunch Reminder",
		OwnerProjectID: "quickbite",
		Type:           domain.ScheduleTypeInterval,
		Expression:     "30m",
		ExecutionMode:  domain.ExecutionParallel,
		Enabled:        false,
		Actions: []domain.NotificationAction{
			{
				ActionID:  "act_qb_1",
				ProjectID: "quickbite",
				Scope:     domain.ScopeProjectGlobal,
				Channels:  []domain.Channel{domain.ChannelWebSocket},
				Content: domain.ContentInfo{
					Type: "raw",
					Data: map[string]interface{}{"message": "Hungry? Order lunch now!"},
				},
			},
		},
	}
	if err := store.Save(ctx, sched2); err != nil {
		t.Fatalf("failed to save sched2: %v", err)
	}

	// 5. List all schedules (admin view)
	allSchedules, err := store.List(ctx, "")
	if err != nil {
		t.Fatalf("List all failed: %v", err)
	}
	if len(allSchedules) != 2 {
		t.Fatalf("expected 2 schedules, got %d", len(allSchedules))
	}

	// 6. List filtered by tenant "shorterlink"
	slSchedules, err := store.List(ctx, "shorterlink")
	if err != nil {
		t.Fatalf("List shorterlink failed: %v", err)
	}
	if len(slSchedules) != 1 || slSchedules[0].ID != sched1.ID {
		t.Fatalf("expected 1 shorterlink schedule, got %d", len(slSchedules))
	}

	// 7. Update Schedule 1 (e.g. toggle enabled)
	sched1.Enabled = false
	if err := store.Save(ctx, sched1); err != nil {
		t.Fatalf("failed to update sched1: %v", err)
	}
	updated, err := store.Get(ctx, sched1.ID)
	if err != nil || updated.Enabled != false {
		t.Fatalf("expected updated enabled to be false, got: %+v, err: %v", updated, err)
	}

	// Ensure updating didn't create duplicate files
	allAfterUpdate, _ := store.List(ctx, "")
	if len(allAfterUpdate) != 2 {
		t.Fatalf("expected still 2 schedules after update, got %d", len(allAfterUpdate))
	}

	// 8. Delete Schedule 2
	if err := store.Delete(ctx, sched2.ID); err != nil {
		t.Fatalf("failed to delete sched2: %v", err)
	}
	_, err = store.Get(ctx, sched2.ID)
	if err != ErrScheduleNotFound {
		t.Fatalf("expected ErrScheduleNotFound after delete, got %v", err)
	}

	allAfterDelete, _ := store.List(ctx, "")
	if len(allAfterDelete) != 1 {
		t.Fatalf("expected 1 schedule remaining after delete, got %d", len(allAfterDelete))
	}
}

func TestJSONScheduleStore_AutoCreateDir(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "quicknotify-autodir-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	nestedDir := filepath.Join(tempDir, "deep", "nested", "schedules")
	store, err := NewJSONScheduleStore(nestedDir, zap.NewNop())
	if err != nil {
		t.Fatalf("expected auto-creation of nested directory, got err: %v", err)
	}

	sched := &domain.Schedule{
		ID:             "auto_created_test",
		Name:           "Test",
		OwnerProjectID: "system",
		Type:           domain.ScheduleTypeOnce,
		Expression:     time.Now().Add(1 * time.Hour).Format(time.RFC3339),
		Enabled:        true,
	}

	if err := store.Save(context.Background(), sched); err != nil {
		t.Fatalf("Save in auto-created dir failed: %v", err)
	}

	got, err := store.Get(context.Background(), sched.ID)
	if err != nil || got.ID != sched.ID {
		t.Fatalf("Get in auto-created dir failed: %+v, %v", got, err)
	}
}

