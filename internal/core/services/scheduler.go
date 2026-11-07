package services

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"quicknotify/internal/core/domain"
	"quicknotify/internal/core/ports"

	"github.com/robfig/cron/v3"
	"go.uber.org/zap"
)

// SchedulerService orchestrates recurring and one-off notification schedules,
// manages their next-execution calculations, and dispatches multi-action batches.
type SchedulerService struct {
	store      ports.ScheduleStore
	dispatcher ports.NotificationService
	logger     *zap.Logger
	cronParser cron.Parser
	ticker     time.Duration
	stopCh     chan struct{}
	wg         sync.WaitGroup
	running    bool
	mu         sync.Mutex
}

// NewSchedulerService constructs a new SchedulerService.
func NewSchedulerService(
	store ports.ScheduleStore,
	dispatcher ports.NotificationService,
	logger *zap.Logger,
) *SchedulerService {
	// Standard 5-field cron parser with descriptor support (@daily, @hourly, etc.)
	parser := cron.NewParser(
		cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor,
	)

	return &SchedulerService{
		store:      store,
		dispatcher: dispatcher,
		logger:     logger,
		cronParser: parser,
		ticker:     1 * time.Second,
		stopCh:     make(chan struct{}),
	}
}

// ComputeNextRun calculates the next run timestamp for a schedule based on its type and expression.
func (s *SchedulerService) ComputeNextRun(sched *domain.Schedule, from time.Time) (*time.Time, error) {
	if sched == nil {
		return nil, fmt.Errorf("nil schedule")
	}

	switch sched.Type {
	case domain.ScheduleTypeCron:
		loc := time.UTC
		if sched.Timezone != "" {
			if l, err := time.LoadLocation(sched.Timezone); err == nil {
				loc = l
			} else if s.logger != nil {
				s.logger.Warn("Failed to load timezone location, falling back to UTC",
					zap.String("timezone", sched.Timezone),
					zap.Error(err),
				)
			}
		}

		cronSchedule, err := s.cronParser.Parse(sched.Expression)
		if err != nil {
			return nil, fmt.Errorf("invalid cron expression '%s': %w", sched.Expression, err)
		}

		next := cronSchedule.Next(from.In(loc)).UTC()
		return &next, nil

	case domain.ScheduleTypeInterval:
		d, err := time.ParseDuration(strings.TrimSpace(sched.Expression))
		if err != nil {
			return nil, fmt.Errorf("invalid interval duration '%s': %w", sched.Expression, err)
		}
		if d <= 0 {
			return nil, fmt.Errorf("interval duration must be greater than zero, got %s", d)
		}
		next := from.Add(d).UTC()
		return &next, nil

	case domain.ScheduleTypeOnce:
		// Attempt standard RFC3339 first
		parsed, err := time.Parse(time.RFC3339, strings.TrimSpace(sched.Expression))
		if err != nil {
			// Fallback: local datetime formats e.g. "2006-01-02 15:04:05" or "2006-01-02T15:04"
			formats := []string{
				"2006-01-02 15:04:05",
				"2006-01-02 15:04",
				"2006-01-02T15:04:05",
				"2006-01-02T15:04",
			}
			var matched bool
			for _, fmtStr := range formats {
				if t, parseErr := time.ParseInLocation(fmtStr, strings.TrimSpace(sched.Expression), time.Local); parseErr == nil {
					parsed = t.UTC()
					matched = true
					break
				}
			}
			if !matched {
				return nil, fmt.Errorf("invalid once timestamp '%s': must be RFC3339 or 'YYYY-MM-DD HH:MM:SS'", sched.Expression)
			}
		} else {
			parsed = parsed.UTC()
		}

		if parsed.Before(from) {
			// Past timestamp
			return nil, nil
		}
		return &parsed, nil

	default:
		return nil, fmt.Errorf("unsupported schedule type: %s", sched.Type)
	}
}

// Start begins the background tick loop that checks and executes due schedules.
func (s *SchedulerService) Start(ctx context.Context) error {
	s.mu.Lock()
	if s.running {
		s.mu.Unlock()
		return nil
	}
	s.running = true
	s.stopCh = make(chan struct{})
	s.mu.Unlock()

	// Initial pass: ensure all enabled schedules have valid NextRunAt calculated
	s.initScheduleTimers(ctx)

	s.wg.Add(1)
	go s.runLoop(ctx)

	if s.logger != nil {
		s.logger.Info("Scheduler Worker Engine started")
	}

	return nil
}

// Stop gracefully halts the scheduler loop and waits for in-flight jobs to finish.
func (s *SchedulerService) Stop() {
	s.mu.Lock()
	if !s.running {
		s.mu.Unlock()
		return
	}
	s.running = false
	close(s.stopCh)
	s.mu.Unlock()

	s.wg.Wait()

	if s.logger != nil {
		s.logger.Info("Scheduler Worker Engine stopped")
	}
}

// initScheduleTimers computes initial NextRunAt for any enabled schedule missing it.
func (s *SchedulerService) initScheduleTimers(ctx context.Context) {
	schedules, err := s.store.List(ctx, "")
	if err != nil {
		if s.logger != nil {
			s.logger.Error("Failed to list schedules on startup", zap.Error(err))
		}
		return
	}

	now := time.Now().UTC()
	for i := range schedules {
		sched := schedules[i]
		if !sched.Enabled {
			continue
		}
		if sched.NextRunAt == nil || sched.NextRunAt.Before(now) {
			next, err := s.ComputeNextRun(&sched, now)
			if err != nil {
				if s.logger != nil {
					s.logger.Warn("Failed to compute next run on startup",
						zap.String("schedule_id", sched.ID),
						zap.Error(err),
					)
				}
				continue
			}
			sched.NextRunAt = next
			if next == nil && sched.Type == domain.ScheduleTypeOnce {
				sched.Enabled = false
			}
			_ = s.store.Save(ctx, &sched)
		}
	}
}

// runLoop executes every tick duration (1 second), inspecting all schedules.
func (s *SchedulerService) runLoop(ctx context.Context) {
	defer s.wg.Done()
	ticker := time.NewTicker(s.ticker)
	defer ticker.Stop()

	for {
		select {
		case <-s.stopCh:
			return
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.checkDueSchedules(ctx)
		}
	}
}

// checkDueSchedules scans persistent storage for schedules that are due for execution.
func (s *SchedulerService) checkDueSchedules(ctx context.Context) {
	schedules, err := s.store.List(ctx, "")
	if err != nil {
		return
	}

	now := time.Now().UTC()

	for i := range schedules {
		sched := schedules[i]
		if !sched.Enabled {
			continue
		}

		if sched.NextRunAt == nil {
			// Schedule needs a computed next run time
			next, err := s.ComputeNextRun(&sched, now)
			if err == nil {
				sched.NextRunAt = next
				_ = s.store.Save(ctx, &sched)
			}
			continue
		}

		if now.After(*sched.NextRunAt) || now.Equal(*sched.NextRunAt) {
			// Trigger schedule execution
			s.triggerDueSchedule(ctx, &sched, now)
		}
	}
}

// triggerDueSchedule updates schedule state, computes next execution, and launches action runner.
func (s *SchedulerService) triggerDueSchedule(ctx context.Context, sched *domain.Schedule, triggeredAt time.Time) {
	sched.LastRunAt = &triggeredAt
	sched.RunCount++

	if sched.Type == domain.ScheduleTypeOnce || (sched.MaxRuns > 0 && sched.RunCount >= sched.MaxRuns) {
		sched.Enabled = false
		sched.NextRunAt = nil
	} else {
		next, err := s.ComputeNextRun(sched, triggeredAt)
		if err != nil {
			if s.logger != nil {
				s.logger.Error("Failed to calculate next schedule run",
					zap.String("schedule_id", sched.ID),
					zap.Error(err),
				)
			}
			sched.Enabled = false
			sched.NextRunAt = nil
		} else {
			sched.NextRunAt = next
		}
	}

	// Persist the updated state
	_ = s.store.Save(ctx, sched)

	if s.logger != nil {
		s.logger.Info("Triggering due scheduled job",
			zap.String("schedule_id", sched.ID),
			zap.String("schedule_name", sched.Name),
			zap.Int("run_count", sched.RunCount),
			zap.Int("actions_count", len(sched.Actions)),
		)
	}

	// Execute actions asynchronously in background
	s.wg.Add(1)
	go func(targetSched domain.Schedule) {
		defer s.wg.Done()
		_ = s.ExecuteSchedule(ctx, &targetSched)
	}(*sched)
}

// TriggerNow allows manually executing a schedule immediately (e.g. for testing / "Run Now" in admin).
func (s *SchedulerService) TriggerNow(ctx context.Context, id string) error {
	sched, err := s.store.Get(ctx, id)
	if err != nil {
		return err
	}

	now := time.Now().UTC()
	sched.LastRunAt = &now
	sched.RunCount++

	// Recalculate next run if it's interval
	if sched.Type == domain.ScheduleTypeInterval {
		if next, err := s.ComputeNextRun(sched, now); err == nil {
			sched.NextRunAt = next
		}
	}

	_ = s.store.Save(ctx, sched)

	if s.logger != nil {
		s.logger.Info("Manually triggering scheduled job",
			zap.String("schedule_id", sched.ID),
			zap.String("schedule_name", sched.Name),
		)
	}

	// Execute actions synchronously for manual trigger or in background
	return s.ExecuteSchedule(ctx, sched)
}

// ExecuteSchedule processes all actions contained within a schedule according to its ExecutionMode.
func (s *SchedulerService) ExecuteSchedule(ctx context.Context, sched *domain.Schedule) error {
	if sched == nil || len(sched.Actions) == 0 {
		return nil
	}

	if sched.ExecutionMode == domain.ExecutionParallel {
		var wg sync.WaitGroup
		for _, act := range sched.Actions {
			wg.Add(1)
			go func(action domain.NotificationAction) {
				defer wg.Done()
				s.dispatchAction(ctx, sched, action)
			}(act)
		}
		wg.Wait()
		return nil
	}

	// Sequential execution (default)
	for _, act := range sched.Actions {
		if act.DelayBeforeSec > 0 {
			select {
			case <-time.After(time.Duration(act.DelayBeforeSec) * time.Second):
			case <-ctx.Done():
				return ctx.Err()
			case <-s.stopCh:
				return nil
			}
		}

		s.dispatchAction(ctx, sched, act)
	}

	return nil
}

// dispatchAction handles rendering dynamic placeholders and dispatching an individual action.
func (s *SchedulerService) dispatchAction(ctx context.Context, sched *domain.Schedule, act domain.NotificationAction) {
	renderedContent := s.renderContentPlaceholders(sched, act.Content)

	channels := act.Channels
	if len(channels) == 0 {
		channels = []domain.Channel{domain.ChannelWebSocket}
	}

	priority := act.Priority
	if priority == "" {
		priority = "normal"
	}

	switch act.Scope {
	case domain.ScopeUser:
		// If targets are specified, deliver to each individual recipient
		if len(act.Targets) > 0 {
			for _, target := range act.Targets {
				env := domain.NotificationEnvelope{
					EventID:   fmt.Sprintf("evt_sched_%s_%d", sched.ID, time.Now().UnixNano()),
					ProjectID: act.ProjectID,
					Scope:     domain.ScopeUser,
					Priority:  priority,
					Channels:  channels,
					Target:    target,
					Content:   renderedContent,
				}
				if err := s.dispatcher.ProcessEvent(ctx, env); err != nil && s.logger != nil {
					s.logger.Warn("Failed to dispatch scheduled user notification",
						zap.String("schedule_id", sched.ID),
						zap.String("user_id", target.UserID),
						zap.Error(err),
					)
				}
			}
		}

	case domain.ScopeProjectGlobal, domain.ScopeSystemGlobal:
		env := domain.NotificationEnvelope{
			EventID:   fmt.Sprintf("evt_sched_%s_%d", sched.ID, time.Now().UnixNano()),
			ProjectID: act.ProjectID,
			Scope:     act.Scope,
			Priority:  priority,
			Channels:  channels,
			Content:   renderedContent,
		}
		if err := s.dispatcher.ProcessEvent(ctx, env); err != nil && s.logger != nil {
			s.logger.Warn("Failed to dispatch scheduled broadcast notification",
				zap.String("schedule_id", sched.ID),
				zap.String("scope", string(act.Scope)),
				zap.Error(err),
			)
		}
	}
}

// renderContentPlaceholders substitutes dynamic variables in notification message and title.
func (s *SchedulerService) renderContentPlaceholders(sched *domain.Schedule, content domain.ContentInfo) domain.ContentInfo {
	if content.Data == nil {
		return content
	}

	copiedData := make(map[string]interface{}, len(content.Data))
	nowStr := time.Now().Format("15:04:05 02/01/2006")

	for k, v := range content.Data {
		if strVal, ok := v.(string); ok {
			strVal = strings.ReplaceAll(strVal, "{{schedule_id}}", sched.ID)
			strVal = strings.ReplaceAll(strVal, "{{schedule_name}}", sched.Name)
			strVal = strings.ReplaceAll(strVal, "{{run_count}}", fmt.Sprintf("%d", sched.RunCount))
			strVal = strings.ReplaceAll(strVal, "{{triggered_at}}", nowStr)
			copiedData[k] = strVal
		} else {
			copiedData[k] = v
		}
	}

	return domain.ContentInfo{
		Type:       content.Type,
		TemplateID: content.TemplateID,
		Data:       copiedData,
	}
}

