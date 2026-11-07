package domain

import (
	"time"
)

// ScheduleType defines the temporal triggering mechanism for a scheduled job.
type ScheduleType string

const (
	// ScheduleTypeCron runs on standard 5-field cron intervals (e.g. "0 8 * * *").
	ScheduleTypeCron ScheduleType = "cron"

	// ScheduleTypeInterval runs repeatedly after fixed durations (e.g. "15m", "1h", "30s").
	ScheduleTypeInterval ScheduleType = "interval"

	// ScheduleTypeOnce executes a single time at a specific timestamp.
	ScheduleTypeOnce ScheduleType = "once"
)

// ExecutionMode determines how multiple actions within a single schedule are triggered.
type ExecutionMode string

const (
	// ExecutionSequential executes actions one by one in order, respecting DelayBeforeSec.
	ExecutionSequential ExecutionMode = "sequential"

	// ExecutionParallel executes all actions simultaneously in separate goroutines.
	ExecutionParallel ExecutionMode = "parallel"
)

// NotificationAction represents an individual notification dispatch step inside a schedule.
type NotificationAction struct {
	ActionID       string       `json:"action_id"`
	Name           string       `json:"name"`
	DelayBeforeSec int          `json:"delay_before_sec,omitempty"` // Seconds to wait before dispatching (sequential mode)
	ProjectID      string       `json:"project_id"`                 // Target project / tenant ID (e.g. "shorterlink")
	Scope          Scope        `json:"scope"`                      // "user", "project_global", "system_global"
	Priority       string       `json:"priority,omitempty"`         // "low", "normal", "high"
	Channels       []Channel    `json:"channels"`                   // Delivery channels, e.g. ["websocket", "email"]
	Targets        []TargetInfo `json:"targets,omitempty"`          // One or multiple target recipients
	Content        ContentInfo  `json:"content"`                    // Notification message content and metadata
}

// Schedule defines a recurring or one-off job containing one or more notification actions.
type Schedule struct {
	ID             string               `json:"id"`
	Name           string               `json:"name"`
	Description    string               `json:"description,omitempty"`
	OwnerProjectID string               `json:"owner_project_id"` // "system" for admin, or project ID (e.g. "shorterlink")
	Type           ScheduleType         `json:"type"`             // "cron", "interval", "once"
	Expression     string               `json:"expression"`       // Cron spec, duration ("15m"), or RFC3339 timestamp
	Timezone       string               `json:"timezone,omitempty"` // Default: "Asia/Ho_Chi_Minh" or "UTC"
	ExecutionMode  ExecutionMode        `json:"execution_mode"`   // "sequential" or "parallel"
	Actions        []NotificationAction `json:"actions"`          // Collection of notification actions to perform
	Enabled        bool                 `json:"enabled"`
	MaxRuns        int                  `json:"max_runs,omitempty"` // 0 = unlimited runs
	RunCount       int                  `json:"run_count"`
	LastRunAt      *time.Time           `json:"last_run_at,omitempty"`
	NextRunAt      *time.Time           `json:"next_run_at,omitempty"`
	CreatedAt      time.Time            `json:"created_at"`
	UpdatedAt      time.Time            `json:"updated_at"`
}

