package ports

import (
	"context"
	"quicknotify/internal/core/domain"
)

// ScheduleStore defines the outbound port for persisting and querying scheduled notification jobs.
type ScheduleStore interface {
	// List returns all schedules. If ownerProjectID is non-empty and not "system" / "*",
	// it filters the results to only include schedules owned by that project.
	List(ctx context.Context, ownerProjectID string) ([]domain.Schedule, error)

	// Get retrieves a single schedule by its ID.
	Get(ctx context.Context, id string) (*domain.Schedule, error)

	// Save writes or updates a schedule to persistent storage.
	Save(ctx context.Context, schedule *domain.Schedule) error

	// Delete removes a schedule by its ID.
	Delete(ctx context.Context, id string) error
}

