package storage

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"quicknotify/internal/core/domain"
	"quicknotify/internal/core/ports"

	"go.uber.org/zap"
)

var (
	// ErrScheduleNotFound is returned when a requested schedule does not exist.
	ErrScheduleNotFound = errors.New("schedule not found")
)

// JSONScheduleStore implements ports.ScheduleStore by persisting each schedule
// as an individual JSON file with a hashed filename inside a dedicated directory.
type JSONScheduleStore struct {
	dir    string
	mu     sync.RWMutex
	logger *zap.Logger
}

// NewJSONScheduleStore creates and initialises a new JSONScheduleStore.
// If the target directory does not exist, it is automatically created.
func NewJSONScheduleStore(dir string, logger *zap.Logger) (*JSONScheduleStore, error) {
	if strings.TrimSpace(dir) == "" {
		dir = filepath.Join("data", "schedules")
	}

	// Ensure the storage directory exists
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create schedules directory at %s: %w", dir, err)
	}

	store := &JSONScheduleStore{
		dir:    dir,
		logger: logger,
	}

	if logger != nil {
		logger.Info("JSON Schedule Store initialised", zap.String("storage_dir", dir))
	}

	return store, nil
}

// Ensure JSONScheduleStore implements ports.ScheduleStore interface.
var _ ports.ScheduleStore = (*JSONScheduleStore)(nil)

// hashFilename generates a deterministic, safe, collision-resistant filename for a schedule ID.
// Format: sched_<sha256[:16]>.json
func (s *JSONScheduleStore) hashFilename(id string) string {
	cleaned := strings.ToLower(strings.TrimSpace(id))
	hash := sha256.Sum256([]byte(cleaned))
	hexStr := hex.EncodeToString(hash[:])
	return fmt.Sprintf("sched_%s.json", hexStr[:16])
}

// filePath returns the absolute or relative file path for a schedule ID.
func (s *JSONScheduleStore) filePath(id string) string {
	return filepath.Join(s.dir, s.hashFilename(id))
}

// List retrieves all schedules, optionally filtered by owner project ID.
func (s *JSONScheduleStore) List(ctx context.Context, ownerProjectID string) ([]domain.Schedule, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	entries, err := os.ReadDir(s.dir)
	if err != nil {
		if os.IsNotExist(err) {
			return []domain.Schedule{}, nil
		}
		return nil, fmt.Errorf("read schedules directory: %w", err)
	}

	filterByOwner := ownerProjectID != "" && ownerProjectID != "system" && ownerProjectID != "*"
	var schedules []domain.Schedule

	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}

		fullPath := filepath.Join(s.dir, entry.Name())
		data, err := os.ReadFile(fullPath)
		if err != nil {
			if s.logger != nil {
				s.logger.Warn("Failed to read schedule file", zap.String("file", fullPath), zap.Error(err))
			}
			continue
		}

		var sched domain.Schedule
		if err := json.Unmarshal(data, &sched); err != nil {
			if s.logger != nil {
				s.logger.Warn("Failed to unmarshal schedule file", zap.String("file", fullPath), zap.Error(err))
			}
			continue
		}

		if filterByOwner && !strings.EqualFold(sched.OwnerProjectID, ownerProjectID) {
			continue
		}

		schedules = append(schedules, sched)
	}

	// Sort schedules: newest first
	sort.Slice(schedules, func(i, j int) bool {
		return schedules[i].CreatedAt.After(schedules[j].CreatedAt)
	})

	return schedules, nil
}

// Get loads a single schedule by its ID.
func (s *JSONScheduleStore) Get(ctx context.Context, id string) (*domain.Schedule, error) {
	if strings.TrimSpace(id) == "" {
		return nil, ErrScheduleNotFound
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	path := s.filePath(id)
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, ErrScheduleNotFound
		}
		return nil, fmt.Errorf("failed to read schedule file: %w", err)
	}

	var sched domain.Schedule
	if err := json.Unmarshal(data, &sched); err != nil {
		return nil, fmt.Errorf("failed to decode schedule data: %w", err)
	}

	return &sched, nil
}

// Save creates or updates a schedule JSON file.
func (s *JSONScheduleStore) Save(ctx context.Context, schedule *domain.Schedule) error {
	if schedule == nil {
		return errors.New("cannot save nil schedule")
	}

	if strings.TrimSpace(schedule.ID) == "" {
		schedule.ID = fmt.Sprintf("sched_%d", time.Now().UnixNano())
	}

	now := time.Now().UTC()
	if schedule.CreatedAt.IsZero() {
		schedule.CreatedAt = now
	}
	schedule.UpdatedAt = now

	data, err := json.MarshalIndent(schedule, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal schedule to JSON: %w", err)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// Ensure directory exists in case it was deleted at runtime
	_ = os.MkdirAll(s.dir, 0755)

	path := s.filePath(schedule.ID)
	// Write atomic to temp file first to prevent corruption during unexpected shutdowns
	tempPath := fmt.Sprintf("%s.tmp.%d", path, time.Now().UnixNano())
	if err := os.WriteFile(tempPath, data, 0644); err != nil {
		return fmt.Errorf("failed to write temporary schedule file: %w", err)
	}

	// Rename temp file to final hashed destination (atomic on POSIX, standard replace on Windows)
	if err := os.Rename(tempPath, path); err != nil {
		// On Windows, rename fails if destination exists, so remove first if needed
		_ = os.Remove(path)
		if err := os.Rename(tempPath, path); err != nil {
			_ = os.Remove(tempPath)
			return fmt.Errorf("failed to commit schedule file %s: %w", path, err)
		}
	}

	return nil
}

// Delete removes the hashed JSON file associated with the schedule ID.
func (s *JSONScheduleStore) Delete(ctx context.Context, id string) error {
	if strings.TrimSpace(id) == "" {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	path := s.filePath(id)
	if err := os.Remove(path); err != nil {
		if os.IsNotExist(err) {
			return ErrScheduleNotFound
		}
		return fmt.Errorf("failed to delete schedule file: %w", err)
	}

	return nil
}

