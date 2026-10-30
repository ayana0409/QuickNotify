package translators

import (
	"encoding/json"
	"fmt"
	"time"

	"quicknotify/internal/core/domain"
	"quicknotify/internal/core/ports"
)

// EduTrackExamPayload represents the raw grading event published by the EduTrack school system.
type EduTrackExamPayload struct {
	ExamID       string  `json:"exam_id"`
	StudentID    string  `json:"student_id"`
	StudentEmail string  `json:"student_email,omitempty"`
	Subject      string  `json:"subject"`
	Score        float64 `json:"score"`
	MaxScore     float64 `json:"max_score,omitempty"`
	GradedAt     string  `json:"graded_at,omitempty"`
}

// EduTrackExamTranslator converts EduTrack exam grading events into standard NotificationEnvelope.
type EduTrackExamTranslator struct{}

// NewEduTrackExamTranslator creates a new EduTrackExamTranslator instance.
func NewEduTrackExamTranslator() ports.MessageTranslator {
	return &EduTrackExamTranslator{}
}

// Translate transforms raw EduTrack exam JSON into a domain.NotificationEnvelope.
func (t *EduTrackExamTranslator) Translate(key []byte, value []byte) (domain.NotificationEnvelope, error) {
	var payload EduTrackExamPayload
	if err := json.Unmarshal(value, &payload); err != nil {
		return domain.NotificationEnvelope{}, fmt.Errorf("edutrack unmarshal error: %w", err)
	}

	if payload.ExamID == "" || payload.StudentID == "" {
		return domain.NotificationEnvelope{}, fmt.Errorf("edutrack message missing required fields (exam_id or student_id)")
	}

	channels := []domain.Channel{domain.ChannelWebSocket}
	if payload.StudentEmail != "" {
		channels = append(channels, domain.ChannelEmail)
	}

	eventID := fmt.Sprintf("edutrack_%s_%s_%d", payload.ExamID, payload.StudentID, time.Now().UnixNano())

	return domain.NotificationEnvelope{
		EventID:   eventID,
		ProjectID: "edutrack",
		Scope:     domain.ScopeUser,
		Channels:  channels,
		Target: domain.TargetInfo{
			UserID: payload.StudentID,
			Email:  payload.StudentEmail,
		},
		Content: domain.ContentInfo{
			Type:       "template",
			TemplateID: "EDUTRACK_EXAM_GRADED",
			Data: map[string]interface{}{
				"exam_id":   payload.ExamID,
				"subject":   payload.Subject,
				"score":     payload.Score,
				"max_score": payload.MaxScore,
				"graded_at": payload.GradedAt,
			},
		},
	}, nil
}

