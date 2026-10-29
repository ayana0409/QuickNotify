package email

import (
	"context"
	"encoding/json"
	"fmt"
	"net/smtp"
	"strings"

	"quicknotify/internal/core/domain"
	"quicknotify/internal/core/ports"

	"go.uber.org/zap"
)

// Config holds SMTP server connection parameters.
type Config struct {
	Host     string `mapstructure:"host"`
	Port     int    `mapstructure:"port"`
	Username string `mapstructure:"username"`
	Password string `mapstructure:"password"`
	From     string `mapstructure:"from"`
	Enabled  bool   `mapstructure:"enabled"` // Toggle actual sending vs mock logging
}

// SMTPSender implements ports.EmailPort for sending email notifications.
type SMTPSender struct {
	cfg    Config
	logger *zap.Logger
}

// NewSMTPSender creates a new SMTPSender instance.
func NewSMTPSender(cfg Config, logger *zap.Logger) ports.EmailPort {
	return &SMTPSender{
		cfg:    cfg,
		logger: logger,
	}
}

// SendEmail delivers an email notification to the specified recipient.
func (s *SMTPSender) SendEmail(ctx context.Context, target domain.TargetInfo, content domain.ContentInfo) error {
	if target.Email == "" {
		s.logger.Warn("Email delivery skipped: target email is empty")
		return nil
	}

	subject := "QuickNotify Notification"
	body := s.formatBody(content)

	if !s.cfg.Enabled {
		s.logger.Info("[DEV/DEMO] Email sending simulated (SMTP disabled)",
			zap.String("to", target.Email),
			zap.String("subject", subject),
			zap.String("body", body),
		)
		return nil
	}

	// Format MIME message
	addr := fmt.Sprintf("%s:%d", s.cfg.Host, s.cfg.Port)
	message := fmt.Appendf(nil, "To: %s\r\nSubject: %s\r\nContent-Type: text/plain; charset=UTF-8\r\n\r\n%s",
		target.Email, subject, body)

	var auth smtp.Auth
	if s.cfg.Username != "" && s.cfg.Password != "" {
		auth = smtp.PlainAuth("", s.cfg.Username, s.cfg.Password, s.cfg.Host)
	}

	// Send email asynchronously in background goroutine to avoid blocking caller
	go func() {
		err := smtp.SendMail(addr, auth, s.cfg.From, []string{target.Email}, message)
		if err != nil {
			s.logger.Error("Failed to send SMTP email",
				zap.String("to", target.Email),
				zap.Error(err),
			)
			return
		}
		s.logger.Info("Email sent successfully via SMTP",
			zap.String("to", target.Email),
		)
	}()

	return nil
}

// formatBody constructs a readable text body from ContentInfo.
func (s *SMTPSender) formatBody(content domain.ContentInfo) string {
	switch content.Type {
	case "raw":
		if msg, ok := content.Data["message"].(string); ok {
			return msg
		}
	case "template":
		var sb strings.Builder
		sb.WriteString(fmt.Sprintf("Notification [Template: %s]\n\n", content.TemplateID))
		for k, v := range content.Data {
			sb.WriteString(fmt.Sprintf("- %s: %v\n", k, v))
		}
		return sb.String()
	}

	dataBytes, _ := json.MarshalIndent(content.Data, "", "  ")
	return string(dataBytes)
}
