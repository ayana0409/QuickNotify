package kafka

import (
	"context"
	"errors"
	"fmt"
	"time"

	"quicknotify/internal/adapters/inbound/kafka/translators"
	"quicknotify/internal/core/ports"

	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

// ConsumerConfig holds connection and consumption settings for a Kafka consumer group.
type ConsumerConfig struct {
	Brokers  []string
	GroupID  string
	Topic    string
	MinBytes int
	MaxBytes int
}

// Consumer wraps a segmentio/kafka-go Reader to consume messages and route them to the Core Dispatcher.
type Consumer struct {
	reader     *kafka.Reader
	dispatcher ports.NotificationService
	translator ports.MessageTranslator
	logger     *zap.Logger
	topic      string
}

// NewConsumer creates a new Kafka consumer instance.
func NewConsumer(
	cfg ConsumerConfig,
	dispatcher ports.NotificationService,
	translator ports.MessageTranslator,
	logger *zap.Logger,
) *Consumer {
	if translator == nil {
		translator = translators.NewDirectTranslator()
	}

	if cfg.MinBytes <= 0 {
		cfg.MinBytes = 10e3 // 10KB
	}
	if cfg.MaxBytes <= 0 {
		cfg.MaxBytes = 10e6 // 10MB
	}

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        cfg.Brokers,
		GroupID:        cfg.GroupID,
		Topic:          cfg.Topic,
		MinBytes:       cfg.MinBytes,
		MaxBytes:       cfg.MaxBytes,
		CommitInterval: 1 * time.Second,
		StartOffset:    kafka.LastOffset,
	})

	return &Consumer{
		reader:     reader,
		dispatcher: dispatcher,
		translator: translator,
		logger:     logger,
		topic:      cfg.Topic,
	}
}

// Start begins the consumer loop, fetching messages, translating payloads, and dispatching events.
// It continues running until the context is cancelled.
func (c *Consumer) Start(ctx context.Context) {
	c.logger.Info("Starting Kafka consumer listener",
		zap.String("topic", c.topic),
	)

	for {
		select {
		case <-ctx.Done():
			c.logger.Info("Kafka consumer stopping due to context cancellation",
				zap.String("topic", c.topic),
			)
			return

		default:
			// FetchMessage allows manual offset commit after processing
			msg, err := c.reader.FetchMessage(ctx)
			if err != nil {
				if errors.Is(err, context.Canceled) {
					return
				}
				c.logger.Error("Failed to fetch message from Kafka",
					zap.String("topic", c.topic),
					zap.Error(err),
				)
				// Backoff briefly on error before retrying fetch
				time.Sleep(500 * time.Millisecond)
				continue
			}

			c.processMessage(ctx, msg)
		}
	}
}

// processMessage translates a raw Kafka message, dispatches it, and commits the offset.
func (c *Consumer) processMessage(ctx context.Context, msg kafka.Message) {
	c.logger.Debug("Received Kafka message",
		zap.String("topic", msg.Topic),
		zap.Int("partition", msg.Partition),
		zap.Int64("offset", msg.Offset),
	)

	envelope, err := c.translator.Translate(msg.Value)
	if err != nil {
		c.logger.Error("Failed to translate message payload",
			zap.String("topic", msg.Topic),
			zap.Int64("offset", msg.Offset),
			zap.Error(err),
		)
		// Still commit unparseable message to avoid poisoning the queue loop
		_ = c.reader.CommitMessages(ctx, msg)
		return
	}

	if err := c.dispatcher.ProcessEvent(ctx, envelope); err != nil {
		c.logger.Error("Failed to dispatch event",
			zap.String("event_id", envelope.EventID),
			zap.Error(err),
		)
	}

	// Commit offset upon completion
	if err := c.reader.CommitMessages(ctx, msg); err != nil {
		c.logger.Error("Failed to commit offset",
			zap.String("topic", msg.Topic),
			zap.Int64("offset", msg.Offset),
			zap.Error(err),
		)
	}
}

// Close closes the underlying Kafka reader connection safely.
func (c *Consumer) Close() error {
	c.logger.Info("Closing Kafka consumer reader", zap.String("topic", c.topic))
	if err := c.reader.Close(); err != nil {
		return fmt.Errorf("failed to close kafka reader for topic %s: %w", c.topic, err)
	}
	return nil
}

