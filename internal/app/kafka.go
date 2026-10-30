package app

import (
	"context"
	"fmt"

	"quicknotify/internal/adapters/inbound/kafka"
	"quicknotify/internal/adapters/inbound/kafka/translators"
	"quicknotify/internal/core/ports"
	"quicknotify/internal/pkg/config"

	"go.uber.org/zap"
)

// kafkaBundle groups all Kafka-related lifecycle objects so app.go can start,
// cancel, and close them cleanly without exposing raw context primitives.
type kafkaBundle struct {
	manager         *kafka.ConsumerManager
	consumerCtx     context.Context    // passed to manager.Start()
	cancelConsumers context.CancelFunc // called during shutdown to stop all consumers
}

// initKafkaConsumers wires up the translator registry, loads multi-cluster
// consumer definitions from consumers.yaml (when configured), and falls back
// to the standard single-topic consumer when no definitions are loaded.
//
// The dispatcher is injected here because ConsumerManager forwards translated
// messages to it after each Kafka message is consumed.
//
// Returns a kafkaBundle; the caller must invoke shutdown() which calls
// bundle.cancelConsumers() then bundle.manager.Close().
func initKafkaConsumers(
	cfg *config.AppConfig,
	orderCache ports.OrderCache,
	dispatcher ports.NotificationService,
	logger *zap.Logger,
) (*kafkaBundle, error) {
	translatorRegistry := kafka.NewTranslatorRegistry(orderCache, logger)

	// Create a cancellable context that drives the consumer goroutine lifecycle
	consumerCtx, cancelConsumers := context.WithCancel(context.Background())

	consumerManager := kafka.NewConsumerManager(translatorRegistry, dispatcher, logger)

	// Load multi-cluster consumer definitions from an external consumers.yaml when set
	if cfg.Kafka.ConsumersConfigPath != "" {
		consumersCfg, err := config.LoadConsumersConfig(cfg.Kafka.ConsumersConfigPath)
		if err != nil {
			// Non-fatal: log and continue so the standard-topic fallback still runs
			logger.Warn(
				"Failed to load consumers config, will fall back to standard topic",
				zap.String("path", cfg.Kafka.ConsumersConfigPath),
				zap.Error(err),
			)
		} else {
			if err := consumerManager.InitFromConfig(consumersCfg, cfg.Kafka.ConsumerGroup); err != nil {
				cancelConsumers()
				return nil, fmt.Errorf("init consumers from config: %w", err)
			}
		}
	}

	// Fallback: if no consumers were loaded from file AND a standard topic is configured,
	// spin up one consumer on the standard topic using the DirectTranslator.
	if consumerManager.TotalConsumers() == 0 &&
		len(cfg.Kafka.Brokers) > 0 &&
		cfg.Kafka.StandardTopic != "" {
		logger.Info(
			"No consumers loaded from consumers.yaml; initialising standard topic consumer",
			zap.String("topic", cfg.Kafka.StandardTopic),
		)
		coreConsumer := kafka.NewConsumer(
			kafka.ConsumerConfig{
				Brokers: cfg.Kafka.Brokers,
				GroupID: cfg.Kafka.ConsumerGroup,
				Topic:   cfg.Kafka.StandardTopic,
			},
			dispatcher,
			translators.NewDirectTranslator(),
			logger,
		)
		consumerManager.AddConsumer(coreConsumer)
	}

	return &kafkaBundle{
		manager:         consumerManager,
		consumerCtx:     consumerCtx,
		cancelConsumers: cancelConsumers,
	}, nil
}
