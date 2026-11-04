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
// consumer definitions from KAFKA_CLUSTERS/KAFKA_CONSUMERS env vars (when configured),
// and falls back to the standard single-topic consumer when no custom mapping is loaded.
//
// Architecture:
// 1. QuickNotify's own core consumer: KAFKA_BROKERS + KAFKA_STANDARD_TOPIC (DirectTranslator)
// 2. External project consumers: KAFKA_CLUSTERS + KAFKA_CONSUMERS (custom translators per cluster/topic)
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

	// --- Path 1: Multi-cluster mapping from KAFKA_CLUSTERS + KAFKA_CONSUMERS ---
	// When external/legacy project Kafka clusters are configured, use the registry.
	hasStandardTopicInConsumers := false
	if cfg.Kafka.ConsumersConfig != nil && len(cfg.Kafka.ConsumersConfig.Consumers) > 0 {
		for _, c := range cfg.Kafka.ConsumersConfig.Consumers {
			if c.Topic == cfg.Kafka.StandardTopic {
				hasStandardTopicInConsumers = true
				break
			}
		}
		if err := consumerManager.InitFromConfig(cfg.Kafka.ConsumersConfig, cfg.Kafka.ConsumerGroup); err != nil {
			cancelConsumers()
			return nil, fmt.Errorf("init consumers from env config: %w", err)
		}
	}

	// --- Path 2: Core standard topic consumer ---
	// If the standard core topic was not already declared in KAFKA_CONSUMERS,
	// always initialize it using QuickNotify's primary brokers and DirectTranslator.
	if !hasStandardTopicInConsumers &&
		len(cfg.Kafka.Brokers) > 0 &&
		cfg.Kafka.StandardTopic != "" {
		logger.Info(
			"Initialising core standard topic consumer",
			zap.Strings("brokers", cfg.Kafka.Brokers),
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
