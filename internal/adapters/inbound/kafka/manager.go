package kafka

import (
	"context"
	"fmt"
	"strings"

	"quicknotify/internal/core/ports"
	"quicknotify/internal/pkg/config"

	"go.uber.org/zap"
)

// ConsumerManager manages multiple Kafka consumer listeners across different clusters and topics.
type ConsumerManager struct {
	consumers  []*Consumer
	registry   *TranslatorRegistry
	dispatcher ports.NotificationService
	logger     *zap.Logger
}

// NewConsumerManager initializes a new ConsumerManager instance.
func NewConsumerManager(
	registry *TranslatorRegistry,
	dispatcher ports.NotificationService,
	logger *zap.Logger,
) *ConsumerManager {
	if registry == nil {
		registry = NewTranslatorRegistry(nil, logger)
	}
	return &ConsumerManager{
		consumers:  make([]*Consumer, 0),
		registry:   registry,
		dispatcher: dispatcher,
		logger:     logger,
	}
}

// Registry returns the underlying TranslatorRegistry to allow registering custom translators.
func (m *ConsumerManager) Registry() *TranslatorRegistry {
	return m.registry
}

// AddConsumer directly adds an existing consumer instance to the manager.
func (m *ConsumerManager) AddConsumer(c *Consumer) {
	m.consumers = append(m.consumers, c)
}

// InitFromConfig parses ConsumersConfig, resolves cluster brokers, matches translators,
// and initializes Kafka consumers for all declared topic mappings.
func (m *ConsumerManager) InitFromConfig(cfg *config.ConsumersConfig, defaultGroupID string) error {
	if cfg == nil {
		return fmt.Errorf("consumers configuration cannot be nil")
	}

	// Index clusters by ID
	clusterMap := make(map[string][]string)
	for _, cluster := range cfg.Clusters {
		clusterMap[cluster.ID] = cluster.Brokers
	}

	for _, entry := range cfg.Consumers {
		brokers, exists := clusterMap[entry.ClusterID]
		if !exists || len(brokers) == 0 {
			m.logger.Warn("Skipping consumer: cluster_id not found or has no brokers",
				zap.String("topic", entry.Topic),
				zap.String("cluster_id", entry.ClusterID),
			)
			continue
		}

		translator, err := m.registry.Get(entry.Translator)
		if err != nil {
			m.logger.Warn("Skipping consumer: translator not found in registry",
				zap.String("topic", entry.Topic),
				zap.String("translator", entry.Translator),
				zap.Error(err),
			)
			continue
		}

		groupID := defaultGroupID
		if groupID == "" {
			groupID = "quicknotify-consumer-group"
		}

		consumer := NewConsumer(
			ConsumerConfig{
				Brokers: brokers,
				GroupID: groupID,
				Topic:   entry.Topic,
			},
			m.dispatcher,
			translator,
			m.logger,
		)

		m.consumers = append(m.consumers, consumer)
		m.logger.Info("Configured Kafka consumer for topic",
			zap.String("topic", entry.Topic),
			zap.String("cluster_id", entry.ClusterID),
			zap.String("translator", entry.Translator),
			zap.Strings("brokers", brokers),
		)
	}

	return nil
}

// Start launches all registered consumers in independent background goroutines.
func (m *ConsumerManager) Start(ctx context.Context) {
	m.logger.Info("Starting all configured Kafka consumers", zap.Int("total_consumers", len(m.consumers)))
	for _, c := range m.consumers {
		go c.Start(ctx)
	}
}

// Close gracefully closes all active Kafka consumer reader connections.
func (m *ConsumerManager) Close() error {
	m.logger.Info("Closing all Kafka consumers", zap.Int("total_consumers", len(m.consumers)))
	var errs []string

	for _, c := range m.consumers {
		if err := c.Close(); err != nil {
			errs = append(errs, err.Error())
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("errors closing consumers: %s", strings.Join(errs, "; "))
	}
	return nil
}

// TotalConsumers returns the number of registered consumers.
func (m *ConsumerManager) TotalConsumers() int {
	return len(m.consumers)
}

