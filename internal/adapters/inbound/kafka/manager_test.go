package kafka

import (
	"context"
	"testing"

	"quicknotify/internal/core/domain"
	"quicknotify/internal/pkg/config"

	"go.uber.org/zap"
)

type dummyDispatcher struct{}

func (d *dummyDispatcher) ProcessEvent(ctx context.Context, env domain.NotificationEnvelope) error {
	return nil
}

func TestConsumerManager_InitFromConfig(t *testing.T) {
	logger := zap.NewNop()
	registry := NewTranslatorRegistry(nil, nil)
	dispatcher := &dummyDispatcher{}
	manager := NewConsumerManager(registry, dispatcher, logger)

	cfg := &config.ConsumersConfig{
		Clusters: []config.ClusterConfig{
			{
				ID:      "cluster_main",
				Brokers: []string{"localhost:9092"},
			},
			{
				ID:      "cluster_secondary",
				Brokers: []string{"localhost:9093"},
			},
		},
		Consumers: []config.ConsumerEntry{
			{
				Topic:      "global-notify-events",
				ClusterID:  "cluster_main",
				Translator: "direct",
			},
			{
				Topic:      "quickbite-orders",
				ClusterID:  "cluster_main",
				Translator: "quickbite_order",
			},
			{
				Topic:      "edutrack-exam-results",
				ClusterID:  "cluster_secondary",
				Translator: "edutrack_exam",
			},
			{
				Topic:      "invalid-cluster-topic",
				ClusterID:  "non_existent_cluster",
				Translator: "direct",
			},
			{
				Topic:      "invalid-translator-topic",
				ClusterID:  "cluster_main",
				Translator: "unknown_translator",
			},
		},
	}

	err := manager.InitFromConfig(cfg, "test-group")
	if err != nil {
		t.Fatalf("unexpected error initializing from config: %v", err)
	}

	// 3 valid consumers should have been initialized (2 invalid ones skipped)
	if total := manager.TotalConsumers(); total != 3 {
		t.Errorf("expected 3 valid consumers, got %d", total)
	}

	// Close all consumers (readers) safely
	if err := manager.Close(); err != nil {
		t.Fatalf("unexpected error closing manager: %v", err)
	}
}

