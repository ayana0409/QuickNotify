package cache

import (
	"context"
	"testing"
	"time"

	"quicknotify/internal/core/ports"
	"quicknotify/internal/pkg/config"

	"go.uber.org/zap"
)

func TestRedisOrderCache_KeyFormat(t *testing.T) {
	cache := &RedisOrderCache{}
	orderID := "3a2484da-14de-c9f6-8b88-719595417493"
	expected := "quick-bite-order-3a2484da-14de-c9f6-8b88-719595417493-user"

	key := cache.FormatKey(orderID)
	if key != expected {
		t.Errorf("expected key '%s', got '%s'", expected, key)
	}
}

func TestMemoryOrderCache(t *testing.T) {
	cache := NewMemoryOrderCache()
	ctx := context.Background()

	orderID := "ord_test_123"
	meta := ports.OrderMetadata{
		CustomerID:  "cust_001",
		OrderCode:   "QB-001",
		TotalAmount: 150000,
		Currency:    "VND",
	}

	// 1. SetOrder
	err := cache.SetOrder(ctx, orderID, meta, 9*24*time.Hour)
	if err != nil {
		t.Fatalf("unexpected error setting order: %v", err)
	}

	// 2. GetOrder
	fetched, found, err := cache.GetOrder(ctx, orderID)
	if err != nil || !found {
		t.Fatalf("expected order to be found, got error: %v", err)
	}
	if fetched.CustomerID != "cust_001" || fetched.OrderCode != "QB-001" {
		t.Errorf("fetched metadata mismatch: %+v", fetched)
	}

	// 3. DeleteOrder
	err = cache.DeleteOrder(ctx, orderID)
	if err != nil {
		t.Fatalf("unexpected error deleting order: %v", err)
	}

	_, foundAfter, _ := cache.GetOrder(ctx, orderID)
	if foundAfter {
		t.Errorf("expected order to be deleted")
	}
}

func TestLiveRedisConnection(t *testing.T) {
	cfg := config.RedisConfig{
		Host:     "throat-viable-plough-13341.db.redis.io",
		Port:     13166,
		Username: "default",
		Password: "g9vUFQs8hO7ZgUSCLXrKyI2Xa91VhhEl",
		DB:       0,
		Enabled:  true,
	}

	logger := zap.NewNop()
	redisCache, err := NewRedisOrderCache(cfg, logger)
	if err != nil {
		t.Logf("Live Redis connection skipped (e.g. running in sandbox or offline): %v", err)
		return
	}
	defer func() { _ = redisCache.Close() }()

	ctx := context.Background()
	testOrderID := "test_live_order_999"
	testMeta := ports.OrderMetadata{
		CustomerID:  "test_cust_live",
		OrderCode:   "QB-LIVE-TEST",
		TotalAmount: 99000,
		Currency:    "VND",
	}

	// Test Set with 9 days TTL
	if err := redisCache.SetOrder(ctx, testOrderID, testMeta, OrderCacheTTL); err != nil {
		t.Fatalf("failed to set live redis key: %v", err)
	}

	// Test Get
	fetched, found, err := redisCache.GetOrder(ctx, testOrderID)
	if err != nil || !found {
		t.Fatalf("failed to get live redis key: %v", err)
	}
	if fetched.OrderCode != "QB-LIVE-TEST" {
		t.Errorf("expected OrderCode 'QB-LIVE-TEST', got '%s'", fetched.OrderCode)
	}

	// Test Delete
	if err := redisCache.DeleteOrder(ctx, testOrderID); err != nil {
		t.Fatalf("failed to delete live redis key: %v", err)
	}

	_, foundAfter, err := redisCache.GetOrder(ctx, testOrderID)
	if err != nil || foundAfter {
		t.Errorf("expected live key to be deleted, found: %v", foundAfter)
	}
}
