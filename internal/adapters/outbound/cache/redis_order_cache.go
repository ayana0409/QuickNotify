package cache

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"quicknotify/internal/core/ports"
	"quicknotify/internal/pkg/config"

	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

const (
	// OrderCacheTTL defines the cache duration for orders (9 days).
	OrderCacheTTL = 9 * 24 * time.Hour
)

// RedisOrderCache implements ports.OrderCache backed by a Redis instance.
type RedisOrderCache struct {
	client *redis.Client
	logger *zap.Logger
}

// NewRedisOrderCache creates a new Redis client and checks connection health.
func NewRedisOrderCache(cfg config.RedisConfig, logger *zap.Logger) (*RedisOrderCache, error) {
	addr := fmt.Sprintf("%s:%d", cfg.Host, cfg.Port)

	opts := &redis.Options{
		Addr:     addr,
		Username: cfg.Username,
		Password: cfg.Password,
		DB:       cfg.DB,
	}

	client := redis.NewClient(opts)

	// Ping to verify authentication and network reachability with a 5-second timeout
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("failed to ping redis at %s: %w", addr, err)
	}

	logger.Info("Successfully connected to Redis cache",
		zap.String("host", cfg.Host),
		zap.Int("port", cfg.Port),
	)

	return &RedisOrderCache{
		client: client,
		logger: logger,
	}, nil
}

// FormatKey generates a clear key identifier for the order: quick-bite-order-{orderId}-user
func (r *RedisOrderCache) FormatKey(orderID string) string {
	return fmt.Sprintf("quick-bite-order-%s-user", orderID)
}

// SetOrder saves order metadata into Redis with the given TTL.
func (r *RedisOrderCache) SetOrder(ctx context.Context, orderID string, meta ports.OrderMetadata, ttl time.Duration) error {
	if ttl <= 0 {
		ttl = OrderCacheTTL
	}

	key := r.FormatKey(orderID)
	data, err := json.Marshal(meta)
	if err != nil {
		return fmt.Errorf("failed to serialize order metadata: %w", err)
	}

	if err := r.client.Set(ctx, key, data, ttl).Err(); err != nil {
		r.logger.Error("Failed to cache order in Redis",
			zap.String("key", key),
			zap.Error(err),
		)
		return err
	}

	r.logger.Debug("Cached order in Redis",
		zap.String("key", key),
		zap.Duration("ttl", ttl),
	)
	return nil
}

// GetOrder retrieves order metadata from Redis.
func (r *RedisOrderCache) GetOrder(ctx context.Context, orderID string) (ports.OrderMetadata, bool, error) {
	key := r.FormatKey(orderID)
	val, err := r.client.Get(ctx, key).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return ports.OrderMetadata{}, false, nil
		}
		r.logger.Error("Failed to fetch order from Redis",
			zap.String("key", key),
			zap.Error(err),
		)
		return ports.OrderMetadata{}, false, err
	}

	var meta ports.OrderMetadata
	if err := json.Unmarshal([]byte(val), &meta); err != nil {
		return ports.OrderMetadata{}, false, fmt.Errorf("failed to unmarshal cached order metadata: %w", err)
	}

	return meta, true, nil
}

// DeleteOrder removes the cached order from Redis immediately.
func (r *RedisOrderCache) DeleteOrder(ctx context.Context, orderID string) error {
	key := r.FormatKey(orderID)
	if err := r.client.Del(ctx, key).Err(); err != nil {
		r.logger.Error("Failed to delete order from Redis",
			zap.String("key", key),
			zap.Error(err),
		)
		return err
	}

	r.logger.Debug("Deleted order from Redis cache", zap.String("key", key))
	return nil
}

// Close gracefully closes the Redis client.
func (r *RedisOrderCache) Close() error {
	r.logger.Info("Closing Redis cache connection")
	return r.client.Close()
}

