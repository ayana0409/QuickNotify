package app

import (
	"quicknotify/internal/adapters/outbound/cache"
	"quicknotify/internal/core/ports"
	"quicknotify/internal/pkg/config"

	"go.uber.org/zap"
)

// initOrderCache initialises the order metadata cache.
// Tries to connect to Redis when enabled; falls back to in-memory if Redis is
// unavailable or not configured.
func initOrderCache(cfg config.RedisConfig, logger *zap.Logger) ports.OrderCache {
	if cfg.Enabled && cfg.Host != "" {
		redisCache, err := cache.NewRedisOrderCache(cfg, logger)
		if err != nil {
			logger.Warn(
				"Failed to connect to Redis, falling back to in-memory order cache",
				zap.Error(err),
			)
			return cache.NewMemoryOrderCache()
		}

		logger.Info("Order cache: Redis connected",
			zap.String("host", cfg.Host),
			zap.Int("port", cfg.Port),
		)
		return redisCache
	}

	logger.Info("Order cache: Redis disabled, using in-memory fallback")
	return cache.NewMemoryOrderCache()
}

