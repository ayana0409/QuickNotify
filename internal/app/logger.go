package app

import (
	"quicknotify/internal/pkg/config"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// initLogger constructs a zap.Logger based on the LogConfig.
// In development mode, it uses colored console output; in production, structured JSON.
func initLogger(cfg config.LogConfig) *zap.Logger {
	var zapCfg zap.Config
	if cfg.Development {
		zapCfg = zap.NewDevelopmentConfig()
		// Colorize log levels in development for easier reading
		zapCfg.EncoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder
	} else {
		zapCfg = zap.NewProductionConfig()
	}

	// Parse and apply the log level from config (debug, info, warn, error...)
	var level zapcore.Level
	if err := level.UnmarshalText([]byte(cfg.Level)); err == nil {
		zapCfg.Level.SetLevel(level)
	}

	logger, err := zapCfg.Build()
	if err != nil {
		// If something went wrong building the configured logger, fall back to development
		fallback, _ := zap.NewDevelopment()
		return fallback
	}

	return logger
}

