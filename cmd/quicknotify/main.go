package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"quicknotify/internal/adapters/inbound/kafka"
	"quicknotify/internal/adapters/inbound/kafka/translators"
	"quicknotify/internal/adapters/outbound/email"
	"quicknotify/internal/adapters/outbound/websocket"
	"quicknotify/internal/core/services"
	"quicknotify/internal/pkg/config"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func main() {
	configPath := flag.String("config", "configs/config.yaml", "Path to application configuration YAML file")
	flag.Parse()

	// 1. Load application configuration
	cfg, err := config.LoadConfig(*configPath)
	if err != nil {
		fmt.Printf("FATAL: Failed to load config from %s: %v\n", *configPath, err)
		os.Exit(1)
	}

	// 2. Initialize logger
	logger := initLogger(cfg.Log)
	defer func() { _ = logger.Sync() }()

	logger.Info("Starting QuickNotify Centralized Notification Service",
		zap.Int("http_port", cfg.Server.HTTPPort),
		zap.Strings("kafka_brokers", cfg.Kafka.Brokers),
		zap.String("standard_topic", cfg.Kafka.StandardTopic),
	)

	// 3. Initialize Outbound Adapters
	wsHub := websocket.NewHub(logger)
	emailSender := email.NewSMTPSender(cfg.Email, logger)

	// 4. Initialize Core Dispatcher Service
	dispatcher := services.NewDispatcher(wsHub, emailSender, logger)

	// 5. Initialize Inbound Kafka Consumer for Standard Core Events
	consumerCtx, cancelConsumer := context.WithCancel(context.Background())
	defer cancelConsumer()

	var coreConsumer *kafka.Consumer
	if len(cfg.Kafka.Brokers) > 0 && cfg.Kafka.StandardTopic != "" {
		coreConsumer = kafka.NewConsumer(
			kafka.ConsumerConfig{
				Brokers: cfg.Kafka.Brokers,
				GroupID: cfg.Kafka.ConsumerGroup,
				Topic:   cfg.Kafka.StandardTopic,
			},
			dispatcher,
			translators.NewDirectTranslator(),
			logger,
		)

		// Start Kafka Consumer loop in background goroutine
		go coreConsumer.Start(consumerCtx)
	} else {
		logger.Warn("Kafka brokers or standard topic not defined; skipping standard consumer startup")
	}

	// 6. Setup HTTP Router & Handlers
	mux := http.NewServeMux()

	// Liveness & Health probe
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"status":         "healthy",
			"service":        "quicknotify",
			"ws_connections": wsHub.TotalConnections(),
			"timestamp":      time.Now().UTC().Format(time.RFC3339),
		})
	})

	// Protected WebSocket endpoint with JWT Auth Middleware
	wsHandler := websocket.NewHandler(wsHub, logger)
	authMiddleware := websocket.AuthMiddleware(cfg.Auth.JWTSecret, logger)
	mux.Handle("/ws", authMiddleware(wsHandler))

	// 7. Start HTTP Server
	serverAddr := fmt.Sprintf(":%d", cfg.Server.HTTPPort)
	srv := &http.Server{
		Addr:         serverAddr,
		Handler:      mux,
		ReadTimeout:  time.Duration(cfg.Server.ReadTimeoutSec) * time.Second,
		WriteTimeout: time.Duration(cfg.Server.WriteTimeoutSec) * time.Second,
	}

	go func() {
		logger.Info("HTTP and WebSocket server listening", zap.String("address", serverAddr))
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Fatal("HTTP server encountered unexpected error", zap.Error(err))
		}
	}()

	// 8. Graceful Shutdown (handling SIGINT, SIGTERM)
	shutdownSignal := make(chan os.Signal, 1)
	signal.Notify(shutdownSignal, os.Interrupt, syscall.SIGTERM)

	sig := <-shutdownSignal
	logger.Info("Received termination signal, initiating graceful shutdown",
		zap.String("signal", sig.String()),
	)

	// Stop Kafka Consumer first to finish in-flight messages and commit offsets
	if coreConsumer != nil {
		cancelConsumer()
		if err := coreConsumer.Close(); err != nil {
			logger.Error("Error closing Kafka consumer", zap.Error(err))
		}
	}

	// Graceful shutdown HTTP server with a 10-second timeout
	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelShutdown()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		logger.Error("HTTP server shutdown encountered error", zap.Error(err))
	}

	// Terminate active WebSocket connections cleanly
	wsHub.CloseAll()

	logger.Info("QuickNotify has stopped cleanly. Goodbye!")
}

// initLogger constructs a zap.Logger according to configuration.
func initLogger(cfg config.LogConfig) *zap.Logger {
	var zapCfg zap.Config
	if cfg.Development {
		zapCfg = zap.NewDevelopmentConfig()
		zapCfg.EncoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder
	} else {
		zapCfg = zap.NewProductionConfig()
	}

	var level zapcore.Level
	if err := level.UnmarshalText([]byte(cfg.Level)); err == nil {
		zapCfg.Level.SetLevel(level)
	}

	logger, err := zapCfg.Build()
	if err != nil {
		fallback, _ := zap.NewDevelopment()
		return fallback
	}

	return logger
}

