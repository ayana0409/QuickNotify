package app

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"quicknotify/internal/adapters/outbound/email"
	"quicknotify/internal/adapters/outbound/websocket"
	"quicknotify/internal/core/ports"
	"quicknotify/internal/core/services"
	"quicknotify/internal/pkg/config"

	"go.uber.org/zap"
)

// App is the top-level application object. It owns all infrastructure objects
// and orchestrates their lifecycle: startup → serving → graceful shutdown.
type App struct {
	cfg        *config.AppConfig
	logger     *zap.Logger
	wsHub      *websocket.Hub
	orderCache ports.OrderCache
	dispatcher ports.NotificationService
	kafka      *kafkaBundle
	httpServer *http.Server
}

// New loads configuration, wires all dependencies, and returns a ready-to-run App.
// Returns an error if any critical dependency (config, logger, …) cannot be initialised.
func New(configPath string) (*App, error) {
	// 1. Load application configuration from YAML file
	cfg, err := config.LoadConfig(configPath)
	if err != nil {
		return nil, fmt.Errorf("load config %q: %w", configPath, err)
	}

	// 2. Initialise structured logger (must come before anything that logs)
	logger := initLogger(cfg.Log)

	logger.Info("QuickNotify starting up",
		zap.Int("http_port", cfg.Server.HTTPPort),
		zap.Strings("kafka_brokers", cfg.Kafka.Brokers),
		zap.String("standard_topic", cfg.Kafka.StandardTopic),
		zap.Bool("redis_enabled", cfg.Redis.Enabled),
	)

	// 3. Initialise outbound adapters
	wsHub := websocket.NewHub(logger)
	emailSender := email.NewSMTPSender(cfg.Email, logger)

	// 4. Initialise order metadata cache (Redis with in-memory fallback)
	orderCache := initOrderCache(cfg.Redis, logger)

	// 5. Initialise the core notification dispatcher
	dispatcher := services.NewDispatcher(wsHub, emailSender, logger)

	// 6. Initialise Kafka consumers (multi-cluster from file, or standard fallback)
	kafkaB, err := initKafkaConsumers(cfg, orderCache, dispatcher, logger)
	if err != nil {
		// Release order cache before returning so the caller doesn't need to
		_ = orderCache.Close()
		return nil, fmt.Errorf("init kafka consumers: %w", err)
	}

	// 7. Build the HTTP server (mux, routes, CORS, WS handler)
	srv := initHTTPServer(cfg, wsHub, kafkaB.manager, logger)

	return &App{
		cfg:        cfg,
		logger:     logger,
		wsHub:      wsHub,
		orderCache: orderCache,
		dispatcher: dispatcher,
		kafka:      kafkaB,
		httpServer: srv,
	}, nil
}

// Run starts the HTTP server and Kafka consumers, then blocks until an OS signal
// (SIGINT / SIGTERM) is received. It then performs a graceful shutdown in order:
//
//  1. Stop Kafka consumers (finish in-flight messages, commit offsets)
//  2. Shut down the HTTP server (wait up to 10 s for active requests to finish)
//  3. Close all remaining WebSocket connections
//  4. Release the order cache connection
//  5. Flush the logger
func (a *App) Run() {
	// Flush the logger on exit regardless of shutdown path
	defer func() { _ = a.logger.Sync() }()

	// Start all Kafka consumers in background goroutines using the bundle's context.
	// Cancellation is triggered in shutdown() via kafkaBundle.cancelConsumers().
	a.kafka.manager.Start(a.kafka.consumerCtx)

	// Start the HTTP server in a background goroutine so Run() stays non-blocking
	serverAddr := fmt.Sprintf(":%d", a.cfg.Server.HTTPPort)
	go func() {
		a.logger.Info("HTTP and WebSocket server listening", zap.String("address", serverAddr))
		// ListenAndServe always returns a non-nil error; http.ErrServerClosed is normal on shutdown
		if err := a.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			a.logger.Fatal("HTTP server encountered unexpected error", zap.Error(err))
		}
	}()

	// Block until we receive a termination signal from the OS
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	sig := <-stop

	a.logger.Info("Received shutdown signal, starting graceful shutdown",
		zap.String("signal", sig.String()),
	)

	a.shutdown()

	a.logger.Info("QuickNotify stopped cleanly. Goodbye!")
}

// shutdown performs an ordered, graceful shutdown of all subsystems.
func (a *App) shutdown() {
	// 1. Stop Kafka consumers: cancel their context first, then wait for Close()
	a.kafka.cancelConsumers()
	if err := a.kafka.manager.Close(); err != nil {
		a.logger.Error("Error closing Kafka consumers", zap.Error(err))
	}

	// 2. Shut down the HTTP server (allow up to 10 s for in-flight requests to finish)
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := a.httpServer.Shutdown(shutdownCtx); err != nil {
		a.logger.Error("HTTP server shutdown error", zap.Error(err))
	}

	// 3. Close all active WebSocket connections cleanly
	a.wsHub.CloseAll()

	// 4. Release the order cache connection (Redis or in-memory)
	if err := a.orderCache.Close(); err != nil {
		a.logger.Error("Error closing order cache", zap.Error(err))
	}
}
