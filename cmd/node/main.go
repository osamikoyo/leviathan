package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/osamikoyo/leviathan/config"
	"github.com/osamikoyo/leviathan/logger"
	"github.com/osamikoyo/leviathan/node"
	"go.uber.org/zap"
)

const (
	defaultConfigPath = "node_config.yaml"
	defaultLogFile    = "logs/node.log"
	defaultLogLevel   = "info"
)

var (
	ErrInvalidConfigPath = errors.New("invalid config path")
	ErrNodeInitFailed    = errors.New("node initialization failed")
)

func main() {
	if err := run(); err != nil {
		log.Fatalf("Application failed: %v", err)
	}
}

func run() error {
	// Parse command line arguments
	configPath, logLevel, err := parseArgs()
	if err != nil {
		return fmt.Errorf("failed to parse arguments: %w", err)
	}

	// Initialize logger
	logcfg := logger.Config{
		AppName:   "node",
		AddCaller: false,
		LogFile:   defaultLogFile,
		LogLevel:  logLevel,
	}

	if err := logger.Init(logcfg); err != nil {
		return fmt.Errorf("failed to initialize logger: %w", err)
	}

	log := logger.Get()
	log.Info("Node starting", zap.String("config_path", configPath))

	// Create shutdown context
	ctx, cancel := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
		syscall.SIGQUIT,
	)
	defer cancel()

	// Load configuration
	cfg, err := loadConfig(configPath, log)
	if err != nil {
		log.Error("Failed to load config", zap.Error(err))
		return fmt.Errorf("config loading failed: %w", err)
	}

	// Initialize and start node
	nodeInstance, err := initializeNode(cfg, log)
	if err != nil {
		log.Error("Failed to initialize node", zap.Error(err))
		return fmt.Errorf("%w: %v", ErrNodeInitFailed, err)
	}

	return runNodeWithGracefulShutdown(ctx, nodeInstance, log)
}

func parseArgs() (string, string, error) {
	var (
		configPath = flag.String("config", defaultConfigPath, "Path to configuration file")
		logLevel   = flag.String("log-level", defaultLogLevel, "Log level (debug, info, warn, error)")
		help       = flag.Bool("help", false, "Show help")
	)

	flag.Parse()

	if *help {
		flag.Usage()
		os.Exit(0)
	}

	// Validate config path
	if *configPath == "" {
		return "", "", ErrInvalidConfigPath
	}

	// Convert to absolute path for better logging
	absPath, err := filepath.Abs(*configPath)
	if err != nil {
		// If we can't get absolute path, use the original
		absPath = *configPath
	}

	return absPath, *logLevel, nil
}

func loadConfig(path string, log *logger.Logger) (*config.NodeConfig, error) {
	log.Info("Loading configuration", zap.String("path", path))

	// Check if config file exists
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return nil, fmt.Errorf("configuration file does not exist: %s", path)
	}

	cfg, err := config.NewNodeConfig(path)
	if err != nil {
		return nil, fmt.Errorf("failed to parse config file %s: %w", path, err)
	}

	log.Info("Configuration loaded successfully",
		zap.String("path", path),
		zap.Any("config", cfg))

	return cfg, nil
}

func initializeNode(cfg *config.NodeConfig, log *logger.Logger) (*node.Node, error) {
	log.Info("Initializing node components")

	nodeInstance, err := node.ConnectNode(cfg, log)
	if err != nil {
		return nil, fmt.Errorf("failed to connect node: %w", err)
	}

	log.Info("Node components initialized successfully")
	return nodeInstance, nil
}

func runNodeWithGracefulShutdown(ctx context.Context, nodeInstance *node.Node, log *logger.Logger) error {
	log.Info("Starting node services")

	// Channel to receive errors from the node.Run goroutine
	errChan := make(chan error, 1)

	// Start node in a separate goroutine
	go func() {
		defer close(errChan)
		if err := nodeInstance.Run(ctx); err != nil {
			log.Error("Node run failed", zap.Error(err))
			errChan <- err
		}
	}()

	// Wait for either context cancellation or node error
	select {
	case <-ctx.Done():
		log.Info("Shutdown signal received, initiating graceful shutdown")
		return performGracefulShutdown(nodeInstance, log)

	case err := <-errChan:
		if err != nil {
			log.Error("Node encountered an error", zap.Error(err))
			// Still attempt graceful shutdown
			if shutdownErr := performGracefulShutdown(nodeInstance, log); shutdownErr != nil {
				log.Error("Graceful shutdown failed", zap.Error(shutdownErr))
			}
			return fmt.Errorf("node runtime error: %w", err)
		}
		log.Info("Node stopped normally")
		return nil
	}
}

func performGracefulShutdown(nodeInstance *node.Node, log *logger.Logger) error {
	log.Info("Performing graceful shutdown")

	// Create a timeout context for shutdown operations
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := nodeInstance.Close(shutdownCtx); err != nil {
		log.Error("Error during node shutdown", zap.Error(err))
		return fmt.Errorf("shutdown failed: %w", err)
	}

	log.Info("Graceful shutdown completed successfully")
	return nil
}
