package main

import (
	"context"
	"log"
	"os"
	"os/signal"

	"github.com/osamikoyo/leviathan/config"
	"github.com/osamikoyo/leviathan/heart"
	"github.com/osamikoyo/leviathan/logger"
	"github.com/osamikoyo/leviathan/metrics"
	"go.uber.org/zap"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	logcfg := logger.Config{
		AppName:   "heart",
		AddCaller: false,
		LogFile:   "logs/heart.log",
		LogLevel:  "debug",
	}

	if err := logger.Init(logcfg); err != nil {
		log.Fatalf("failed to initialize logger: %v", err)
	}

	logger := logger.Get()

	metrics.InitMetrics()

	path := "heart_config.yaml"

	// Better argument parsing
	for i, arg := range os.Args {
		if arg == "--config" && i+1 < len(os.Args) {
			path = os.Args[i+1]
			break
		}
	}

	cfg, err := config.NewHeartConfig(path)
	if err != nil {
		logger.Fatal("failed get config",
			zap.String("path", path),
			zap.Error(err))
	}

	logger.Info("heart service starting",
		zap.String("config_path", path),
		zap.String("listen_addr", cfg.Addr),
		zap.String("rabbitmq_url", cfg.RabbitmqUrl),
		zap.Int("nodes_count", len(cfg.Nodes)))

	heart, err := heart.ConnectHeart(cfg, logger)
	if err != nil {
		logger.Fatal("failed to connect heart",
			zap.Error(err))
		return
	}

	// Setup graceful shutdown
	defer func() {
		heart.Close()
		logger.Info("heart service stopped")
		logger.Sync()
	}()

	if err := heart.Run(ctx); err != nil {
		logger.Error("heart service error",
			zap.Error(err))
		os.Exit(1)
	}
}
