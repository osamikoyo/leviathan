package heart

import (
	"context"
	"crypto/tls"
	"errors"
	"net"

	"github.com/osamikoyo/leviathan/config"
	"github.com/osamikoyo/leviathan/heartcore"
	"github.com/osamikoyo/leviathan/heartpb"
	"github.com/osamikoyo/leviathan/heartserver"
	"github.com/osamikoyo/leviathan/logger"
	"github.com/osamikoyo/leviathan/producer"
	"github.com/osamikoyo/leviathan/retrier"
	amqp "github.com/rabbitmq/amqp091-go"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

type Heart struct {
	cfg        *config.HeartConfig
	logger     *logger.Logger
	core       *heartcore.HeartCore
	server     *heartserver.HeartServer
	grpcserver *grpc.Server
	certPath   string
	keyPath    string
}

func ConnectHeart(cfg *config.HeartConfig, logger *logger.Logger, cert, key string) (*Heart, error) {
	if cfg == nil {
		return nil, errors.New("config cannot be nil")
	}
	if logger == nil {
		return nil, errors.New("logger cannot be nil")
	}

	logger.Info("connecting heart", zap.Any("cfg", cfg))

	conn, err := retrier.Connect(5, func() (*amqp.Connection, error) {
		return amqp.Dial(cfg.RabbitmqUrl)
	})
	if err != nil {
		logger.Error("failed to connect to rabbitmq",
			zap.String("url", cfg.RabbitmqUrl),
			zap.Error(err))
		return nil, err
	}

	producer, err := producer.NewProducer(conn, cfg, logger)
	if err != nil {
		logger.Error("failed to get producer",
			zap.Error(err))
		return nil, err
	}

	// Create the heart core with enhanced node clients and health checking
	core, err := heartcore.NewHeartCore(logger, cfg, producer)
	if err != nil {
		logger.Error("failed to create heart core", zap.Error(err))
		return nil, err
	}

	return &Heart{
		core:     core,
		server:   heartserver.NewHeartServer(core, logger, cfg),
		logger:   logger,
		cfg:      cfg,
		certPath: cert,
		keyPath:  key,
	}, nil
}

func (h *Heart) Run(ctx context.Context) error {
	h.logger.Info("starting heart")

	// Start health checking
	h.core.Start(ctx)

	lis, err := net.Listen("tcp", h.cfg.Addr)
	if err != nil {
		h.logger.Error("failed to listen",
			zap.String("addr", h.cfg.Addr),
			zap.Error(err))
		return err
	}

	cert, err := tls.LoadX509KeyPair(h.certPath, h.keyPath)
	if err != nil {
		h.logger.Error("failed load keys",
			zap.String("cert_path", h.certPath),
			zap.String("key_path", h.keyPath),
			zap.Error(err))

		return err
	}

	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{cert},
	}

	creds := credentials.NewTLS(tlsConfig)

	server := grpc.NewServer(grpc.Creds(creds))
	h.grpcserver = server // Store server for graceful shutdown

	heartpb.RegisterHeartServer(server, h.server)

	// Start server in goroutine to handle graceful shutdown
	errorChan := make(chan error, 1)
	go func() {
		h.logger.Info("heart server listening", zap.String("addr", h.cfg.Addr))
		if err := server.Serve(lis); err != nil {
			errorChan <- err
		}
	}()

	// Wait for context cancellation or server error
	select {
	case <-ctx.Done():
		h.logger.Info("shutting down heart server gracefully")
		server.GracefulStop()
		h.core.Stop()
		return nil
	case err := <-errorChan:
		h.logger.Error("heart server error",
			zap.String("addr", h.cfg.Addr),
			zap.Error(err))
		h.core.Stop()
		return err
	}
}

func (h *Heart) Close() {
	if h.grpcserver != nil {
		h.grpcserver.GracefulStop()
	}
	if h.core != nil {
		h.core.Stop()
	}
}
