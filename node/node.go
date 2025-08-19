package node

import (
	"context"
	"database/sql"
	"net"
	"os"

	_ "github.com/mattn/go-sqlite3"
	"github.com/osamikoyo/leviathan/config"
	"github.com/osamikoyo/leviathan/consumer"
	"github.com/osamikoyo/leviathan/logger"
	"github.com/osamikoyo/leviathan/models"
	"github.com/osamikoyo/leviathan/nodecore"
	"github.com/osamikoyo/leviathan/nodepb"
	"github.com/osamikoyo/leviathan/nodeserver"
	"github.com/osamikoyo/leviathan/reader"
	"github.com/osamikoyo/leviathan/retrier"
	"github.com/osamikoyo/leviathan/writer"
	amqp "github.com/rabbitmq/amqp091-go"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

type Node struct {
	server   *grpc.Server
	writer   *writer.Writer
	reader   *reader.Reader
	consumer *consumer.Consumer
	logger   *logger.Logger
	cfg      *config.NodeConfig
}

func ConnectNode(cfg *config.NodeConfig, logger *logger.Logger) (*Node, error) {
	logger.Info("connecting node", zap.Any("cfg", cfg))

	os.Create(cfg.SqlitePath)

	db, err := sql.Open("sqlite3", cfg.SqlitePath)
	if err != nil {
		logger.Error("failed connect node components",
			zap.String("path", cfg.SqlitePath),
			zap.Error(err))

		return nil, err
	}

	logger.Info("successfully connected to sqlite", zap.String("path", cfg.SqlitePath))

	readChan := make(chan *models.ReadRequest, 5)
	writeChan := make(chan *models.WriteRequest, 5)

	core, err := nodecore.NewNodeCore(
		cfg,
		writer.NewWriterClient(writeChan),
		reader.NewReaderClient(readChan),
		logger,
	)
	if err != nil {
		logger.Error("failed create node core", zap.Error(err))
		return nil, err
	}

	conn, err := retrier.Connect(5, func() (*amqp.Connection, error) {
		return amqp.Dial(cfg.RabbitmqURL)
	})
	if err != nil {
		logger.Error("failed connect to RabbitMQ",
			zap.String("url", cfg.RabbitmqURL),
			zap.Error(err))
		return nil, err
	}

	consumer, err := consumer.NewConsumer(conn, core, logger, cfg)
	if err != nil {
		logger.Error("failed get consumer", zap.Error(err))

		return nil, err
	}

	writer := writer.NewWriter(writeChan, db, logger)
	reader := reader.NewReader(readChan, db, logger)

	grpcserver := grpc.NewServer()

	server := nodeserver.NewNodeServer(core, logger, cfg)

	nodepb.RegisterNodeServer(grpcserver, server)

	return &Node{
		reader:   reader,
		writer:   writer,
		cfg:      cfg,
		consumer: consumer,
		logger:   logger,
		server:   grpcserver,
	}, nil
}

func (n *Node) Close(ctx context.Context) error {
	if err := n.consumer.Close(ctx); err != nil {
		return err
	}

	if err := n.reader.Close(); err != nil {
		return err
	}

	if err := n.writer.Close(); err != nil {
		return err
	}
	n.server.GracefulStop()

	return nil
}

func (n *Node) Run(ctx context.Context) error {
	n.logger.Info("starting node")

	go n.writer.StartDaemon(ctx)
	go n.reader.StartDaemon(ctx)
	go n.consumer.Run(ctx)

	lis, err := net.Listen("tcp", n.cfg.Addr)
	if err != nil {
		n.logger.Fatal("failed listen",
			zap.String("addr", n.cfg.Addr),
			zap.Error(err))

		return err
	}

	if err = n.server.Serve(lis); err != nil {
		n.logger.Fatal("failed serve", zap.Error(err))

		return err
	}

	return nil
}
