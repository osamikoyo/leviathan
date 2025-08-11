package node

import (
	"database/sql"
	"os"

	_ "github.com/mattn/go-sqlite3"
	"github.com/osamikoyo/leviathan/config"
	"github.com/osamikoyo/leviathan/logger"
	"github.com/osamikoyo/leviathan/models"
	"github.com/osamikoyo/leviathan/nodecore"
	"github.com/osamikoyo/leviathan/nodepb"
	"github.com/osamikoyo/leviathan/nodeserver"
	"github.com/osamikoyo/leviathan/reader"
	"github.com/osamikoyo/leviathan/writer"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

type Node struct {
	server *grpc.Server
	writer *writer.Writer
	reader *reader.Reader
	logger *logger.Logger
	cfg    *config.NodeConfig
}

func ConnectNode(cfg *config.NodeConfig, logger *logger.Logger) (*Node, error) {
	os.Create(cfg.SqlitePath)

	db, err := sql.Open("sqlite3", cfg.SqlitePath)
	if err != nil {
		logger.Error("failed connect node components",
			zap.String("path", cfg.SqlitePath),
			zap.Error(err))

		return nil, err
	}

	readChan := make(chan *models.ReadRequest, 5)
	writeChan := make(chan *models.WriteRequest, 5)

	core := nodecore.NewNodeCore(
		cfg,
		writer.NewWriterClient(writeChan),
		reader.NewReaderClient(readChan),
		logger,
	)

	writer := writer.NewWriter(writeChan, db, logger)
	reader := reader.NewReader(readChan, db, logger)

	grpcserver := grpc.NewServer()

	server := nodeserver.NewNodeServer(core, logger, cfg)

	nodepb.RegisterNodeServer(grpcserver, server)

	return &Node{
		reader: reader,
		writer: writer,
		cfg:    cfg,
		logger: logger,
		server: grpcserver,
	}, nil
}
