package nodecore

import (
	"github.com/osamikoyo/leviathan/config"
	"github.com/osamikoyo/leviathan/logger"
	"github.com/osamikoyo/leviathan/reader"
	"github.com/osamikoyo/leviathan/writer"
	"go.uber.org/zap"
)

type NodeCore struct {
	cfg         *config.NodeConfig
	writeClient *writer.WriterClient
	readClient  *reader.ReaderClient
	logger      *logger.Logger
}

func NewNodeCore(
	cfg *config.NodeConfig,
	writer *writer.WriterClient,
	reader *reader.ReaderClient,
	logger *logger.Logger,
) *NodeCore {
	return &NodeCore{
		cfg:         cfg,
		writeClient: writer,
		readClient:  reader,
		logger:      logger,
	}
}

func (n *NodeCore) RouteWriteRequest(sql string, args ...any) error {
	n.logger.Info("new write request", zap.Any("sql", sql))

	if err := n.writeClient.Write(sql, args...); err != nil {
		n.logger.Error("failed write",
			zap.String("sql", sql),
			zap.Error(err))

		return err
	}

	return nil
}

func (n *NodeCore) RouteReadRequest(sql string, args ...any) (reader.Result, error) {
	n.logger.Info("new read request", zap.Any("req", req))

	result, err := n.readClient.Read(sql, args......)
	if err != nil {
		n.logger.Info("failed read",
			zap.String("sql", sql),
			zap.Error(err))

		return nil, err
	}

	return result, nil
}
