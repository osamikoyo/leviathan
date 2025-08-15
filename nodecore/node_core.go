package nodecore

import (
	"errors"
	"fmt"

	"github.com/osamikoyo/leviathan/config"
	"github.com/osamikoyo/leviathan/logger"
	"github.com/osamikoyo/leviathan/reader"
	"github.com/osamikoyo/leviathan/writer"
	"go.uber.org/zap"
)

var (
	ErrEmptySQL    = errors.New("sql query cannot be empty")
	ErrWriteFailed = errors.New("write operation failed")
	ErrReadFailed  = errors.New("read operation failed")
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
) (*NodeCore, error) {
	if cfg == nil {
		return nil, errors.New("config cannot be nil")
	}
	if writer == nil {
		return nil, errors.New("writer client cannot be nil")
	}
	if reader == nil {
		return nil, errors.New("reader client cannot be nil")
	}
	if logger == nil {
		return nil, errors.New("logger cannot be nil")
	}

	logger.Info("nodecore initialized successfully",
		zap.Uint32("node_id", cfg.NodeID))

	return &NodeCore{
		cfg:         cfg,
		writeClient: writer,
		readClient:  reader,
		logger:      logger,
	}, nil
}

func (n *NodeCore) RouteWriteRequest(sql string, args ...any) error {
	if len(sql) == 0 {
		n.logger.Error("received empty SQL for write request")
		return ErrEmptySQL
	}

	n.logger.Debug("processing write request",
		zap.String("sql", sql),
		zap.Int("args_count", len(args)),
		zap.Uint32("node_id", n.cfg.NodeID))

	if err := n.writeClient.Write(sql, args...); err != nil {
		n.logger.Error("write request failed",
			zap.String("sql", sql),
			zap.Any("args", args),
			zap.Uint32("node_id", n.cfg.NodeID),
			zap.Error(err))
		return fmt.Errorf("%w: %v", ErrWriteFailed, err)
	}

	n.logger.Info("write request completed successfully",
		zap.String("sql", sql),
		zap.Uint32("node_id", n.cfg.NodeID))

	return nil
}

func (n *NodeCore) RouteReadRequest(sql string, args ...any) (reader.Result, error) {
	if len(sql) == 0 {
		n.logger.Error("received empty SQL for read request")
		return nil, ErrEmptySQL
	}

	n.logger.Debug("processing read request",
		zap.String("sql", sql),
		zap.Int("args_count", len(args)),
		zap.Uint32("node_id", n.cfg.NodeID))

	result, err := n.readClient.Read(sql, args...)
	if err != nil {
		n.logger.Error("read request failed",
			zap.String("sql", sql),
			zap.Any("args", args),
			zap.Uint32("node_id", n.cfg.NodeID),
			zap.Error(err))
		return nil, fmt.Errorf("%w: %v", ErrReadFailed, err)
	}

	n.logger.Info("read request completed successfully",
		zap.String("sql", sql),
		zap.Int("result_count", len(result)),
		zap.Uint32("node_id", n.cfg.NodeID))

	return result, nil
}
