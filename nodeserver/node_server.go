package nodeserver

import (
	"context"

	"github.com/bytedance/sonic"
	"github.com/osamikoyo/leviathan/config"
	"github.com/osamikoyo/leviathan/logger"
	"github.com/osamikoyo/leviathan/nodecore"
	"github.com/osamikoyo/leviathan/nodepb"
)

type NodeServer struct {
	nodepb.UnimplementedNodeServer

	logger *logger.Logger
	cfg    *config.NodeConfig
	core   *nodecore.NodeCore
}

func NewNodeServer(core *nodecore.NodeCore, logger *logger.Logger, cfg *config.NodeConfig) *NodeServer {
	return &NodeServer{
		logger: logger,
		cfg:    cfg,
		core:   core,
	}
}

func (n *NodeServer) Route(ctx context.Context, req *nodepb.Request) (*nodepb.Response, error) {
	if req.WriteRequest {
		err := n.core.RouteWriteRequest(req.Sql, req.Args)
		if err != nil {
			return &nodepb.Response{
				Message: err.Error(),
			}, err
		}

		return &nodepb.Response{
			Message: "ok",
		}, nil
	} else {
		value, err := n.core.RouteReadRequest(req.Sql, req.Args)
		if err != nil {
			return &nodepb.Response{
				Message: err.Error(),
			}, err
		}

		payload, err := sonic.Marshal(&value)
		if err != nil {
			return &nodepb.Response{
				Message: err.Error(),
			}, err
		}

		return &nodepb.Response{
			Payload: string(payload),
			Message: "ok",
		}, nil
	}
}
