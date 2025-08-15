package nodeserver

import (
	"context"
	"errors"
	"fmt"

	"github.com/bytedance/sonic"
	"github.com/osamikoyo/leviathan/config"
	"github.com/osamikoyo/leviathan/logger"
	"github.com/osamikoyo/leviathan/nodecore"
	"github.com/osamikoyo/leviathan/nodepb"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

var (
	ErrEmptySQL           = errors.New("sql query cannot be empty")
	ErrFailedUnmarshalArg = errors.New("failed to unmarshal argument")
	ErrFailedMarshalResp  = errors.New("failed to marshal response")
	ErrCoreRequestFailed  = errors.New("core request failed")
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
	if req == nil {
		n.logger.Error("received nil request")
		return &nodepb.Response{
			NodeId:  n.cfg.NodeID,
			Message: "request cannot be nil",
		}, errors.New("nil request")
	}

	if len(req.Sql) == 0 {
		n.logger.Error("received empty SQL query")
		return &nodepb.Response{
			NodeId:  n.cfg.NodeID,
			Message: ErrEmptySQL.Error(),
		}, ErrEmptySQL
	}

	n.logger.Debug("processing route request",
		zap.String("sql", req.Sql),
		zap.Int("args_count", len(req.Args)))

	// Convert protobuf Any arguments to Go values
	args := make([]any, len(req.Args))
	for i, anyArg := range req.Args {
		value, err := n.unpackAnyValue(anyArg)
		if err != nil {
			n.logger.Error("failed to unpack argument",
				zap.Int("arg_index", i),
				zap.Error(err))
			return &nodepb.Response{
				NodeId:  n.cfg.NodeID,
				Message: fmt.Sprintf("failed to unpack argument at index %d: %v", i, err),
			}, fmt.Errorf("%w at index %d: %v", ErrFailedUnmarshalArg, i, err)
		}
		args[i] = value
	}

	// Execute read request through core
	resp, err := n.core.RouteReadRequest(req.Sql, args...)
	if err != nil {
		n.logger.Error("core read request failed",
			zap.String("sql", req.Sql),
			zap.Any("args", args),
			zap.Error(err))
		return &nodepb.Response{
			NodeId:  n.cfg.NodeID,
			Message: fmt.Sprintf("core request failed: %v", err),
		}, fmt.Errorf("%w: %v", ErrCoreRequestFailed, err)
	}

	// Marshal response to JSON
	body, err := sonic.Marshal(resp)
	if err != nil {
		n.logger.Error("failed to marshal response",
			zap.Any("response", resp),
			zap.Error(err))
		return &nodepb.Response{
			NodeId:  n.cfg.NodeID,
			Message: fmt.Sprintf("failed to marshal response: %v", err),
		}, fmt.Errorf("%w: %v", ErrFailedMarshalResp, err)
	}

	n.logger.Info("request processed successfully",
		zap.String("sql", req.Sql),
		zap.Int("result_size", len(body)))

	return &nodepb.Response{
		NodeId:  n.cfg.NodeID,
		Message: "success",
		Payload: string(body),
	}, nil
}

// unpackAnyValue extracts the actual value from protobuf Any type
func (n *NodeServer) unpackAnyValue(anyValue *anypb.Any) (any, error) {
	if anyValue == nil {
		return nil, errors.New("any value cannot be nil")
	}

	// Try to unmarshal as different wrapper types
	switch anyValue.GetTypeUrl() {
	case "type.googleapis.com/google.protobuf.StringValue":
		var val wrapperspb.StringValue
		if err := anyValue.UnmarshalTo(&val); err != nil {
			return nil, err
		}
		return val.GetValue(), nil

	case "type.googleapis.com/google.protobuf.Int32Value":
		var val wrapperspb.Int32Value
		if err := anyValue.UnmarshalTo(&val); err != nil {
			return nil, err
		}
		return val.GetValue(), nil

	case "type.googleapis.com/google.protobuf.Int64Value":
		var val wrapperspb.Int64Value
		if err := anyValue.UnmarshalTo(&val); err != nil {
			return nil, err
		}
		return val.GetValue(), nil

	case "type.googleapis.com/google.protobuf.UInt32Value":
		var val wrapperspb.UInt32Value
		if err := anyValue.UnmarshalTo(&val); err != nil {
			return nil, err
		}
		return val.GetValue(), nil

	case "type.googleapis.com/google.protobuf.UInt64Value":
		var val wrapperspb.UInt64Value
		if err := anyValue.UnmarshalTo(&val); err != nil {
			return nil, err
		}
		return val.GetValue(), nil

	case "type.googleapis.com/google.protobuf.FloatValue":
		var val wrapperspb.FloatValue
		if err := anyValue.UnmarshalTo(&val); err != nil {
			return nil, err
		}
		return val.GetValue(), nil

	case "type.googleapis.com/google.protobuf.DoubleValue":
		var val wrapperspb.DoubleValue
		if err := anyValue.UnmarshalTo(&val); err != nil {
			return nil, err
		}
		return val.GetValue(), nil

	case "type.googleapis.com/google.protobuf.BoolValue":
		var val wrapperspb.BoolValue
		if err := anyValue.UnmarshalTo(&val); err != nil {
			return nil, err
		}
		return val.GetValue(), nil

	case "type.googleapis.com/google.protobuf.BytesValue":
		var val wrapperspb.BytesValue
		if err := anyValue.UnmarshalTo(&val); err != nil {
			return nil, err
		}
		return val.GetValue(), nil

	default:
		// Fallback to generic unmarshaling
		value, err := anyValue.UnmarshalNew()
		if err != nil {
			return nil, fmt.Errorf("unsupported type %s: %w", anyValue.GetTypeUrl(), err)
		}
		return value, nil
	}
}
