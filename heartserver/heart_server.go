package heartserver

import (
	"context"
	"fmt"

	"github.com/bytedance/sonic"
	"github.com/osamikoyo/leviathan/config"
	"github.com/osamikoyo/leviathan/heartcore"
	"github.com/osamikoyo/leviathan/heartpb"
	"github.com/osamikoyo/leviathan/logger"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

type HeartServer struct {
	core   *heartcore.HeartCore
	logger *logger.Logger
	cfg    *config.HeartConfig
}

func NewHeartServer(core *heartcore.HeartCore, logger *logger.Logger, cfg *config.HeartConfig) *HeartServer {
	return &HeartServer{
		core:   core,
		logger: logger,
		cfg:    cfg,
	}
}

// unpackAny converts anypb.Any back to Go native type
func unpackAny(anyVal *anypb.Any) (interface{}, error) {
	if anyVal == nil {
		return nil, fmt.Errorf("any value is nil")
	}

	// Try to unpack as different wrapper types
	var stringVal wrapperspb.StringValue
	if err := anyVal.UnmarshalTo(&stringVal); err == nil {
		return stringVal.GetValue(), nil
	}

	var boolVal wrapperspb.BoolValue
	if err := anyVal.UnmarshalTo(&boolVal); err == nil {
		return boolVal.GetValue(), nil
	}

	var int32Val wrapperspb.Int32Value
	if err := anyVal.UnmarshalTo(&int32Val); err == nil {
		return int32Val.GetValue(), nil
	}

	var int64Val wrapperspb.Int64Value
	if err := anyVal.UnmarshalTo(&int64Val); err == nil {
		return int64Val.GetValue(), nil
	}

	var uint32Val wrapperspb.UInt32Value
	if err := anyVal.UnmarshalTo(&uint32Val); err == nil {
		return uint32Val.GetValue(), nil
	}

	var uint64Val wrapperspb.UInt64Value
	if err := anyVal.UnmarshalTo(&uint64Val); err == nil {
		return uint64Val.GetValue(), nil
	}

	var floatVal wrapperspb.FloatValue
	if err := anyVal.UnmarshalTo(&floatVal); err == nil {
		return floatVal.GetValue(), nil
	}

	var doubleVal wrapperspb.DoubleValue
	if err := anyVal.UnmarshalTo(&doubleVal); err == nil {
		return doubleVal.GetValue(), nil
	}

	var bytesVal wrapperspb.BytesValue
	if err := anyVal.UnmarshalTo(&bytesVal); err == nil {
		return bytesVal.GetValue(), nil
	}

	return nil, fmt.Errorf("unsupported any type: %s", anyVal.GetTypeUrl())
}

func (h *HeartServer) Route(ctx context.Context, req *heartpb.Request) (*heartpb.Response, error) {
	// Convert anypb.Any arguments to native Go types
	args := make([]interface{}, len(req.Args))
	for i, anyArg := range req.Args {
		val, err := unpackAny(anyArg)
		if err != nil {
			h.logger.Error("failed to unpack argument",
				zap.Int("arg_index", i),
				zap.Error(err))
			return &heartpb.Response{
				Message: fmt.Sprintf("failed to unpack argument %d: %v", i, err),
			}, err
		}
		args[i] = val
	}

	if req.WriteRequest {
		err := h.core.RouteWriteRequest(ctx, req.Sql, args...)
		if err != nil {
			h.logger.Error("failed route write request", zap.Error(err))

			return &heartpb.Response{
				Message: err.Error(),
			}, err
		}

		return &heartpb.Response{
			Message: "ok",
		}, nil
	} else {
		values, err := h.core.RouteReadRequest(ctx, req.Sql, args...)
		if err != nil {
			h.logger.Error("failed route read request", zap.Error(err))

			return &heartpb.Response{
				Message: err.Error(),
			}, err
		}

		body, err := sonic.Marshal(values)
		if err != nil {
			return &heartpb.Response{
				Message: err.Error(),
			}, err
		}

		return &heartpb.Response{
			Payload: string(body),
			Message: "ok",
		}, nil
	}
}
