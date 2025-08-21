package heartclient

import (
	"context"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
	"reflect"

	"github.com/osamikoyo/leviathan/heartpb"
	"github.com/osamikoyo/leviathan/logger"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

type HeartClient struct {
	client   heartpb.HeartClient
	logger   *logger.Logger
	certPath string
}

func packAny(value any) (*anypb.Any, error) {
	if value == nil {
		return nil, errors.New("nil value")
	}

	switch v := value.(type) {
	case string:
		return anypb.New(wrapperspb.String(v))
	case bool:
		return anypb.New(wrapperspb.Bool(v))
	case int:
		return anypb.New(wrapperspb.Int64(int64(v)))
	case int32:
		return anypb.New(wrapperspb.Int32(v))
	case int64:
		return anypb.New(wrapperspb.Int64(v))
	case uint32:
		return anypb.New(wrapperspb.UInt32(v))
	case uint64:
		return anypb.New(wrapperspb.UInt64(v))
	case float32:
		return anypb.New(wrapperspb.Float(v))
	case float64:
		return anypb.New(wrapperspb.Double(v))
	case []byte:
		return anypb.New(wrapperspb.Bytes(v))
	default:
		return nil, fmt.Errorf("%s: %v", "unsupported type", reflect.TypeOf(value))
	}
}

func NewHeartClient(hearturl string, logger *logger.Logger, cert string) (*HeartClient, error) {
	certPool := x509.NewCertPool()
	body, err := os.ReadFile(cert)
	if err != nil {
		logger.Fatal("failed open file",
			zap.String("cert", cert),
			zap.Error(err))

		return nil, err
	}

	if !certPool.AppendCertsFromPEM(body) {
		logger.Fatal("failed to append server certificate",
			zap.String("path", cert),
			zap.Error(err))

		return nil, err
	}

	creds := credentials.NewClientTLSFromCert(certPool, "")

	conn, err := grpc.NewClient(hearturl, grpc.WithTransportCredentials(creds))
	if err != nil {
		logger.Fatal("failed get heart connection",
			zap.String("url", hearturl),
			zap.Error(err))

		return nil, err
	}

	logger.Info("successfully connect to server!", zap.String("url", hearturl))

	client := heartpb.NewHeartClient(conn)
	return &HeartClient{
		client:   client,
		logger:   logger,
		certPath: cert,
	}, nil
}

func (h *HeartClient) SendWriteRequest(ctx context.Context, sql string, args ...any) error {
	argsPb := make([]*anypb.Any, len(args))
	for i, arg := range args {
		argPb, err := packAny(arg)
		if err != nil {
			h.logger.Error("failed to pack argument",
				zap.Int("arg_index", i),
				zap.Any("arg_value", arg),
				zap.Error(err))
			return fmt.Errorf("failed to pack argument at index %d: %w", i, err)
		}
		argsPb[i] = argPb
	}

	resp, err := h.client.Route(ctx, &heartpb.Request{
		Sql:  sql,
		Args: argsPb,
	})
	if err != nil {
		h.logger.Error("failed send request",
			zap.String("sql", sql),
			zap.Error(err))

		return err
	}

	h.logger.Info("write request send successfully",
		zap.String("message", resp.Message))

	return nil
}

func (h *HeartClient) RouteReadRequest(ctx context.Context, sql string, args ...any) (string, error) {
	argsPb := make([]*anypb.Any, len(args))
	for i, arg := range args {
		argPb, err := packAny(arg)
		if err != nil {
			h.logger.Error("failed to pack argument",
				zap.Int("arg_index", i),
				zap.Any("arg_value", arg),
				zap.Error(err))
			return "", fmt.Errorf("failed to pack argument at index %d: %w", i, err)
		}
		argsPb[i] = argPb
	}

	resp, err := h.client.Route(ctx, &heartpb.Request{
		Sql:  sql,
		Args: argsPb,
	})
	if err != nil {
		h.logger.Error("failed send read request",
			zap.String("sql", sql),
			zap.Error(err))

		return "", err
	}

	h.logger.Info("read request send successfully",
		zap.String("message", resp.Message))

	return resp.Payload, nil
}
